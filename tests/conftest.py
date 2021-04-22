import contextlib
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import subprocess
import tarfile
import time
import uuid
from urllib.parse import urlsplit, urlunsplit
from pathlib import Path
import pytest


import click
from click.testing import CliRunner
import pygit2
import sqlalchemy


from kart.geometry import Geometry
from kart.repo import SnoRepo
from kart.sqlalchemy.create_engine import gpkg_engine, postgis_engine, sqlserver_engine


pytest_plugins = ["helpers_namespace"]


L = logging.getLogger("kart.tests")


def pytest_addoption(parser):
    if "CI" in os.environ:
        # pytest.ini sets --numprocesses=auto
        # for parallelism in local dev.
        # But in CI we disable xdist because it causes a crash in windows builds.
        # (simply doing --numprocesses=0 is insufficient; the plugin needs to be
        # disabled completely)
        # However, there's no way to *remove* an option that's in pytest.ini's addopts.
        # So here we just define the option so it parses, and then ignore it.
        parser.addoption(
            "--numprocesses",
            action="store",
            default=0,
            help="<ignored>",
        )
    parser.addoption(
        "--preserve-data",
        action="store_true",
        default=False,
        help="Preserve temporary data directories",
    )
    parser.addoption(
        "--pdb-trace",
        action="store_true",
        default=False,
        help="Allow calling pytest.set_trace() within Click commands",
    )


def pytest_report_header(config):
    if config._benchmarksession.disabled:
        click.secho(
            "\nSkipping benchmarks in tests. Use --benchmark-enable to run them.",
            bold=True,
            fg="yellow",
        )


# https://github.com/pytest-dev/pytest/issues/363
@pytest.fixture(scope="session")
def monkeypatch_session(request):
    from _pytest.monkeypatch import MonkeyPatch

    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture
def gen_uuid(request):
    """ Deterministic "random" UUID generator seeded from the test ID """
    seed = int(hashlib.sha1(request.node.nodeid.encode("utf8")).hexdigest(), 16)
    _uuid_gen = random.Random(seed)

    def _uuid():
        return str(uuid.UUID(int=_uuid_gen.getrandbits(128)))

    return _uuid


@pytest.fixture(scope="session", autouse=True)
def git_user_config(monkeypatch_session, tmp_path_factory, request):
    home = tmp_path_factory.mktemp("home")

    # override libgit2's search paths
    pygit2.option(pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_SYSTEM, "")
    pygit2.option(pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_XDG, "")
    pygit2.option(
        pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, str(home)
    )

    # setup environment variables in case we call 'git' commands
    monkeypatch_session.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch_session.setenv("HOME", str(home))
    monkeypatch_session.setenv("GIT_ATTR_NOSYSTEM", "1")
    monkeypatch_session.setenv("GIT_CONFIG_NOSYSTEM", "1")

    USER_NAME = "Kart Tester"
    USER_EMAIL = "kart-tester@example.com"

    with open(home / ".gitconfig", "w") as f:
        f.write(
            f"[user]\n"
            f"\tname = {USER_NAME}\n"
            f"\temail = {USER_EMAIL}\n"
            # make `gc` syncronous in testing.
            # otherwise it has race conditions with test teardown.
            f"[gc]\n"
            f"\tautoDetach = false\n"
            f"[init]\n"
            f"\tdefaultBranch = main\n"
            # used by test_clone_filter
            f"[uploadPack]\n"
            f"\tallowFilter = true\n"
        )

    L.debug("Temporary HOME for git config: %s", home)

    with pytest.raises(IOError):
        pygit2.Config.get_system_config()

    global_cfg = pygit2.Config.get_global_config()
    assert global_cfg["user.email"] == USER_EMAIL
    assert global_cfg["user.name"] == USER_NAME

    return (USER_EMAIL, USER_NAME, home)


@contextlib.contextmanager
def chdir_(path):
    """ Context manager to change the current working directory """
    prev_cwd = os.getcwd()
    try:
        os.chdir(path)
        yield prev_cwd
    finally:
        os.chdir(prev_cwd)


@pytest.fixture
def chdir():
    return chdir_


def cleanup_dir(d):
    try:
        shutil.rmtree(d)
    except PermissionError as e:
        L.debug("Issue cleaning up temporary folder (%s): %s", d, e)


def get_archive_path(name):
    return Path(__file__).parent / "data" / Path(name).with_suffix(".tgz")


def extract_archive(archive_path, extract_dir):
    archive_path = get_archive_path(archive_path)
    with tarfile.open(archive_path) as archive:
        archive.extractall(extract_dir)

    L.info("Extracted %s to %s", archive_path.name, extract_dir)

    # data archive should have a single dir at the top-level, matching the archive name.
    top_dir = archive_path.stem
    assert (
        len(os.listdir(extract_dir)) == 1
    ), f"Expected {top_dir}/ as the only top-level item in {archive_path.name}"
    d = extract_dir / top_dir
    assert (
        d.is_dir()
    ), f"Expected {top_dir}/ as the only top-level item in {archive_path.name}"
    return d


@pytest.fixture
def data_archive(request, tmp_path_factory):
    """
    Extract a .tgz data archive to a temporary folder.

    When --preserve-data is passed on the command line, a failing test will have the folder kept.

    Context-manager produces the directory path and sets the current working directory.
    """

    @contextlib.contextmanager
    def ctx(name):
        extract_dir = tmp_path_factory.mktemp(request.node.name, numbered=True)
        cleanup = True
        try:
            d = extract_archive(name, extract_dir)
            with chdir_(d):
                try:
                    yield d
                finally:
                    if request.config.getoption("--preserve-data"):
                        L.info(
                            "Not cleaning up %s because --preserve-data was specified",
                            extract_dir,
                        )
                        cleanup = False
        finally:
            if cleanup:
                time.sleep(1)
                cleanup_dir(extract_dir)

    return ctx


_archive_hashes = {}


@pytest.fixture()
def data_archive_readonly(request, pytestconfig):
    """
    Extract a .tgz data archive to a temporary folder, and CACHE it in the pytest cache.
    Don't use this if you're going to write to the dir!
    If you don't need to write to the extracted archive, use this in preference to data_archive.

    Context-manager produces the directory path and sets the current working directory.
    """

    @contextlib.contextmanager
    def ctx(archive_path):
        archive_path = get_archive_path(archive_path)
        key = str(archive_path.relative_to(Path(__file__).parent))

        if key not in _archive_hashes:
            # Store extracted data in a content-addressed cache,
            # so if the archives change we don't have to manually `pytest --cache-clear`
            with archive_path.open("rb") as f:
                _archive_hashes[key] = hashlib.md5(f.read()).hexdigest()

        root = Path(request.config.cache.makedir("data_archive_readonly"))
        path = root / _archive_hashes[key]
        if path.exists():
            L.info("Found cache at %s", path)
        else:
            extract_archive(archive_path, path)
        path /= archive_path.stem
        with chdir_(path):
            yield path

    yield ctx


@pytest.fixture
def data_working_copy(request, data_archive, tmp_path_factory, cli_runner):
    """
    Extract a repo archive with a working copy geopackage
    If the geopackage isn't in the archive, create it via `kart create-workingcopy`

    Context-manager produces a 2-tuple: (repository_path, working_copy_path)
    """
    incr = 0

    @contextlib.contextmanager
    def _data_working_copy(archive_path, force_new=False):
        nonlocal incr

        archive_path = get_archive_path(archive_path)
        with data_archive(archive_path) as repo_dir:
            repo = SnoRepo(repo_dir)
            if repo.working_copy:
                wc_path = repo.working_copy.full_path
                if force_new:
                    L.info("force_new is set, deleting existing WC: %s", wc_path)
                    del repo.working_copy
                    assert not hasattr(repo, "_working_copy")
                    del wc_path

            if not repo.working_copy:
                wc_path = (
                    tmp_path_factory.mktemp(request.node.name, str(incr))
                    / archive_path.with_suffix(".gpkg").name
                )
                incr += 1
                L.info("Creating working copy at %s", wc_path)
                r = cli_runner.invoke(
                    ["create-workingcopy", wc_path, "--delete-existing"]
                )
                assert r.exit_code == 0, r.stderr

            del repo

            L.info("data_working_copy: %s %s", repo_dir, wc_path)
            yield repo_dir, wc_path

    return _data_working_copy


@pytest.fixture
def data_imported(cli_runner, data_archive, chdir, request, tmp_path_factory):
    """
    Extract a source geopackage archive, then import the table into a new repository.

    Caches it in the pytest cache, so don't use it for writeable things!

    Returns the path to the repository path
    """
    L = logging.getLogger("data_imported")
    incr = 0

    def _data_imported(archive, source_gpkg, table):
        nonlocal incr

        params = [archive, source_gpkg, table]
        cache_key = f"data_imported~{'~'.join(params)}"

        repo_path = Path(request.config.cache.makedir(cache_key)) / "repo"
        if repo_path.exists():
            L.info("Found cache at %s", repo_path)
            return str(repo_path)

        with data_archive(archive) as data:
            import_path = tmp_path_factory.mktemp(request.node.name, str(incr)) / "data"
            incr += 1

            import_path.mkdir()
            with chdir(import_path):
                r = cli_runner.invoke(["init"])
                assert r.exit_code == 0, r.stderr

                repo = SnoRepo(import_path)
                assert repo.is_empty
                repo.free()
                del repo

                r = cli_runner.invoke(
                    [
                        "import",
                        f"GPKG:{data / source_gpkg}",
                        f"{table}:mytable",
                    ]
                )
                assert r.exit_code == 0, r.stderr

            time.sleep(1)
            shutil.copytree(import_path, repo_path)
            L.info("Created cache at %s", repo_path)
            return str(repo_path)

    return _data_imported


class SnoCliRunner(CliRunner):
    def __init__(self, *args, in_pdb=False, mix_stderr=False, **kwargs):
        self._in_pdb = in_pdb
        super().__init__(*args, mix_stderr=mix_stderr, **kwargs)

    def invoke(self, args=None, **kwargs):
        from kart.cli import load_commands_from_args, cli
        from kart.cli_util import init_git_config

        init_git_config.cache_clear()

        if args:
            # force everything to strings (eg. PathLike objects, numbers)
            args = [str(a) for a in args]

        L.debug("Invoking Click command: %s (%s)", args, kwargs)

        params = {"catch_exceptions": not self._in_pdb}
        params.update(kwargs)

        load_commands_from_args(args)
        r = super().invoke(cli, args=args, **params)

        L.debug("Command result: %s (%s)", r.exit_code, repr(r))
        L.debug("Command stdout=%s", r.stdout)
        L.debug("Command stderr=%s", (r.stderr if r.stderr_bytes else ""))

        if r.exception and not isinstance(r.exception, SystemExit):
            raise r.exception

        return r

    def isolation(self, input=None, env=None, color=False):
        if self._in_pdb:
            if input or env or color:
                L.warning("PDB un-isolation doesn't work if input/env/color are passed")
            else:
                return self.isolation_pdb(env=env)

        return super().isolation(input=input, env=env, color=color)

    @contextlib.contextmanager
    def isolation_pdb(self, env=None):
        s = io.BytesIO(b"{stdout not captured because --pdb-trace}")
        old_env = {}
        env = self.make_env(env)
        try:
            for key, value in env.items():
                old_env[key] = os.environ.get(key)
                if value is None:
                    try:
                        del os.environ[key]
                    except Exception:
                        pass
                else:
                    os.environ[key] = value
            yield (s, not self.mix_stderr and s)
        finally:
            for key, value in old_env.items():
                if value is None:
                    try:
                        del os.environ[key]
                    except Exception:
                        pass
                else:
                    os.environ[key] = value


@pytest.fixture
def cli_runner(request):
    """ A wrapper round Click's test CliRunner to improve usefulness """
    return SnoCliRunner(
        # kart.cli._execvp() looks for this env var to prevent fork/exec in tests.
        env={"_SNO_NO_EXEC": "1"},
        # workaround Click's environment isolation so debugging works.
        in_pdb=request.config.getoption("--pdb-trace"),
    )


@pytest.helpers.register
def helpers():
    return TestHelpers


class TestHelpers:

    # Test Dataset (gpkg-points / points)
    class POINTS:
        ARCHIVE = "points"
        LAYER = "nz_pa_points_topo_150k"
        LAYER_PK = "fid"
        INSERT = f"""
            INSERT INTO {LAYER}
                            ("fid", "geom", "t50_fid", "name_ascii", "macronated", "name")
                        VALUES
                            (:fid, :geom, :t50_fid, :name_ascii, :macronated, :name);
        """
        RECORD = {
            "fid": 9999,
            "geom": Geometry.from_wkt("POINT(0 0)").with_crs_id(4326),
            "t50_fid": 9_999_999,
            "name_ascii": "Te Motu-a-kore",
            "macronated": "N",
            "name": "Te Motu-a-kore",
        }
        HEAD_SHA = "0c64d8211c072a08d5fc6e6fe898cbb59fc83d16"
        HEAD1_SHA = "7bc3b56f20d1559208bcf5bb56860dda6e190b70"
        HEAD_TREE_SHA = "a8fa3347aed53547b194fc2101974b79b7fc337b"
        HEAD1_TREE_SHA = "8feb827cf21831cc4766345894cd122947bba748"
        ROWCOUNT = 2143
        TEXT_FIELD = "name"
        SAMPLE_PKS = list(range(1, 11))

    # Test Dataset (gpkg-polygons / polygons)
    class POLYGONS:
        ARCHIVE = "polygons"
        LAYER = "nz_waca_adjustments"
        LAYER_PK = "id"
        INSERT = f"""
            INSERT INTO {LAYER}
                            ("id", "geom", "date_adjusted", "survey_reference", "adjusted_nodes")
                        VALUES
                            (:id, :geom, :date_adjusted, :survey_reference, :adjusted_nodes);
        """
        RECORD = {
            "id": 9_999_999,
            "geom": Geometry.from_wkt(
                "MULTIPOLYGON(((0 0, 0 0.001, 0.001 0.001, 0.001 0, 0 0)))"
            ).with_crs_id(4167),
            "date_adjusted": "2019-07-05T13:04:00Z",
            "survey_reference": "Null Island™ 🗺",
            "adjusted_nodes": 123,
        }
        HEAD_SHA = "5bb25a2da966b15ae7743db4666c1599001e2443"
        ROWCOUNT = 228
        TEXT_FIELD = "survey_reference"
        SAMPLE_PKS = [
            1424927,
            1443053,
            1452332,
            1456853,
            1456912,
            1457297,
            1457355,
            1457612,
            1457636,
            1458553,
        ]

    # Test Dataset (gpkg-spec / table)
    class TABLE:
        ARCHIVE = "table"
        LAYER = "countiestbl"
        LAYER_PK = "OBJECTID"
        INSERT = f"""
            INSERT INTO {LAYER}
                            ("OBJECTID", "NAME", "STATE_NAME", "STATE_FIPS", "CNTY_FIPS", "FIPS", "AREA", "POP1990", "POP2000", "POP90_SQMI", "Shape_Leng", "Shape_Area")
                        VALUES
                            (:OBJECTID, :NAME, :STATE_NAME, :STATE_FIPS, :CNTY_FIPS, :FIPS, :AREA, :POP1990, :POP2000, :POP90_SQMI, :Shape_Leng, :Shape_Area);
        """
        RECORD = {
            "OBJECTID": 9999,
            "NAME": "Lake of the Gruffalo",
            "STATE_NAME": "Minnesota",
            "STATE_FIPS": "27",
            "CNTY_FIPS": "077",
            "FIPS": "27077",
            "AREA": 1784.0634,
            "POP1990": 4076,
            "POP2000": 4651,
            "POP90_SQMI": 2,
            "Shape_Leng": 4.055_459_982_439_92,
            "Shape_Area": 0.565_449_933_741_451,
        }
        HEAD_SHA = "e3d4ad33461d1603098666052310cb330bc812b5"
        ROWCOUNT = 3141
        TEXT_FIELD = "NAME"
        SAMPLE_PKS = list(range(1, 11))

    @classmethod
    def metadata(cls, l):
        metadatas = (cls.POINTS, cls.POLYGONS, cls.TABLE)
        return next((m for m in metadatas if m.LAYER == l or m.__name__ == l))

    @classmethod
    def last_change_time(cls, conn, table=POINTS.LAYER):
        """
        Get the last change time from the GeoPackage DB.
        This is the same as the commit time.
        """
        return conn.execute(
            f"SELECT last_change FROM gpkg_contents WHERE table_name=:table_name;",
            {"table_name": table},
        ).scalar()

    @classmethod
    def row_count(cls, conn, table):
        return conn.execute(f'SELECT COUNT(*) FROM "{table}";').scalar()

    @classmethod
    def table_pattern_count(cls, conn, pattern):
        # Only works for sqlite / GPKG.
        return conn.scalar(
            f"SELECT count(*) FROM sqlite_master WHERE type='table' AND name LIKE (:pattern);",
            {"pattern": pattern},
        )

    @classmethod
    def clear_working_copy(cls, repo_path="."):
        """ Delete any existing working copy & associated config """
        repo = SnoRepo(repo_path)
        wc = repo.get_working_copy(allow_invalid_state=True)
        if wc:
            print(f"Deleting existing working copy: {repo.workingcopy_location}")
            wc.delete()

        if repo.WORKINGCOPY_LOCATION_KEY in repo.config:
            del repo.config[repo.WORKINGCOPY_LOCATION_KEY]

    @classmethod
    def db_table_hash(cls, conn, table, pk=None):
        """ Calculate a SHA1 hash of the contents of a SQLite table """
        if pk is None:
            pk = "ROWID"

        sql = f"SELECT * FROM {table} ORDER BY {pk};"
        r = conn.execute(sql)
        h = hashlib.sha1()
        for row in r:
            h.update("🔸".join(repr(col) for col in row).encode("utf-8"))
        return h.hexdigest()

    @classmethod
    def git_graph(cls, request, message, count=10, *paths):
        """ Print a pretty graph of recent git revisions """
        cmd = [
            "git",
            "log",
            "--all",
            "--decorate",
            "--oneline",
            "--graph",
            f"--max-count={count}",
        ]

        # total hackery to figure out whether we're _actually_ in a terminal
        try:
            cm = request.config.pluginmanager.getplugin("capturemanager")
            fd = cm._global_capturing.in_.targetfd_save
            if os.isatty(fd):
                cmd += ["--color=always"]
        except Exception:
            pass

        print(f"{message}:")
        subprocess.check_call(cmd + list(paths))

    @classmethod
    def parameter_ids(cls, request):
        """ Get an array of parameter IDs """
        # nodeid = 'test_import_feature_performance[0.2.0-spec-counties-table]'
        param_ids = re.match(r".*\[(.+)\]$", request.node.nodeid).group(1).split("-")
        return tuple(param_ids)

    @classmethod
    def verify_gpkg_extent(cls, conn, table):
        """ Check the aggregate layer extent from the table matches the values in gpkg_contents """
        r = conn.execute(
            """SELECT column_name FROM "gpkg_geometry_columns" WHERE table_name=:table_name;""",
            {"table_name": table},
        ).fetchone()
        geom_col = r[0] if r else None

        gpkg_extent = tuple(
            conn.execute(
                """SELECT min_x,min_y,max_x,max_y FROM "gpkg_contents" WHERE table_name=:table_name;""",
                {"table_name": table},
            ).fetchone()
        )

        if geom_col:
            layer_extent = tuple(
                conn.execute(
                    f"""
                WITH _E AS (
                    SELECT extent("{geom_col}") AS extent
                    FROM "{table}"
                )
                SELECT
                    ST_MinX(extent),
                    ST_MinY(extent),
                    ST_MaxX(extent),
                    ST_MaxY(extent)
                FROM _E
                """
                ).fetchone()
            )
            assert gpkg_extent == pytest.approx(layer_extent)
        else:
            assert gpkg_extent == (None, None, None, None)


def _find_layer(conn):
    H = pytest.helpers.helpers()
    return conn.execute(
        "SELECT table_name FROM gpkg_contents WHERE table_name IN (:points, :polygons, :table) LIMIT 1",
        {
            "points": H.POINTS.LAYER,
            "polygons": H.POLYGONS.LAYER,
            "table": H.TABLE.LAYER,
        },
    ).scalar()


@pytest.fixture
def insert(request, cli_runner):
    H = pytest.helpers.helpers()

    def func(conn, layer=None, commit=True, reset_index=None, insert_str=None):
        if reset_index is not None:
            func.index = reset_index

        layer = layer or _find_layer(conn)

        metadata = H.metadata(layer)
        rec = metadata.RECORD.copy()
        pk_field = metadata.LAYER_PK
        sql = metadata.INSERT
        pk_start = 98000

        # th
        new_pk = pk_start + func.index
        rec[pk_field] = new_pk
        if insert_str:
            rec[metadata.TEXT_FIELD] = insert_str

        r = conn.execute(sql, rec)
        assert r.rowcount == 1
        func.inserted_fids.append(new_pk)

        func.index += 1

        if commit:
            if hasattr(conn, "commit"):
                conn.commit()
            r = cli_runner.invoke(
                ["commit", "-m", f"commit-{func.index}", "-o", "json"]
            )
            assert r.exit_code == 0, r.stderr

            commit_id = json.loads(r.stdout)["kart.commit/v1"]["commit"]
            return commit_id
        else:
            return new_pk

    func.index = 0
    func.inserted_fids = []

    return func


@pytest.fixture
def update(request, cli_runner):
    H = pytest.helpers.helpers()

    def func(conn, pk, update_str, layer=None, commit=True):
        layer = layer or _find_layer(conn)
        metadata = H.metadata(layer)
        pk_field = metadata.LAYER_PK
        text_field = metadata.TEXT_FIELD

        sql = (
            f"""UPDATE {layer} SET {text_field} = :update_str WHERE {pk_field} = {pk}"""
        )
        r = conn.execute(sql, {"update_str": update_str})
        assert r.rowcount == 1

        if commit:
            if hasattr(conn, "commit"):
                conn.commit()
            r = cli_runner.invoke(["commit", "-m", f"commit-update-{pk}", "-o", "json"])
            assert r.exit_code == 0, r.stderr

            commit_id = json.loads(r.stdout)["kart.commit/v1"]["commit"]
            return commit_id
        else:
            return pk

    return func


def _insert_command(table_name, col_names):
    return sqlalchemy.table(
        table_name, *[sqlalchemy.column(c) for c in col_names]
    ).insert()


def _edit_points(conn, dataset=None, working_copy=None):
    H = pytest.helpers.helpers()

    if working_copy is None:
        layer = H.POINTS.LAYER
        insert_cmd = _insert_command(H.POINTS.LAYER, H.POINTS.RECORD.keys())
    else:
        layer = f"{working_copy.DB_SCHEMA}.{H.POINTS.LAYER}"
        insert_cmd = working_copy._insert_into_dataset(dataset)

    # Note - different DB backends support and interpret rowcount differently.
    # Sometimes rowcount is not supported for inserts, so it just returns -1.
    # Rowcount can be 1 or 2 if 1 row has changed its PK
    r = conn.execute(insert_cmd, H.POINTS.RECORD)
    assert r.rowcount in (1, -1)
    r = conn.execute(f"UPDATE {layer} SET fid=9998 WHERE fid=1;")
    assert r.rowcount in (1, 2)
    r = conn.execute(f"UPDATE {layer} SET name='test' WHERE fid=2;")
    assert r.rowcount == 1
    r = conn.execute(f"DELETE FROM {layer} WHERE fid IN (3,30,31,32,33);")
    assert r.rowcount == 5
    pk_del = 3
    return pk_del


@pytest.fixture
def edit_points():
    return _edit_points


def _edit_polygons(conn, dataset=None, working_copy=None):
    H = pytest.helpers.helpers()
    if working_copy is None:
        layer = H.POLYGONS.LAYER
        insert_cmd = _insert_command(H.POLYGONS.LAYER, H.POLYGONS.RECORD.keys())
    else:
        layer = f"{working_copy.DB_SCHEMA}.{H.POLYGONS.LAYER}"
        insert_cmd = working_copy._insert_into_dataset(dataset)

    # See note on rowcount at _edit_points
    r = conn.execute(insert_cmd, H.POLYGONS.RECORD)
    assert r.rowcount in (1, -1)
    r = conn.execute(f"UPDATE {layer} SET id=9998 WHERE id=1424927;")
    assert r.rowcount in (1, 2)
    r = conn.execute(f"UPDATE {layer} SET survey_reference='test' WHERE id=1443053;")
    assert r.rowcount == 1
    r = conn.execute(
        f"DELETE FROM {layer} WHERE id IN (1452332, 1456853, 1456912, 1457297, 1457355);"
    )
    assert r.rowcount == 5
    pk_del = 1452332
    return pk_del


@pytest.fixture
def edit_polygons():
    return _edit_polygons


def _edit_table(conn, dataset=None, working_copy=None):
    H = pytest.helpers.helpers()

    if working_copy is None:
        layer = H.TABLE.LAYER
        insert_cmd = _insert_command(H.TABLE.LAYER, H.TABLE.RECORD.keys())
    else:
        layer = f"{working_copy.DB_SCHEMA}.{H.TABLE.LAYER}"
        insert_cmd = working_copy._insert_into_dataset(dataset)

    r = conn.execute(insert_cmd, H.TABLE.RECORD)
    # rowcount is not actually supported for inserts, but works in certain DB types - otherwise is -1.
    assert r.rowcount in (1, -1)
    r = conn.execute(f"""UPDATE {layer} SET "OBJECTID"=9998 WHERE "OBJECTID"=1;""")
    assert r.rowcount in (1, 2)
    r = conn.execute(f"""UPDATE {layer} SET "NAME"='test' WHERE "OBJECTID"=2;""")
    assert r.rowcount == 1
    r = conn.execute(f"""DELETE FROM {layer} WHERE "OBJECTID" IN (3,30,31,32,33);""")
    assert r.rowcount == 5
    pk_del = 3
    return pk_del


@pytest.fixture
def edit_table():
    return _edit_table


@pytest.fixture
def create_conflicts(
    data_working_copy,
    cli_runner,
    update,
    insert,
):
    @contextlib.contextmanager
    def ctx(data):
        with data_working_copy(data.ARCHIVE) as (repo_path, wc):
            repo = SnoRepo(repo_path)
            sample_pks = data.SAMPLE_PKS

            cli_runner.invoke(["checkout", "-b", "ancestor_branch"])
            cli_runner.invoke(["checkout", "-b", "theirs_branch"])

            with gpkg_engine(wc).connect() as conn:
                update(conn, sample_pks[0], "theirs_version")
                update(conn, sample_pks[1], "ours_theirs_version")
                update(conn, sample_pks[2], "theirs_version")
                update(conn, sample_pks[3], "theirs_version")
                update(conn, sample_pks[4], "theirs_version")
                insert(conn, reset_index=1, insert_str="insert_theirs")

                cli_runner.invoke(["checkout", "ancestor_branch"])
                cli_runner.invoke(["checkout", "-b", "ours_branch"])

                update(conn, sample_pks[1], "ours_theirs_version")
                update(conn, sample_pks[2], "ours_version")
                update(conn, sample_pks[3], "ours_version")
                update(conn, sample_pks[4], "ours_version")
                update(conn, sample_pks[5], "ours_version")
                insert(conn, reset_index=1, insert_str="insert_ours")

            yield repo

    return ctx


@pytest.fixture
def disable_editor():
    old_environ = dict(os.environ)
    os.environ["GIT_EDITOR"] = "echo"
    yield
    os.environ.clear()
    os.environ.update(old_environ)


@pytest.fixture()
def postgis_db():
    """
    Using docker, you can run a PostGIS test - such as test_postgis_import - as follows:
        docker run -it --rm -d -p 15432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgis/postgis
        SNO_POSTGRES_URL='postgresql://postgres:@localhost:15432/postgres' pytest -k postgis --pdb -vvs -n 0
    """
    if "SNO_POSTGRES_URL" not in os.environ:
        raise pytest.skip("Requires PostGIS - read docstring at conftest.postgis_db")
    engine = postgis_engine(os.environ["SNO_POSTGRES_URL"])
    with engine.connect() as conn:
        # test connection and postgis support
        try:
            conn.execute("SELECT postgis_version();")
        except sqlalchemy.exc.DBAPIError:
            raise pytest.skip("Requires PostGIS")
    yield engine


@pytest.fixture()
def new_postgis_db_schema(request, postgis_db):
    @contextlib.contextmanager
    def ctx(create=False):
        sha = hashlib.sha1(request.node.nodeid.encode("utf8")).hexdigest()[:20]
        schema = f"sno_test_{sha}"
        with postgis_db.connect() as conn:
            # Start by deleting in case it is left over from last test-run...
            conn.execute(f"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""")
            # Actually create only if create=True, otherwise the test will create it
            if create:
                conn.execute(f"""CREATE SCHEMA "{schema}";""")
        try:
            url = urlsplit(os.environ["SNO_POSTGRES_URL"])
            url_path = url.path.rstrip("/") + "/" + schema
            new_schema_url = urlunsplit(
                [url.scheme, url.netloc, url_path, url.query, ""]
            )
            yield new_schema_url, schema
        finally:
            # Clean up - delete it again if it exists.
            with postgis_db.connect() as conn:
                conn.execute(f"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""")

    return ctx


@pytest.fixture()
def sqlserver_db():
    """
    Using docker, you can run a SQL Server test - such as those in test_working_copy_sqlserver - as follows:
        docker run -it --rm -d -p 11433:1433 -e ACCEPT_EULA=Y -e 'SA_PASSWORD=PassWord1' mcr.microsoft.com/mssql/server
        SNO_SQLSERVER_URL='mssql://sa:PassWord1@127.0.0.1:11433/master' pytest -k sqlserver --pdb -vvs
    """
    if "SNO_SQLSERVER_URL" not in os.environ:
        raise pytest.skip(
            "Requires SQL Server - read docstring at conftest.sqlserver_db"
        )
    engine = sqlserver_engine(os.environ["SNO_SQLSERVER_URL"])
    with engine.connect() as conn:
        # Test connection
        try:
            conn.execute("SELECT @@version;")
        except sqlalchemy.exc.DBAPIError:
            raise pytest.skip("Requires SQL Server")
    yield engine


@pytest.fixture()
def new_sqlserver_db_schema(request, sqlserver_db):
    @contextlib.contextmanager
    def ctx(create=False):
        sha = hashlib.sha1(request.node.nodeid.encode("utf8")).hexdigest()[:20]
        schema = f"sno_test_{sha}"
        with sqlserver_db.connect() as conn:
            # Start by deleting in case it is left over from last test-run...
            _sqlserver_drop_schema_cascade(conn, schema)
            # Actually create only if create=True, otherwise the test will create it
            if create:
                conn.execute(f"""CREATE SCHEMA "{schema}";""")
        try:
            url = urlsplit(os.environ["SNO_SQLSERVER_URL"])
            url_path = url.path.rstrip("/") + "/" + schema
            new_schema_url = urlunsplit(
                [url.scheme, url.netloc, url_path, url.query, ""]
            )
            yield new_schema_url, schema
        finally:
            # Clean up - delete it again if it exists.
            with sqlserver_db.connect() as conn:
                _sqlserver_drop_schema_cascade(conn, schema)

    return ctx


def _sqlserver_drop_schema_cascade(conn, db_schema):
    r = conn.execute(
        sqlalchemy.text(
            "SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID(:schema);"
        ),
        {"schema": db_schema},
    )
    table_identifiers = ", ".join([f"{db_schema}.{row[0]}" for row in r])
    if table_identifiers:
        conn.execute(f"DROP TABLE IF EXISTS {table_identifiers};")

    conn.execute(f"DROP SCHEMA IF EXISTS {db_schema};")
