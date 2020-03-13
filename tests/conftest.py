import contextlib
import hashlib
import io
import logging
import os
import re
import shutil
import subprocess
import sys
import tarfile
import time
from pathlib import Path

import pytest
from click.testing import CliRunner

import apsw
import pygit2


pytest_plugins = ["helpers_namespace"]


L = logging.getLogger("sno.tests")


def pytest_addoption(parser):
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


# https://github.com/pytest-dev/pytest/issues/363
@pytest.fixture(scope="session")
def monkeypatch_session(request):
    from _pytest.monkeypatch import MonkeyPatch
    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope='session', autouse=True)
def git_user_config(monkeypatch_session, tmp_path_factory, request):
    home = tmp_path_factory.mktemp('home')

    # override libgit2's search paths
    pygit2.option(pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_SYSTEM, '')
    pygit2.option(pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_XDG, '')
    pygit2.option(pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, str(home))

    # setup environment variables in case we call 'git' commands
    monkeypatch_session.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch_session.setenv("HOME", str(home))
    monkeypatch_session.setenv("GIT_ATTR_NOSYSTEM", "1")
    monkeypatch_session.setenv("GIT_CONFIG_NOSYSTEM", "1")

    USER_NAME = 'Sno Tester'
    USER_EMAIL = 'sno-tester@example.com'

    with open(home / '.gitconfig', 'w') as f:
        f.write(f"[user]\n\tname = {USER_NAME}\n\temail = {USER_EMAIL}\n")

    L.debug("Temporary HOME for git config: %s", home)

    with pytest.raises(IOError):
        pygit2.Config.get_system_config()

    global_cfg = pygit2.Config.get_global_config()
    assert global_cfg['user.email'] == USER_EMAIL
    assert global_cfg['user.name'] == USER_NAME

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


@pytest.fixture
def data_archive(request, tmp_path_factory, chdir):
    """
    Extract a .tgz data archive to a temporary folder.

    When --preserve-data is passed on the command line, a failing test will have the folder kept.

    Context-manager produces the directory path and sets the current working directory.
    """
    incr = 0

    @contextlib.contextmanager
    def _data_archive(name):
        nonlocal incr

        extract_dir = tmp_path_factory.mktemp(request.node.name, str(incr))
        incr += 1
        cleanup = True
        try:
            archive_name = f"{name}.tgz"
            archive_path = Path(__file__).parent / "data" / archive_name
            with tarfile.open(archive_path) as archive:
                archive.extractall(extract_dir)

            L.info("Extracted %s to %s", archive_name, extract_dir)

            # data archive should have a single dir at the top-level, matching the archive name.
            assert (
                len(os.listdir(extract_dir)) == 1
            ), f"Expected {name}/ as the only top-level item in {archive_name}"
            d = extract_dir / name
            assert (
                d.is_dir()
            ), f"Expected {name}/ as the only top-level item in {archive_name}"

            with chdir(d):
                try:
                    yield d
                except Exception:
                    if request.config.getoption("--preserve-data"):
                        L.info(
                            "Not cleaning up %s because --preserve-data was specified",
                            extract_dir,
                        )
                        cleanup = False
                    raise
        finally:
            if cleanup:
                time.sleep(1)
                try:
                    shutil.rmtree(extract_dir)
                except PermissionError as e:
                    L.debug("W: Issue cleaning up temporary folder: %s", e)

    return _data_archive


@pytest.fixture
def data_working_copy(request, data_archive, tmp_path_factory, cli_runner):
    """
    Extract a repo archive with a working copy geopackage
    If the geopackage isn't in the archive, create it via `sno checkout`

    Context-manager produces a 2-tuple: (repository_path, working_copy_path)
    """
    raise pytest.skip()
    from sno.structure import RepositoryStructure
    incr = 0

    @contextlib.contextmanager
    def _data_working_copy(name, force_new=False):
        nonlocal incr

        with data_archive(name) as repo_dir:
            if name.endswith(".sno"):
                name = name[:-5]

            repo = pygit2.Repository(str(repo_dir))
            rs = RepositoryStructure(repo)
            if rs.working_copy:
                wc_path = rs.working_copy.full_path
                if force_new:
                    L.info("force_new is set, deleting existing WC: %s", wc_path)
                    del rs.working_copy
                    assert not rs.working_copy
                    del wc_path

            if not rs.working_copy:
                wc_path = tmp_path_factory.mktemp(request.node.name, str(incr)) / f"{name}.gpkg"
                incr += 1

                L.info("Checking out to %s", wc_path)
                r = cli_runner.invoke(
                    ["checkout", f"--path={wc_path}"]
                )
                assert r.exit_code == 0, r
                L.debug("Checkout result: %s", r)

            del rs
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
    L = logging.getLogger('data_imported')
    incr = 0

    def _data_imported(archive, source_gpkg, table, version):
        nonlocal incr

        params = [archive, source_gpkg, table, version]
        cache_key = f"data_imported~{'~'.join(params)}"

        repo_path = Path(request.config.cache.makedir(cache_key)) / "data.sno"
        if repo_path.exists():
            L.info("Found cache at %s", repo_path)
            return str(repo_path)

        with data_archive(archive) as data:
            import_path = tmp_path_factory.mktemp(request.node.name, str(incr)) / "data"
            incr += 1

            import_path.mkdir()
            with chdir(import_path):
                r = cli_runner.invoke(
                    ["init"]
                )
                assert r.exit_code == 0, r

                repo = pygit2.Repository(str(import_path))
                assert repo.is_bare
                assert repo.is_empty
                repo.free()
                del repo

                r = cli_runner.invoke(
                    ["import", f"GPKG:{data / source_gpkg}:{table}", f"--version={version}", "mytable"]
                )
                assert r.exit_code == 0, r

            time.sleep(1)
            shutil.copytree(import_path, repo_path)
            L.info("Created cache at %s", repo_path)
            return str(repo_path)

    return _data_imported


@pytest.fixture
def geopackage():
    """ Return a SQLite3 (APSW) db connection for the specified DB, with spatialite loaded """

    def _geopackage(path, **kwargs):
        from sno import spatialite_path
        from sno.gpkg import Row

        db = apsw.Connection(str(path), **kwargs)
        db.setrowtrace(Row)
        dbcur = db.cursor()
        dbcur.execute("PRAGMA foreign_keys = ON;")
        db.enableloadextension(True)
        dbcur.execute("SELECT load_extension(?)", (spatialite_path,))
        dbcur.execute("SELECT EnableGpkgMode();")
        return db

    return _geopackage


class SnoCliRunner(CliRunner):
    def __init__(self, *args, in_pdb=False, **kwargs):
        self._in_pdb = in_pdb
        super().__init__(*args, **kwargs)

    def invoke(self, args=None, **kwargs):
        from sno.cli import cli

        if args:
            # force everything to strings (eg. PathLike objects, numbers)
            args = [str(a) for a in args]

        L.debug("Invoking Click command: %s (%s)", args, kwargs)

        params = {"catch_exceptions": not self._in_pdb}
        params.update(kwargs)

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
                return self.isolation_pdb()

        return super().isolation(input=input, env=env, color=color)

    @contextlib.contextmanager
    def isolation_pdb(self):
        s = io.BytesIO(b"{stdout not captured because --pdb-trace}")
        yield (s, not self.mix_stderr and s)


@pytest.fixture
def cli_runner(request):
    """ A wrapper round Click's test CliRunner to improve usefulness """
    return SnoCliRunner(
        # sno.cli._execvp() looks for this env var to prevent fork/exec in tests.
        env={"_SNO_NO_EXEC": "1"},
        # workaround Click's environment isolation so debugging works.
        in_pdb=request.config.getoption("--pdb-trace"),
    )


@pytest.helpers.register
def helpers():
    return TestHelpers


class TestHelpers:
    # Test Dataset (gpkg-points / points)
    POINTS_LAYER = "nz_pa_points_topo_150k"
    POINTS_LAYER_PK = "fid"
    POINTS_INSERT = f"""
        INSERT INTO {POINTS_LAYER}
                        (fid, geom, t50_fid, name_ascii, macronated, name)
                    VALUES
                        (:fid, GeomFromEWKT(:geom), :t50_fid, :name_ascii, :macronated, :name);
    """
    POINTS_RECORD = {
        "fid": 9999,
        "geom": "POINT(0 0)",
        "t50_fid": 9_999_999,
        "name_ascii": "Te Motu-a-kore",
        "macronated": False,
        "name": "Te Motu-a-kore",
    }
    POINTS_HEAD_SHA = "2a1b7be8bdef32aea1510668e3edccbc6d454852"
    POINTS_HEAD1_SHA = "63a9492dd785b1f04dfc446330fa017f9459db4f"
    POINTS_ROWCOUNT = 2143

    # Test Dataset (gpkg-polygons / polygons)
    POLYGONS_LAYER = "nz_waca_adjustments"
    POLYGONS_LAYER_PK = "id"
    POLYGONS_INSERT = f"""
        INSERT INTO {POLYGONS_LAYER}
                        (id, geom, date_adjusted, survey_reference, adjusted_nodes)
                    VALUES
                        (:id, GeomFromEWKT(:geom), :date_adjusted, :survey_reference, :adjusted_nodes);
    """
    POLYGONS_RECORD = {
        "id": 9_999_999,
        "geom": "POLYGON((0 0, 0 0.001, 0.001 0.001, 0.001 0, 0 0))",
        "date_adjusted": "2019-07-05T13:04:00+01:00",
        "survey_reference": "Null Island™ 🗺",
        "adjusted_nodes": 123,
    }
    POLYGONS_HEAD_SHA = "1fb58eb54237c6e7bfcbd7ea65dc999a164b78ec"
    POLYGONS_ROWCOUNT = 228

    # Test Dataset (gpkg-spec / table)

    TABLE_LAYER = "countiestbl"
    TABLE_LAYER_PK = "OBJECTID"
    TABLE_INSERT = f"""
        INSERT INTO {TABLE_LAYER}
                        (OBJECTID, NAME, STATE_NAME, STATE_FIPS, CNTY_FIPS, FIPS, AREA, POP1990, POP2000, POP90_SQMI, Shape_Leng, Shape_Area)
                    VALUES
                        (:OBJECTID, :NAME, :STATE_NAME, :STATE_FIPS, :CNTY_FIPS, :FIPS, :AREA, :POP1990, :POP2000, :POP90_SQMI, :Shape_Leng, :Shape_Area);
    """
    TABLE_RECORD = {
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
    TABLE_HEAD_SHA = "03622015ea5a82bc75228de052d9c84bc6f41667"

    @classmethod
    def last_change_time(cls, db, table=POINTS_LAYER):
        """
        Get the last change time from the GeoPackage DB.
        This is the same as the commit time.
        """
        return db.cursor().execute(
            f"SELECT last_change FROM gpkg_contents WHERE table_name=?;",
            [table],
        ).fetchone()[0]

    @classmethod
    def row_count(cls, db, table):
        return db.cursor().execute(f'SELECT COUNT(*) FROM "{table}";').fetchone()[0]

    @classmethod
    def clear_working_copy(cls, repo_path="."):
        """ Delete any existing working copy & associated config """
        repo = pygit2.Repository(repo_path)
        if "sno.workingcopy.path" in repo.config:
            print(f"Deleting existing working copy: {repo.config['sno.workingcopy.path']}")
            working_copy = Path(repo.config['sno.workingcopy.path'])
            if working_copy.exists():
                working_copy.unlink()
            del repo.config['sno.workingcopy.path']
            del repo.config['sno.workingcopy.version']

    @classmethod
    def db_table_hash(cls, db, table, pk=None):
        """ Calculate a SHA1 hash of the contents of a SQLite table """
        if pk is None:
            pk = "ROWID"

        sql = f"SELECT * FROM {table} ORDER BY {pk};"
        r = db.execute(sql)
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
        param_ids = re.match(r'.*\[(.+)\]$', request.node.nodeid).group(1).split('-')
        return tuple(param_ids)

    @classmethod
    def verify_gpkg_extent(cls, db, table):
        """ Check the aggregate layer extent from the table matches the values in gpkg_contents """
        dbcur = db.cursor()
        r = dbcur.execute(
            """SELECT column_name FROM "gpkg_geometry_columns" WHERE table_name=?;""",
            [table]
        ).fetchone()
        geom_col = r[0] if r else None

        gpkg_extent = tuple(dbcur.execute(
            """SELECT min_x,min_y,max_x,max_y FROM "gpkg_contents" WHERE table_name=?;""",
            [table]
        ).fetchone())

        if geom_col:
            layer_extent = tuple(dbcur.execute(
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
            ).fetchone())
            assert gpkg_extent == pytest.approx(layer_extent)
        else:
            assert gpkg_extent == (None, None, None, None)


@pytest.fixture
def insert(request, cli_runner):
    H = pytest.helpers.helpers()

    def func(db, layer=None, commit=True, reset_index=None):
        if reset_index is not None:
            func.index = reset_index

        if layer is None:
            # autodetect
            layer = db.cursor().execute(
                "SELECT table_name FROM gpkg_contents WHERE table_name IN (?,?,?) LIMIT 1",
                [H.POINTS_LAYER, H.POLYGONS_LAYER, H.TABLE_LAYER],
            ).fetchone()[0]

        if layer == H.POINTS_LAYER:
            rec = H.POINTS_RECORD.copy()
            pk_field = H.POINTS_LAYER_PK
            sql = H.POINTS_INSERT
            pk_start = 98000
        elif layer == H.POLYGONS_LAYER:
            rec = H.POLYGONS_RECORD.copy()
            pk_field = H.POLYGONS_LAYER_PK
            sql = H.POLYGONS_INSERT
            pk_start = 98000
        elif layer == H.TABLE_LAYER:
            rec = H.TABLE_RECORD.copy()
            pk_field = H.TABLE_LAYER_PK
            sql = H.TABLE_INSERT
            pk_start = 98000
        else:
            raise NotImplementedError(f"Layer {layer}")

        # th
        new_pk = pk_start + func.index
        rec[pk_field] = new_pk

        with db:
            cur = db.cursor()
            cur.execute(sql, rec)
            assert db.changes() == 1
            func.inserted_fids.append(new_pk)

        func.index += 1

        if commit:
            r = cli_runner.invoke(["commit", "-m", f"commit-{func.index}"])
            assert r.exit_code == 0, r

            commit_id = r.stdout.splitlines()[-1].split(": ")[1]
            return commit_id
        else:
            return new_pk

    func.index = 0
    func.inserted_fids = []

    return func
