import argparse
import contextlib
import hashlib
import io
import json
import logging
import os
import random
import re
import shutil
import tarfile
import time
import uuid
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime
from pathlib import Path
import pytest

# this sets up a bunch of package/lib/venv-related stuff
import kart

import click
from click.testing import CliRunner
import pygit2
import sqlalchemy

# Pytest sets up its own keyboard interrupt handler - don't mess with that.
os.environ["NO_CONFIGURE_PROCESS_CLEANUP"] = "1"  # noqa

from kart.diff_estimation import terminate_estimate_thread
from kart.geometry import Geometry  # noqa: E402
from kart.repo import KartRepo  # noqa: E402
from kart.sqlalchemy.postgis import Db_Postgis  # noqa: E402
from kart.sqlalchemy.sqlserver import Db_SqlServer  # noqa: E402
from kart.sqlalchemy.mysql import Db_MySql  # noqa: E402
from kart import subprocess_util as subprocess


pytest_plugins = ["helpers_namespace"]


L = logging.getLogger("kart.tests")


def pytest_addoption(parser):
    # pytest.ini sets --numprocesses=auto for parallelism in local dev.
    # But in CI we disable xdist because it causes a crash in windows builds.
    # (simply doing --numprocesses=0 is insufficient; the plugin needs to be
    # disabled completely via -p no:xdist)
    # However, there's no way to *remove* an option that's in pytest.ini's addopts.
    xdist_parser = next((g for g in parser._groups if g.name == "xdist"), None)
    if xdist_parser:
        if not any(o for o in xdist_parser.options if "--numprocesses" in o._long_opts):
            # So here we just define the option so it parses, and then ignore it.
            parser.addoption(
                "--numprocesses",
                action="store",
                default=0,
                help=argparse.SUPPRESS,
            )
        if not any(o for o in xdist_parser.options if "--dist" in o._long_opts):
            # do the same thing for --dist
            parser.addoption(
                "--dist",
                action="store",
                default="no",
                help=argparse.SUPPRESS,
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
    """Deterministic "random" UUID generator seeded from the test ID"""
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

        if os.name == "posix":
            if os.geteuid() == 0:
                # running as root (container) - disable git ownership checks
                f.write("[safe]\n" "\tdirectory = *\n")

    L.debug("Temporary HOME for git config: %s", home)

    with pytest.raises(IOError):
        pygit2.Config.get_system_config()

    global_cfg = pygit2.Config.get_global_config()
    assert global_cfg["user.email"] == USER_EMAIL
    assert global_cfg["user.name"] == USER_NAME

    return (USER_EMAIL, USER_NAME, home)


@contextlib.contextmanager
def chdir_(path):
    """Context manager to change the current working directory"""
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


def is_within_directory(directory, target):
    """
    Returns a boolean indicating whether the target path exists and is contained inside
    the given directory.

    Relative paths are resolved to absolute paths first. Symlinks are *not* traversed.
    """
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)

    prefix = os.path.commonprefix([abs_directory, abs_target])

    return prefix == abs_directory


def safe_tar_extract(tar, path=".", members=None, *, numeric_owner=False):
    """
    Extracts a tar file, but raises ValueError if any of the files will be extract outside
    the given extraction directory (by default, the current working directory)

    This is similar to TarFile.extractall(), but it avoids CVE-2007-4559
    https://github.com/advisories/GHSA-gw9q-c7gh-j9vm
    """
    for member in tar.getmembers():
        member_path = os.path.join(path, member.name)
        if not is_within_directory(path, member_path):
            raise ValueError(
                f"Attempted Path Traversal in Tar File (path={member_path})"
            )

    tar.extractall(path, members, numeric_owner=numeric_owner)


def extract_archive(archive_path, extract_dir):
    archive_path = get_archive_path(archive_path)
    with tarfile.open(archive_path) as archive:
        safe_tar_extract(archive, extract_dir)

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


@pytest.fixture(scope="session")
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
            repo = KartRepo(repo_dir)
            if repo.working_copy.tabular:
                table_wc_path = repo.working_copy.tabular.full_path
                if force_new:
                    L.info("force_new is set, deleting existing WC: %s", table_wc_path)
                    repo.working_copy.delete_tabular()
                    assert not hasattr(repo.working_copy, "_tabular")
                    del table_wc_path

            if not repo.working_copy.tabular:
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

                repo = KartRepo(import_path)
                assert repo.head_is_unborn
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


class KartCliRunner(CliRunner):
    def __init__(self, *args, in_pdb=False, mix_stderr=False, **kwargs):
        self._in_pdb = in_pdb
        super().__init__(*args, mix_stderr=mix_stderr, **kwargs)

    def invoke(self, args=None, **kwargs):
        from kart.cli import load_commands_from_args, cli

        if args:
            # force everything to strings (eg. PathLike objects, numbers)
            args = [str(a) for a in args]

        L.debug("Invoking Click command: %s (%s)", args, kwargs)

        params = {"catch_exceptions": not self._in_pdb}
        params.update(kwargs)

        load_commands_from_args(args, skip_first_arg=False)

        terminate_estimate_thread.clear()

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
    """A wrapper round Click's test CliRunner to improve usefulness"""
    return KartCliRunner(
        # kart.subprocess_util.run looks for this env var to ensure output is captured in tests.
        env={"_KART_RUN_WITH_CAPTURE": "1"},
        # workaround Click's environment isolation so debugging works.
        in_pdb=request.config.getoption("--pdb-trace"),
    )


@pytest.helpers.register
def helpers():
    return TestHelpers


@pytest.helpers.register
def get_env_flag(env_var, ci_default=True):
    """
    Helper to parse environment flags used in tests. This is used to
    assert optional features are present/absent in CI, or ignored in dev.

    ci_default is the value to return if $CI is set and the environment variable
    is not.

    Usage:

        expect_foo = pytest.helpers.get_env_flag("KART_EXPECT_FOO"):
        if expect_foo is not None:
            assert has_foo == expect_foo
        elif not has_foo:
            pytest.skip("foo is not available")
    """
    if env_var in os.environ:
        try:
            return bool(int(os.environ[env_var]))
        except ValueError as e:
            raise ValueError(
                f"${env_var} should be set to 0 or 1, was {os.environ[env_var]!r}"
            )
    elif "CI" in os.environ:
        return ci_default
    else:
        return None


@pytest.helpers.register
def feature_assert_or_skip(name, env_var, has_feature, ci_require=True):
    """
    Helper to enable conditional behaviour in tests based on feature detection.

    This is used to assert optional features are present/absent in CI, or
    ignored in dev using environment variables.

    * if ${env_var}=1, or $CI and ci_require=True: fail if has_feature=False
    * if ${env_var}=0, or $CI and ci_require=False: fail if has_feature=True
    * else skip if has_feature = False

    Usage:

        # figure out whether we have foo
        has_foo = ...
        pytest.helpers.feature_assert_or_skip("foo", "KART_EXPECT_FOO", has_foo)

        # test things that depend on foo being available
    """
    __tracebackhide__ = True

    expect_feature = get_env_flag(env_var, ci_default=ci_require)

    if expect_feature is not None:
        if has_feature ^ expect_feature:
            via = f"${env_var}"
            if ci_require is not None:
                via += "/$CI"

            if has_feature:
                pytest.fail(f"{name}: not expected (via {via}) but available")
            else:
                pytest.fail(f"{name}: expected (via {via}) but not available")

    if not has_feature:
        pytest.skip(f"{name}: not available")


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
        DATE_TIME = datetime.strptime(
            "Thu Jun 20 15:28:33 2019 +0100", "%a %b %d %H:%M:%S %Y %z"
        ).strftime("%c %z")
        DATE_TIME1 = datetime.strptime(
            "Tue Jun 11 12:03:58 2019 +0100", "%a %b %d %H:%M:%S %Y %z"
        ).strftime("%c %z")
        HEAD_SHA = "1582725544d9122251acd4b3fc75b5c88ac3fd17"
        HEAD1_SHA = "6e2984a28150330a6c51019a70f9e8fcfe405e8c"
        HEAD_TREE_SHA = "42b63a2a7c1b5dfe9c21ff9884b59f198e421821"
        HEAD1_TREE_SHA = "622e7cc3b54cd54493eed6c4c5abe35d4bfa168e"
        ROWCOUNT = 2143
        TEXT_FIELD = "name"
        SAMPLE_PKS = list(range(1, 11))
        NEXT_UNASSIGNED_PK = 1174

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
            "date_adjusted": "2019-07-05T13:04:00",
            "survey_reference": "Null Island™ 🗺",
            "adjusted_nodes": 123,
        }
        DATE_TIME = datetime.strptime(
            "Mon Jul 22 12:05:39 2019 +0100", "%a %b %d %H:%M:%S %Y %z"
        ).strftime("%c %z")
        HEAD_SHA = "3f7166eebd11876a9b473a67ed2f66a200493b69"
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
        NEXT_UNASSIGNED_PK = 4423294

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
        HEAD_SHA = "f404fcd4ac2a411ef7bb32070e9ffa663374d875"
        ROWCOUNT = 3141
        TEXT_FIELD = "NAME"
        SAMPLE_PKS = list(range(1, 11))
        NEXT_UNASSIGNED_PK = 3142

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
        """Delete any existing working copy & associated config"""
        repo = KartRepo(repo_path)
        table_wc = repo.working_copy.get_tabular(allow_invalid_state=True)
        if table_wc:
            print(
                f"Deleting existing tabular working copy: {repo.workingcopy_location}"
            )
            table_wc.delete()

        if repo.WORKINGCOPY_LOCATION_KEY in repo.config:
            del repo.config[repo.WORKINGCOPY_LOCATION_KEY]

    @classmethod
    def db_table_hash(cls, conn, table, pk=None):
        """Calculate a SHA1 hash of the contents of a SQLite table"""
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
        """Print a pretty graph of recent git revisions"""
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
    def get_git_datetime(cls, sha: str) -> str:
        cmd = ["git", "show", "-s", "--format=%cd", sha]
        date = subprocess.check_output(cmd)
        return date.decode("utf-8").strip()

    @classmethod
    def parameter_ids(cls, request):
        """Get an array of parameter IDs"""
        # nodeid = 'test_import_feature_performance[0.2.0-spec-counties-table]'
        param_ids = re.match(r".*\[(.+)\]$", request.node.nodeid).group(1).split("-")
        return tuple(param_ids)

    @classmethod
    def verify_gpkg_extent(cls, conn, table):
        """Check the aggregate layer extent from the table matches the values in gpkg_contents"""
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

    def func(
        conn, layer=None, commit=True, reset_index=None, with_pk=None, insert_str=None
    ):
        if reset_index is not None:
            func.index = reset_index

        layer = layer or _find_layer(conn)

        metadata = H.metadata(layer)
        rec = metadata.RECORD.copy()
        pk_field = metadata.LAYER_PK
        sql = metadata.INSERT
        pk_start = 98000

        if with_pk is not None:
            new_pk = with_pk
        else:
            new_pk = pk_start + func.index
            func.index += 1

        rec[pk_field] = new_pk
        if insert_str:
            rec[metadata.TEXT_FIELD] = insert_str

        r = conn.execute(sql, rec)
        assert r.rowcount == 1
        func.inserted_fids.append(new_pk)

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
        insert_cmd = working_copy.insert_into_dataset_cmd(dataset)

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
        insert_cmd = working_copy.insert_into_dataset_cmd(dataset)

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
        insert_cmd = working_copy.insert_into_dataset_cmd(dataset)

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
def disable_editor():
    old_environ = dict(os.environ)
    os.environ["GIT_EDITOR"] = "echo"
    yield
    os.environ.clear()
    os.environ.update(old_environ)


def is_postgis_installed(engine):
    # Run a query to see if PostGIS is installed.
    with engine.connect() as conn:
        query = "SELECT * FROM pg_extension WHERE extname = 'postgis';"
        result = conn.execute(query)
        return result.rowcount > 0


@pytest.fixture()
def postgis_db():
    """Using docker, you can run all PostGIS tests - such as test_postgis_import - as follows:
    docker run -it --rm -d -p 15432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgis/postgis
    KART_POSTGIS_URL='postgresql://postgres:@localhost:15432/postgres' pytest -k 'postgres or postgis'
    """

    if "KART_POSTGIS_URL" in os.environ:
        engine = Db_Postgis.create_engine(os.environ["KART_POSTGIS_URL"])
        if not is_postgis_installed(engine):
            raise AssertionError(
                "PostGIS extension not found in KART_POSTGIS_URL database"
            )
        engine.original_url = os.environ.get("KART_POSTGIS_URL")
    elif "KART_POSTGRES_URL" in os.environ:
        engine = Db_Postgis.create_engine(os.environ["KART_POSTGRES_URL"])
        if not is_postgis_installed(engine):
            raise pytest.skip("PostGIS extension not found - can't run PostGIS tests")
        engine.original_url = os.environ.get("KART_POSTGRES_URL")
    else:
        raise pytest.skip(
            "PostGIS tests require configuration - set KART_POSTGIS_URL or KART_POSTGRES_URL environment variable"
        )
    yield engine


@pytest.fixture()
def no_postgis_db():
    """There are a few PostgreSQL tests that test Kart's behaviour in a PG database where PostGIS is *not* installed. You can run these tests using docker as follows:
    docker run -it --rm -d -p 15432:5432 -e POSTGRES_HOST_AUTH_METHOD=trust postgres/postgres
    KART_NO_POSTGIS_URL='postgresql://postgres:@localhost:15432/postgres' pytest -k 'postgres or postgis'

    Note that the the majority of PostgreSQL tests use PostGIS - see the postgis_db fixture above.
    """

    if "KART_NO_POSTGIS_URL" in os.environ:
        engine = Db_Postgis.create_engine(os.environ["KART_NO_POSTGIS_URL"])
        if is_postgis_installed(engine):
            raise AssertionError(
                "PostGIS extension found in KART_NO_POSTGIS_URL database"
            )
        engine.original_url = os.environ.get("KART_NO_POSTGIS_URL")
    elif "KART_POSTGRES_URL" in os.environ:
        engine = Db_Postgis.create_engine(os.environ["KART_POSTGRES_URL"])
        if is_postgis_installed(engine):
            raise pytest.skip("PostGIS extension found - can't run no_postgis_db tests")
        engine.original_url = os.environ.get("KART_POSTGRES_URL")
    else:
        raise pytest.skip(
            "Non-PostGIS tests require configuration - set KART_NO_POSTGIS_URL or KART_POSTGRES_URL environment variable"
        )
    yield engine


@pytest.fixture()
def new_postgis_db_schema(request, postgis_db):
    return _new_pg_schema_for_test(request, postgis_db)


@pytest.fixture()
def new_no_postgis_db_schema(request, no_postgis_db):
    return _new_pg_schema_for_test(request, no_postgis_db)


def _new_pg_schema_for_test(request, engine):
    @contextlib.contextmanager
    def ctx(create=False):
        sha = hashlib.sha1(request.node.nodeid.encode("utf8")).hexdigest()[:20]
        schema = f"kart_test_{sha}"
        with engine.connect() as conn:
            # Start by deleting in case it is left over from last test-run...
            conn.execute(f"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""")
            # Actually create only if create=True, otherwise the test will create it
            if create:
                conn.execute(f"""CREATE SCHEMA "{schema}";""")
        try:
            url = urlsplit(engine.original_url)
            url_path = url.path.rstrip("/") + "/" + schema
            new_schema_url = urlunsplit(
                [url.scheme, url.netloc, url_path, url.query, ""]
            )
            yield new_schema_url, schema
        finally:
            # Clean up - delete it again if it exists.
            with engine.connect() as conn:
                conn.execute(f"""DROP SCHEMA IF EXISTS "{schema}" CASCADE;""")

    return ctx


@pytest.fixture()
def sqlserver_db():
    """
    Using docker, you can run a SQL Server test - such as those in test_working_copy_sqlserver - as follows:
        docker run -it --rm -d -p 11433:1433 -e ACCEPT_EULA=Y -e 'SA_PASSWORD=PassWord1' mcr.microsoft.com/mssql/server
        KART_SQLSERVER_URL='mssql://sa:PassWord1@127.0.0.1:11433/master' pytest -k sqlserver --pdb -vvs
    """
    if "KART_SQLSERVER_URL" not in os.environ:
        raise pytest.skip(
            "SQL Server tests require configuration - read docstring at conftest.sqlserver_db"
        )
    engine = Db_SqlServer.create_engine(os.environ["KART_SQLSERVER_URL"])
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
        schema = f"kart_test_{sha}"
        with sqlserver_db.connect() as conn:
            # Start by deleting in case it is left over from last test-run...
            Db_SqlServer.drop_all_in_schema(conn, schema)
            conn.execute(f"DROP SCHEMA IF EXISTS {schema};")

            # Actually create only if create=True, otherwise the test will create it
            if create:
                conn.execute(f"""CREATE SCHEMA "{schema}";""")
        try:
            url = urlsplit(os.environ["KART_SQLSERVER_URL"])
            url_path = url.path.rstrip("/") + "/" + schema
            new_schema_url = urlunsplit(
                [url.scheme, url.netloc, url_path, url.query, ""]
            )
            yield new_schema_url, schema
        finally:
            # Clean up - delete it again if it exists.
            with sqlserver_db.connect() as conn:
                Db_SqlServer.drop_all_in_schema(conn, schema)
                conn.execute(f"DROP SCHEMA IF EXISTS {schema};")

    return ctx


@pytest.fixture()
def mysql_db():
    """
    Using docker, you can run a MySQL test - such as those in test_working_copy_mysql - as follows:
        docker run -it --rm -d -p 13306:3306 -e MYSQL_ROOT_PASSWORD=PassWord1 mysql
        KART_MYSQL_URL='mysql://root:PassWord1@localhost:13306' pytest -k mysql --pdb -vvs
    """
    if "KART_MYSQL_URL" not in os.environ:
        raise pytest.skip(
            "MySQL tests require configuration - read docstring at conftest.mysql_db"
        )
    engine = Db_MySql.create_engine(os.environ["KART_MYSQL_URL"])
    with engine.connect() as conn:
        # test connection:
        try:
            conn.execute("SELECT @@version;")
        except sqlalchemy.exc.DBAPIError:
            raise pytest.skip("Requires MySQL")
    yield engine


@pytest.fixture()
def new_mysql_db_schema(request, mysql_db):
    @contextlib.contextmanager
    def ctx(create=False):
        sha = hashlib.sha1(request.node.nodeid.encode("utf8")).hexdigest()[:20]
        schema = f"kart_test_{sha}"
        with mysql_db.connect() as conn:
            # Start by deleting in case it is left over from last test-run...
            conn.execute(f"""DROP SCHEMA IF EXISTS `{schema}`;""")
            # Actually create only if create=True, otherwise the test will create it
            if create:
                conn.execute(f"""CREATE SCHEMA `{schema}`;""")
        try:
            url = urlsplit(os.environ["KART_MYSQL_URL"])
            url_path = url.path.rstrip("/") + "/" + schema
            new_schema_url = urlunsplit(
                [url.scheme, url.netloc, url_path, url.query, ""]
            )
            yield new_schema_url, schema
        finally:
            # Clean up - delete it again if it exists.
            with mysql_db.connect() as conn:
                conn.execute(f"""DROP SCHEMA IF EXISTS `{schema}`;""")

    return ctx


USER = os.getenv("USER", "")

DOT_AWS_FILES = {
    os.path.join(f"~{USER}", ".aws", "config"): ["AWS_CONFIG_FILE"],
    os.path.join(f"~{USER}", ".aws", "credentials"): [
        # Either one of these can inform boto3 where to look, but Arbiter only respects the first.
        "AWS_CREDENTIAL_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
    ],
}


def _restore_aws_config_during_testing():
    # $HOME isn't the user's real homedir during tests - look for AWS_CONFIG_FILE in the real homedir,
    # unless AWS_CONFIG_FILE is already set to look somewhere else. Same for AWS_CREDENTIAL_FILE.
    for path, env_vars in DOT_AWS_FILES.items():
        val = any(os.environ.get(k) for k in env_vars)
        if not val:
            path = os.path.expanduser(path)
            if os.path.exists(path):
                val = path
        if not val:
            continue
        for k in env_vars:
            if k not in os.environ:
                os.environ[k] = val


@pytest.fixture()
def s3_test_data_point_cloud(monkeypatch_session):
    """
    You can run tests that fetch a copy of the auckland test data from S3 (and so test Kart's S3 behaviour)
    by setting KART_S3_TEST_DATA_POINT_CLOUD=s3://some-bucket/path-to-auckland-tiles/*.laz
    The tiles hosted there should be the ones found in tests/data/point-cloud/laz-auckland.tgz
    """
    if "KART_S3_TEST_DATA_POINT_CLOUD" not in os.environ:
        raise pytest.skip(
            "S3 tests require configuration - read docstring at conftest.s3_test_data_point_cloud"
        )
    _restore_aws_config_during_testing()
    return os.environ["KART_S3_TEST_DATA_POINT_CLOUD"]


@pytest.fixture()
def dodgy_restore(cli_runner):
    """
    Basically performs a `kart restore --source RESTORE_COMMIT`.
    However, this works even when the actual kart restore would fail due to "structural changes" -
    specifically, there are schema changes that require the table to be deleted and rewritten,
    which prevents us from tracking feature changes. This version makes no attempt to track feature changes.
    So, only use this if you know there are *only* structural changes - no feature changes at all -
     or you just don't care about the tracking table.
    """

    def _dodgy_restore(repo, restore_commit):
        if isinstance(restore_commit, pygit2.Commit):
            restore_commit = restore_commit.hex

        # This works by checking out restore_commit, which likely destroys and recreates WC tables etc -
        # then forcibly setting HEAD back to its previous value without actually updating the WC.

        head_commit = repo.head_commit.hex
        head_tree = repo.head_tree.hex
        r = cli_runner.invoke(["checkout", restore_commit])
        assert r.exit_code == 0, r.stderr
        repo.write_gitdir_file("HEAD", head_commit)
        repo.working_copy.tabular.update_state_table_tree(head_tree)

    return _dodgy_restore


@pytest.fixture(scope="session")
def requires_git_lfs():
    try:
        r = subprocess.run(["git", "lfs", "--version"])
        has_git_lfs = r.returncode == 0
    except OSError:
        has_git_lfs = False

    pytest.helpers.feature_assert_or_skip(
        "Git LFS installed", "KART_EXPECT_GIT_LFS", has_git_lfs, ci_require=False
    )


@pytest.fixture()
def check_lfs_hashes(requires_git_lfs):
    """
    Makes sure that all the files in <GITDIR_PATH>/lfs/objects have the appropriate
    name - ie, they are named after their sha256 hash.
    Also counts them and asserts there are as many unique files as expected.
    """
    LFS_OID_PATTERN = re.compile("[0-9a-fA-F]{64}")

    from kart.lfs_util import get_hash_and_size_of_file

    def _check_lfs_hashes(repo, expected_file_count=None):
        file_count = 0
        for file in (repo.gitdir_path / "lfs" / "objects").glob("**/*"):
            if not file.is_file() or not LFS_OID_PATTERN.fullmatch(file.name):
                continue
            file_count += 1
            file_hash, size = get_hash_and_size_of_file(file)
            assert file_hash == file.name

            odb_hash = pygit2.hashfile(file)
            assert (
                odb_hash not in repo
            ), f"Tile sha256:{file_hash} aka {odb_hash} should not be in ODB"

        if expected_file_count is not None:
            assert file_count == expected_file_count

    return _check_lfs_hashes


@pytest.fixture()
def check_tile_is_reflinked():
    """
    Makes sure that a particular tile is reflinked to the same tile in the LFS cache.
    Makes no asserts at all
    - on windows, where we don't support reflinks.
    - if reflink is not supported on the filesystem.
    - if we lack the tools to check if files are reflinked (clone_checker or )
    """

    import shutil
    import subprocess

    import reflink

    from kart import is_windows
    from kart.lfs_util import get_hash_and_size_of_file, get_local_path_from_lfs_hash

    clone_checker = shutil.which("clone_checker")
    fienode = shutil.which("fienode")
    has_checker = bool(clone_checker or fienode)

    def _check_tile_is_reflinked(tile_path, repo, do_raise_skip=False):
        if is_windows:
            if do_raise_skip:
                raise pytest.skip("Reflink is not supported on windows")
            else:
                return

        reflink_supported = reflink.supported_at(str(repo.workdir_path))
        if not reflink_supported:
            if do_raise_skip:
                raise pytest.skip("Reflink is not supported on this filesystem")
            else:
                return

        if not has_checker:
            if do_raise_skip:
                raise pytest.skip(
                    "Can't check if tiles are reflinked: install dyorgio/apfs-clone-checker or pwaller/fienode to check reflinks"
                )
            else:
                return

        tile_hash, size = get_hash_and_size_of_file(tile_path)
        lfs_path = get_local_path_from_lfs_hash(repo, tile_hash)

        if clone_checker:
            output = subprocess.check_output(
                [clone_checker, str(tile_path), str(lfs_path)], encoding="utf8"
            )
            assert (
                output.strip() == "1"
            ), f"{tile_path} and {lfs_path} should be reflinked"

        elif fienode:
            fienode1 = subprocess.check_output([fienode, str(tile_path)])
            fienode2 = subprocess.check_output([fienode, str(lfs_path)])
            assert (
                fienode1 == fienode2
            ), f"{tile_path} and {lfs_path} should be reflinked"

    return _check_tile_is_reflinked
