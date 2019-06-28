import contextlib
import io
import logging
import os
import shutil
import sqlite3
import tarfile
from pathlib import Path

import pytest
from click.testing import CliRunner

import pygit2

L = logging.getLogger("snowdrop.tests")


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

    @contextlib.contextmanager
    def _data_archive(name):
        extract_dir = tmp_path_factory.mktemp(request.node.name)
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
                shutil.rmtree(extract_dir)

    return _data_archive


@pytest.fixture
def data_working_copy(data_archive, tmp_path, cli_runner):
    """
    Extract a repo archive with a working copy geopackage
    If the geopackage isn't in the archive, create it via `kxgit checkout`

    Context-manager produces a 2-tuple: (repository_path, working_copy_path)
    """

    @contextlib.contextmanager
    def _data_working_copy(name, force_new=False):
        with data_archive(name) as repo_dir:
            if name.endswith(".git"):
                name = name[:-4]

            wc_path = repo_dir / f"{name}.gpkg"
            if wc_path.exists():
                if force_new:
                    wc_path.unlink()
                    repo = pygit2.Repository(str(repo_dir))
                    del repo.config["kx.workingcopy"]
                else:
                    L.info("Existing working copy at: %s", wc_path)

            if not wc_path.exists():
                wc_path = tmp_path / f"{name}.gpkg"

                # find the layer in the repo
                repo = pygit2.Repository(str(repo_dir))
                tree = repo.head.peel(pygit2.Tree)
                layer = tree[0].name

                L.info("Checking out %s to %s", layer, wc_path)
                r = cli_runner.invoke(
                    ["checkout", f"--working-copy={wc_path}", f"--layer={layer}"]
                )
                assert r.exit_code == 0, r
                L.debug("Checkout result: %s", r)

            yield repo_dir, wc_path

    return _data_working_copy


@pytest.fixture
def geopackage():
    """ Return a sqlite3 db connection for the specified DB, with spatialite loaded """

    def _geopackage(path, **kwargs):
        db = sqlite3.connect(path, **kwargs)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON;")
        db.enable_load_extension(True)
        db.execute("SELECT load_extension('mod_spatialite');")
        return db

    return _geopackage


class SnowdropCliRunner(CliRunner):
    def __init__(self, *args, in_pdb=False, **kwargs):
        self._in_pdb = in_pdb
        super().__init__(*args, **kwargs)

    def invoke(self, args=None, **kwargs):
        from snowdrop.cli import cli

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
        yield (
            s,
            not self.mix_stderr and s
        )


@pytest.fixture
def cli_runner(request):
    """ A wrapper round Click's test CliRunner to improve usefulness """
    return SnowdropCliRunner(
        # snowdrop.cli._execvp() looks for this env var to prevent fork/exec in tests.
        env={"_SNOWDROP_NO_EXEC": "1"},
        # workaround Click's environment isolation so debugging works.
        in_pdb=request.config.getoption("--pdb-trace")
    )
