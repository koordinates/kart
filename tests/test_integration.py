import hashlib
import re
import sqlite3
import subprocess
from pathlib import Path

import pytest  # noqa

import pygit2

from snowdrop import cli

""" Simple integration/E2E tests """

POINTS_LAYER = "nz_pa_points_topo_150k"
POINTS_LAYER_PK = "fid"
POINTS_INSERT = f"""
    INSERT INTO {POINTS_LAYER}
                    (fid, geom, t50_fid, name_ascii, macronated, name)
                VALUES
                    (:fid, AsGPB(GeomFromEWKT(:geom)), :t50_fid, :name_ascii, :macronated, :name);
"""
POINTS_RECORD = {
    'fid': 9999,
    'geom': 'POINT(0 0)',
    't50_fid': 9999999,
    'name_ascii': 'Te Motu-a-kore',
    'macronated': False,
    'name': 'Te Motu-a-kore',
}


def _last_change_time(db):
    """
    Get the last change time from the GeoPackage DB.
    This is the same as the commit time.
    """
    return db.execute(f"SELECT last_change FROM gpkg_contents WHERE table_name=?;", [POINTS_LAYER]).fetchone()[0]


def _clear_working_copy(repo_path="."):
    """ Delete any existing working copy & associated config """
    repo = pygit2.Repository(repo_path)
    if 'kx.workingcopy' in repo.config:
        print(f"Deleting existing working copy: {repo.config['kx.workingcopy']}")
        fmt, working_copy, layer = repo.config["kx.workingcopy"].split(':')
        working_copy = Path(working_copy)
        if working_copy.exists():
            working_copy.unlink()
        del repo.config['kx.workingcopy']


@pytest.mark.slow
def test_import_geopackage(data_archive, tmp_path, cli_runner):
    """ Import the GeoPackage (eg. `kx-foo-layer.gpkg`) into a kxgit repository. """
    with data_archive("gpkg-points") as data:
        repo_path = tmp_path / "data.git"
        r = cli_runner.invoke(
            [
                f"--repo={repo_path}",
                "import-gpkg",
                data / "nz-pa-points-topo-150k.gpkg",
                POINTS_LAYER,
            ]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "HEAD").exists()


def test_checkout_workingcopy(data_archive, tmp_path, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_archive("points.git"):
        _clear_working_copy()

        wc = tmp_path / "data.gpkg"
        r = cli_runner.invoke(
            ["checkout", f"--layer={POINTS_LAYER}", f"--working-copy={wc}"]
        )
        assert r.exit_code == 0, r

    assert wc.exists()
    db = geopackage(wc)
    nrows = db.execute(f"SELECT COUNT(*) FROM {POINTS_LAYER};").fetchone()[0]
    assert nrows > 0


def test_diff(data_working_copy, geopackage, cli_runner):
    """ diff the working copy against the repository (no index!) """
    with data_working_copy("points.git") as (repo, wc):
        db = geopackage(wc)
        with db:
            db.execute(POINTS_INSERT, POINTS_RECORD)

        r = cli_runner.invoke(['diff'])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "+++ {new feature}",
            "+                                      fid = 9999",
            "+                                     geom = POINT(...)",
            "+                                  t50_fid = 9999999",
            "+                               name_ascii = Te Motu-a-kore",
            "+                               macronated = 0",
            "+                                     name = Te Motu-a-kore",
        ]


def test_commit(data_working_copy, geopackage, cli_runner):
    """ commit outstanding changes from the working copy """
    with data_working_copy("points.git") as (repo, wc):
        db = geopackage(wc)
        with db:
            db.execute(POINTS_INSERT, POINTS_RECORD)

        r = cli_runner.invoke(['commit', '-m', 'test-commit-1'])
        assert r.exit_code == 0, r
        commit_id = r.stdout.splitlines()[-1].split(": ")[1]
        print("commit:", commit_id)

        r = pygit2.Repository(str(repo))
        assert str(r.head.target) == commit_id


def test_log(data_archive, cli_runner):
    """ review commit history """
    with data_archive("points.git"):
        r = cli_runner.invoke(['log'])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "commit d1bee0841307242ad7a9ab029dc73c652b9f74f3",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Thu Jun 20 15:28:33 2019 +0100",
            "",
            "    Improve naming on Coromandel East coast",
            "",
            "commit edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00",
            "Author: Robert Coup <robert@coup.net.nz>",
            "Date:   Tue Jun 11 12:03:58 2019 +0100",
            "",
            "    Import from nz-pa-points-topo-150k.gpkg",
        ]


def test_push(data_archive, tmp_path, cli_runner):
    with data_archive("points.git") as repo:
        subprocess.run(['git', 'init', '--bare', tmp_path], check=True)
        subprocess.run(['git', 'remote', 'add', 'myremote', tmp_path], check=True)

        r = cli_runner.invoke(['push', '--set-upstream', 'myremote', 'master'])
        assert r.exit_code == 0, r


def test_checkout_detached(data_working_copy, cli_runner, geopackage):
    """ Checkout a working copy to edit """
    with data_working_copy("points.git") as (repo_dir, wc):
        db = geopackage(wc)
        assert _last_change_time(db) == '2019-06-20T14:28:33.000000Z'

        # checkout the previous commit
        r = cli_runner.invoke(['checkout', 'edd5a4b02a7d2ce608f1839eea5e3a8ddb874e00'])
        assert r.exit_code == 0, r

        assert _last_change_time(db) == '2019-06-11T11:03:58.000000Z'


def test_checkout_references(data_working_copy, cli_runner, geopackage):
    with data_working_copy("points.git") as (repo_dir, wc):
        db = geopackage(wc)

        # checkout the HEAD commit
        r = cli_runner.invoke(['checkout', 'HEAD'])
        assert r.exit_code == 0, r

        assert _last_change_time(db) == '2019-06-20T14:28:33.000000Z'

        # checkout the HEAD-but-1 commit
        r = cli_runner.invoke(['checkout', 'HEAD~1'])
        assert r.exit_code == 0, r

        assert _last_change_time(db) == '2019-06-11T11:03:58.000000Z'

        # checkout the master HEAD via branch-name
        r = cli_runner.invoke(['checkout', 'master'])
        assert r.exit_code == 0, r

        assert _last_change_time(db) == '2019-06-20T14:28:33.000000Z'

        # checkout a short-sha commit
        r = cli_runner.invoke(['checkout', 'edd5a4b'])
        assert r.exit_code == 0, r

        assert _last_change_time(db) == '2019-06-11T11:03:58.000000Z'

        # checkout the master HEAD via refspec
        r = cli_runner.invoke(['checkout', 'refs/heads/master'])
        assert r.exit_code == 0, r

        assert _last_change_time(db) == '2019-06-20T14:28:33.000000Z'


def test_version(cli_runner):
    r = cli_runner.invoke(['--version'])
    assert r.exit_code == 0, r
    assert re.match(r'^kxgit proof of concept\nGDAL v\d\.\d+\.\d+.*?\nPyGit2 v\d\.\d+\.\d+[^;]*; Libgit2 v\d\.\d+\.\d+.*$', r.stdout)


def test_clone(data_archive, tmp_path, cli_runner, monkeypatch):
    with data_archive("points.git") as remote_path:
        with monkeypatch.context() as m:
            m.chdir(tmp_path)
            r = cli_runner.invoke(['clone', remote_path])

            repo_path = tmp_path / 'points.git'
            assert repo_path.is_dir()

        subprocess.check_call(["git", "-C", str(repo_path), "config", "--local", "--list"])

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty
        assert repo.head.name == "refs/heads/master"

        branch = repo.branches.local[repo.head.shorthand]
        assert branch.is_checked_out()
        assert branch.is_head()
        assert branch.upstream_name == "refs/remotes/origin/master"

        assert len(repo.remotes) == 1
        remote = repo.remotes['origin']
        assert remote.url == str(remote_path)
        assert remote.fetch_refspecs == ['+refs/heads/*:refs/remotes/origin/*']


def test_geopackage_locking_edit(data_working_copy, geopackage, cli_runner, monkeypatch):
    with data_working_copy('points.git') as (repo, wc):
        db = geopackage(wc)

        is_checked = False
        orig_func = cli._diff_feature_to_dict

        def _wrap(*args, **kwargs):
            print("hi!", args, kwargs)
            nonlocal is_checked
            if not is_checked:
                with pytest.raises(sqlite3.OperationalError, match=r'database is locked'):
                    db.execute("UPDATE gpkg_context SET table_name=table_name;")
                is_checked = True

            return orig_func(*args, **kwargs)

        monkeypatch.setattr(cli, '_diff_feature_to_dict', _wrap)

        r = cli_runner.invoke(['checkout', 'edd5a4b'])
        assert r.exit_code == 0, r
        assert is_checked

        assert _last_change_time(db) == '2019-06-11T11:03:58.000000Z'


# TODO:
# * `kxgit branch` & `kxgit checkout -b` branch management
# * `kxgit fetch` fetch upstream changes.
# * `kxgit merge` merge. Supports `--ff`/`--no-ff`/`--ff-only` from one merge source.
# * `git reset --soft {commitish}`
# * `git tag ...`
