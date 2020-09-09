import subprocess
from pathlib import PureWindowsPath

import pytest
import pygit2

from sno.clone import get_directory_from_url


H = pytest.helpers.helpers()


def test_clone_empty_repo(tmp_path, cli_runner, chdir):
    src = tmp_path / "src"
    subprocess.check_call(["git", "init", "--bare", str(src)])
    r = cli_runner.invoke(["clone", str(src), tmp_path / "dest"])
    assert r.exit_code == 0


def test_get_directory_from_url():
    assert get_directory_from_url("sno@example.com:def/abc") == "abc"
    assert get_directory_from_url("sno@example.com:abc") == "abc"
    assert get_directory_from_url("https://example.com/def/abc") == "abc"
    assert get_directory_from_url("https://example.com/abc") == "abc"
    assert get_directory_from_url("abc") == "abc"
    assert get_directory_from_url("abc/") == "abc"
    assert get_directory_from_url("def/abc") == "abc"
    assert get_directory_from_url("def/abc/") == "abc"
    assert get_directory_from_url("/def/abc") == "abc"
    assert get_directory_from_url("/def/abc/") == "abc"
    assert get_directory_from_url(PureWindowsPath("C:/def/abc")) == "abc"
    assert get_directory_from_url(PureWindowsPath("C:\\def\\abc")) == "abc"
    assert get_directory_from_url(PureWindowsPath("C:\\def\\abc\\")) == "abc"
    assert get_directory_from_url(PureWindowsPath("C:\\def\\abc/")) == "abc"


@pytest.mark.parametrize(
    "working_copy",
    [
        pytest.param(True, id="with-wc"),
        pytest.param(False, id="without-wc"),
    ],
)
def test_clone(
    working_copy, data_archive_readonly, tmp_path, cli_runner, chdir, geopackage
):
    with data_archive_readonly("points") as remote_path:
        with chdir(tmp_path):

            r = cli_runner.invoke(
                [
                    "clone",
                    remote_path,
                    ("--checkout" if working_copy else "--no-checkout"),
                ]
            )

            repo_path = tmp_path / "points"
            assert repo_path.is_dir()

        r = subprocess.check_output(
            ["git", "-C", str(repo_path), "config", "--local", "--list"]
        )
        print("git config file:", r.decode("utf-8").splitlines())

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty
        assert repo.head.name == "refs/heads/master"
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS.HEAD_SHA

        branch = repo.branches.local[repo.head.shorthand]
        assert branch.is_head()
        assert branch.upstream_name == "refs/remotes/origin/master"

        assert len(repo.remotes) == 1
        remote = repo.remotes["origin"]
        assert remote.url == str(remote_path)
        assert remote.fetch_refspecs == ["+refs/heads/*:refs/remotes/origin/*"]

        wc = repo_path / f"{repo_path.stem}.gpkg"
        if working_copy:
            assert wc.exists() and wc.is_file()

            table = H.POINTS.LAYER
            assert repo.config["sno.repository.version"] == "1"
            assert repo.config["sno.workingcopy.path"] == wc.name

            db = geopackage(wc)
            dbcur = db.cursor()
            nrows = dbcur.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            assert nrows > 0

            wc_tree_id = dbcur.execute(
                """SELECT value FROM ".sno-meta" WHERE table_name='*' AND key='tree';""",
            ).fetchone()[0]
            assert wc_tree_id == repo.head.peel(pygit2.Tree).hex
        else:
            assert not wc.exists()


def test_fetch(
    data_archive_readonly,
    data_working_copy,
    geopackage,
    cli_runner,
    insert,
    tmp_path,
    request,
):
    with data_working_copy("points") as (path1, wc):
        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        db = geopackage(wc)
        commit_id = insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

    with data_working_copy("points") as (path2, wc):
        repo = pygit2.Repository(str(path2))
        h = repo.head.target.hex

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["fetch", "myremote"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-fetch")

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == h

        remote_branch = repo.lookup_reference_dwim("myremote/master")
        assert remote_branch.target.hex == commit_id

        fetch_head = repo.lookup_reference("FETCH_HEAD")
        assert fetch_head.target.hex == commit_id

        # merge
        r = cli_runner.invoke(["merge", "myremote/master"])
        assert r.exit_code == 0, r

        assert repo.head.name == "refs/heads/master"
        assert repo.head.target.hex == commit_id
        commit = repo.head.peel(pygit2.Commit)
        assert len(commit.parents) == 1
        assert commit.parents[0].hex == h


def test_pull(
    data_archive_readonly,
    data_working_copy,
    geopackage,
    cli_runner,
    insert,
    tmp_path,
    request,
    chdir,
):
    with data_working_copy("points") as (path1, wc1), data_working_copy("points") as (
        path2,
        wc2,
    ):
        with chdir(path1):
            subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)
            r = cli_runner.invoke(["remote", "add", "origin", tmp_path])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["push", "--set-upstream", "origin", "master"])
            assert r.exit_code == 0, r

        with chdir(path2):
            r = cli_runner.invoke(["remote", "add", "origin", tmp_path])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["fetch", "origin"])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["branch", "--set-upstream-to=origin/master"])
            assert r.exit_code == 0, r

        with chdir(path1):
            db = geopackage(wc1)
            commit_id = insert(db)

            r = cli_runner.invoke(["push"])
            assert r.exit_code == 0, r

        with chdir(path2):
            repo = pygit2.Repository(str(path2))
            h = repo.head.target.hex

            r = cli_runner.invoke(["pull"])
            assert r.exit_code == 0, r

            H.git_graph(request, "post-pull")

            remote_branch = repo.lookup_reference_dwim("origin/master")
            assert remote_branch.target.hex == commit_id

            assert repo.head.name == "refs/heads/master"
            assert repo.head.target.hex == commit_id
            commit = repo.head.peel(pygit2.Commit)
            assert len(commit.parents) == 1
            assert commit.parents[0].hex == h

            # pull again / no-op
            r = cli_runner.invoke(["branch", "--unset-upstream"])
            assert r.exit_code == 0, r

            r = cli_runner.invoke(["pull"])
            assert r.exit_code == 0, r
            assert repo.head.target.hex == commit_id
