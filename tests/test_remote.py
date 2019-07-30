import subprocess

import pytest

import pygit2


H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "working_copy",
    [
        pytest.param(True, id="with-wc"),
        pytest.param(False, id="without-wc"),
    ],
)
def test_clone(working_copy, data_archive, tmp_path, cli_runner, chdir, geopackage):
    with data_archive("points.snow") as remote_path:
        with chdir(tmp_path):

            r = cli_runner.invoke([
                "clone",
                remote_path,
                ("--checkout" if working_copy else "--no-checkout"),
            ])

            repo_path = tmp_path / "points.snow"
            assert repo_path.is_dir()

        r = subprocess.check_output(
            ["git", "-C", str(repo_path), "config", "--local", "--list"]
        )
        print("git config file:", r.decode('utf-8').splitlines())

        repo = pygit2.Repository(str(repo_path))
        assert repo.is_bare
        assert not repo.is_empty
        assert repo.head.name == "refs/heads/master"
        assert repo.head.peel(pygit2.Commit).hex == H.POINTS_HEAD_SHA

        branch = repo.branches.local[repo.head.shorthand]
        assert branch.is_checked_out()
        assert branch.is_head()
        assert branch.upstream_name == "refs/remotes/origin/master"

        assert len(repo.remotes) == 1
        remote = repo.remotes["origin"]
        assert remote.url == str(remote_path)
        assert remote.fetch_refspecs == ["+refs/heads/*:refs/remotes/origin/*"]

        wc = (repo_path / f"{repo_path.stem}.gpkg")
        if working_copy:
            assert wc.exists() and wc.is_file()

            table = H.POINTS_LAYER
            assert repo.config["kx.workingcopy"] == f"GPKG:{wc.name}:{table}"

            db = geopackage(wc)
            nrows = db.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
            assert nrows > 0

            wc_tree_id = db.execute(
                "SELECT value FROM __kxg_meta WHERE table_name=? AND key='tree';", [table]
            ).fetchone()[0]
            assert wc_tree_id == repo.head.peel(pygit2.Tree).hex
        else:
            assert not wc.exists()


def test_clone_layer(data_archive, tmp_path, cli_runner, chdir, geopackage):
    with data_archive("points.snow") as remote_path:
        with chdir(tmp_path):
            url = f"file://{remote_path}#BOBTHEBUILDER"
            r = cli_runner.invoke([
                "clone",
                url,
            ])
            assert r.exit_code == 1
            assert "Couldn't find layer 'BOBTHEBUILDER' to checkout." in r.stdout

            url = f"file://{remote_path}#{H.POINTS_LAYER}"
            r = cli_runner.invoke([
                "clone",
                url,
                "bob.land"
            ])
            assert r.exit_code == 2

            url = f"file://{remote_path}#{H.POINTS_LAYER}"
            r = cli_runner.invoke([
                "clone",
                url,
                "bob.snow"
            ])
            assert r.exit_code == 0

            repo_path = tmp_path / "bob.snow"
            assert (repo_path / 'HEAD').exists()


def test_fetch(
    data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request
):
    with data_working_copy("points.snow") as (path1, wc):
        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        db = geopackage(wc)
        commit_id = insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

    with data_working_copy("points.snow") as (path2, wc):
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
    data_archive,
    data_working_copy,
    geopackage,
    cli_runner,
    insert,
    tmp_path,
    request,
    chdir,
):
    with data_working_copy("points.snow") as (path1, wc1), data_working_copy(
        "points.snow"
    ) as (path2, wc2):
        with chdir(path1):
            subprocess.run(["git", "init", "--bare", tmp_path], check=True)
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
