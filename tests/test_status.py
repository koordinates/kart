import subprocess

import pytest


H = pytest.helpers.helpers()


def test_status(
    data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request
):
    with data_working_copy("points") as (path1, wc):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "",
            "Nothing to commit, working copy clean",
        ]

        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "HEAD detached at 63a9492",
            "",
            "Nothing to commit, working copy clean",
        ]

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        db = geopackage(wc)
        insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is up to date with 'myremote/master'.",
            "",
            "Nothing to commit, working copy clean",
        ]

    with data_working_copy("points") as (path2, wc):
        db = geopackage(wc)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["fetch", "myremote"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "--set-upstream-to=myremote/master"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-fetch")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is behind 'myremote/master' by 1 commit, and can be fast-forwarded.",
            '  (use "sno pull" to update your local branch)',
            "",
            "Nothing to commit, working copy clean",
        ]

        # local commit
        insert(db, reset_index=100)

        H.git_graph(request, "post-commit")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch and 'myremote/master' have diverged,",
            "and have 1 and 1 different commits each, respectively.",
            '  (use "sno pull" to merge the remote branch into yours)',
            "",
            "Nothing to commit, working copy clean",
        ]

        r = cli_runner.invoke(["merge", "myremote/master"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is ahead of 'myremote/master' by 2 commits.",
            '  (use "sno push" to publish your local commits)',
            "",
            "Nothing to commit, working copy clean",
        ]

        # local edits
        with db:
            insert(db, commit=False)
            db.execute(f"DELETE FROM {H.POINTS_LAYER} WHERE fid <= 2;")
            db.execute(f"UPDATE {H.POINTS_LAYER} SET name='test0' WHERE fid <= 5;")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "On branch master",
            "Your branch is ahead of 'myremote/master' by 2 commits.",
            '  (use "sno push" to publish your local commits)',
            "",
            "Changes in working copy:",
            '  (use "sno commit" to commit)',
            '  (use "sno reset" to discard changes)',
            "",
            f"  {H.POINTS_LAYER}/",
            "    modified:  3 features",
            "    new:       1 feature",
            "    deleted:   2 features",
        ]


def test_status_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / 'wiz.sno'
    r = cli_runner.invoke(
        ["init", repo_path]
    )
    assert r.exit_code == 0, r

    with chdir(repo_path):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            'Empty repository.',
            '  (use "sno import" to add some data)',
        ]


def test_status_none(tmp_path, cli_runner, chdir):
    with chdir(tmp_path):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 2, r
        assert r.stdout.splitlines()[-1] == 'Error: Invalid value for --repo: Not an existing repository'
