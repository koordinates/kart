import json
import subprocess

import pytest


H = pytest.helpers.helpers()


def text_status(cli_runner):
    r = cli_runner.invoke(["status"])
    assert r.exit_code == 0, r
    return r.stdout.splitlines()


def json_status(cli_runner):
    r = cli_runner.invoke(["status", "--json"])
    assert r.exit_code == 0, r
    return json.loads(r.stdout)


def get_commit(jdict):
    commit = jdict["sno.status/v1"]["commit"]
    assert commit
    return commit


def test_status(
    data_archive, data_working_copy, geopackage, cli_runner, insert, tmp_path, request
):
    with data_working_copy("points") as (path1, wc):
        assert text_status(cli_runner) == [
            "On branch master",
            "",
            "Nothing to commit, working copy clean",
        ]
        assert json_status(cli_runner) == {
            "sno.status/v1": {
                "commit": "2a1b7be",
                "branch": "master",
                "upstream": None,
                "workingCopy": {},
            }
        }

        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r

        assert text_status(cli_runner) == [
            "HEAD detached at 63a9492",
            "",
            "Nothing to commit, working copy clean",
        ]
        assert json_status(cli_runner) == {
            "sno.status/v1": {
                "commit": "63a9492",
                "branch": None,
                "upstream": None,
                "workingCopy": {},
            }
        }

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        subprocess.run(["git", "init", "--bare", tmp_path], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        db = geopackage(wc)
        insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "master"])
        assert r.exit_code == 0, r

        assert text_status(cli_runner) == [
            "On branch master",
            "Your branch is up to date with 'myremote/master'.",
            "",
            "Nothing to commit, working copy clean",
        ]
        jdict = json_status(cli_runner)
        commit = get_commit(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 0, "behind": 0,},
                "workingCopy": {},
            }
        }

    with data_working_copy("points") as (path2, wc):
        db = geopackage(wc)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["fetch", "myremote"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "--set-upstream-to=myremote/master"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-fetch")

        assert text_status(cli_runner) == [
            "On branch master",
            "Your branch is behind 'myremote/master' by 1 commit, and can be fast-forwarded.",
            '  (use "sno pull" to update your local branch)',
            "",
            "Nothing to commit, working copy clean",
        ]
        assert json_status(cli_runner) == {
            "sno.status/v1": {
                "commit": "2a1b7be",
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 0, "behind": 1,},
                "workingCopy": {},
            }
        }

        # local commit
        insert(db, reset_index=100)

        H.git_graph(request, "post-commit")

        assert text_status(cli_runner) == [
            "On branch master",
            "Your branch and 'myremote/master' have diverged,",
            "and have 1 and 1 different commits each, respectively.",
            '  (use "sno pull" to merge the remote branch into yours)',
            "",
            "Nothing to commit, working copy clean",
        ]
        jdict = json_status(cli_runner)
        commit = get_commit(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 1, "behind": 1,},
                "workingCopy": {},
            }
        }

        r = cli_runner.invoke(["merge", "myremote/master"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        assert text_status(cli_runner) == [
            "On branch master",
            "Your branch is ahead of 'myremote/master' by 2 commits.",
            '  (use "sno push" to publish your local commits)',
            "",
            "Nothing to commit, working copy clean",
        ]
        jdict = json_status(cli_runner)
        commit = get_commit(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 2, "behind": 0,},
                "workingCopy": {},
            }
        }

        # local edits
        with db:
            insert(db, commit=False)
            db.cursor().execute(f"DELETE FROM {H.POINTS_LAYER} WHERE fid <= 2;")
            db.cursor().execute(
                f"UPDATE {H.POINTS_LAYER} SET name='test0' WHERE fid <= 5;"
            )

        assert text_status(cli_runner) == [
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

        jdict = json_status(cli_runner)
        commit = get_commit(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 2, "behind": 0,},
                "workingCopy": {
                    "nz_pa_points_topo_150k": {
                        "schemaChanges": None,
                        "featureChanges": {"modified": 3, "new": 1, "deleted": 2,},
                    }
                },
            }
        }


def test_status_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "wiz.sno"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r

    with chdir(repo_path):
        assert text_status(cli_runner) == [
            "Empty repository.",
            '  (use "sno import" to add some data)',
        ]

        assert json_status(cli_runner) == {
            "sno.status/v1": {
                "upstream": None,
                "commit": None,
                "branch": None,
                "workingCopy": None,
            }
        }


def test_status_none(tmp_path, cli_runner, chdir):
    with chdir(tmp_path):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == 2, r
        assert (
            r.stdout.splitlines()[-1]
            == "Error: Current directory is not an existing repository"
        )

        r = cli_runner.invoke(["status", "--json"])
        assert r.exit_code == 2, r
        assert (
            r.stdout.splitlines()[-1]
            == "Error: Current directory is not an existing repository"
        )
