import json
import subprocess

import pytest

from sno.exceptions import NO_REPOSITORY
from sno.repo_files import RepoState
from sno.structs import CommitWithReference


H = pytest.helpers.helpers()


def text_status(cli_runner):
    r = cli_runner.invoke(["status"])
    assert r.exit_code == 0, r
    return r.stdout.splitlines()


def json_status(cli_runner):
    r = cli_runner.invoke(["status", "-o", "json"])
    assert r.exit_code == 0, r
    return json.loads(r.stdout)


def get_commit_ids(jdict):
    commit = jdict["sno.status/v1"]["commit"]
    abbrev_commit = jdict["sno.status/v1"]["abbrevCommit"]
    assert commit and abbrev_commit
    assert commit.startswith(abbrev_commit)
    return commit, abbrev_commit


def test_status(
    data_archive,
    data_working_copy,
    geopackage,
    cli_runner,
    insert,
    tmp_path,
    request,
    disable_editor,
):
    with data_working_copy("points") as (path1, wc):
        assert text_status(cli_runner) == [
            "On branch master",
            "",
            "Nothing to commit, working copy clean",
        ]
        assert json_status(cli_runner) == {
            "sno.status/v1": {
                "commit": "2a1b7be8bdef32aea1510668e3edccbc6d454852",
                "abbrevCommit": "2a1b7be",
                "branch": "master",
                "upstream": None,
                "workingCopy": {"path": str(wc), "changes": None},
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
                "commit": "63a9492dd785b1f04dfc446330fa017f9459db4f",
                "abbrevCommit": "63a9492",
                "branch": None,
                "upstream": None,
                "workingCopy": {"path": str(wc), "changes": None},
            }
        }

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)

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
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 0, "behind": 0,},
                "workingCopy": {"path": str(wc), "changes": None},
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
                "commit": "2a1b7be8bdef32aea1510668e3edccbc6d454852",
                "abbrevCommit": "2a1b7be",
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 0, "behind": 1,},
                "workingCopy": {"path": str(wc), "changes": None},
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
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 1, "behind": 1,},
                "workingCopy": {"path": str(wc), "changes": None},
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
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 2, "behind": 0,},
                "workingCopy": {"path": str(wc), "changes": None},
            }
        }

        # local edits
        with db:
            insert(db, commit=False)
            db.cursor().execute(f"DELETE FROM {H.POINTS.LAYER} WHERE fid <= 2;")
            db.cursor().execute(
                f"UPDATE {H.POINTS.LAYER} SET name='test0' WHERE fid <= 5;"
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
            f"  {H.POINTS.LAYER}/",
            "    inserts:   1 feature",
            "    updates:   3 features",
            "    deletes:   2 features",
        ]

        jdict = json_status(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "sno.status/v1": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "master",
                "upstream": {"branch": "myremote/master", "ahead": 2, "behind": 0,},
                "workingCopy": {
                    "path": str(wc),
                    "changes": {
                        "nz_pa_points_topo_150k": {
                            "feature": {"inserts": 1, "updates": 3, "deletes": 2,},
                        }
                    },
                },
            }
        }


def test_status_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "wiz.sno"
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r

    with chdir(repo_path):
        assert text_status(cli_runner) == [
            "Empty repository.",
            '  (use "sno import" to add some data)',
        ]

        assert json_status(cli_runner) == {
            "sno.status/v1": {
                "commit": None,
                "abbrevCommit": None,
                "branch": None,
                "upstream": None,
                "workingCopy": None,
            }
        }


def test_status_none(tmp_path, cli_runner, chdir):
    with chdir(tmp_path):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == NO_REPOSITORY, r
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Current directory is not an existing repository"
        )

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == NO_REPOSITORY, r
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Current directory is not an existing repository"
        )


def test_status_merging(create_conflicts, cli_runner):
    with create_conflicts(H.POINTS) as repo:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r

        assert RepoState.get_state(repo) == RepoState.MERGING
        assert text_status(cli_runner) == [
            "On branch ours_branch",
            "",
            'Repository is in "merging" state.',
            'Merging branch "theirs_branch" into ours_branch',
            "Conflicts:",
            "",
            "nz_pa_points_topo_150k:",
            "    nz_pa_points_topo_150k:feature: 4 conflicts",
            "",
            "",
            "View conflicts with `sno conflicts` and resolve them with `sno resolve`.",
            "Once no conflicts remain, complete this merge with `sno merge --continue`.",
            "Or use `sno merge --abort` to return to the previous state.",
        ]

        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")
        assert json_status(cli_runner) == {
            'sno.status/v1': {
                "abbrevCommit": ours.short_id,
                "commit": ours.id.hex,
                "branch": "ours_branch",
                "upstream": None,
                "state": "merging",
                "merging": {
                    "ancestor": {
                        "abbrevCommit": ancestor.short_id,
                        "commit": ancestor.id.hex,
                    },
                    "ours": {
                        "abbrevCommit": ours.short_id,
                        "commit": ours.id.hex,
                        "branch": "ours_branch",
                    },
                    "theirs": {
                        "abbrevCommit": theirs.short_id,
                        "commit": theirs.id.hex,
                        "branch": "theirs_branch",
                    },
                },
                "conflicts": {"nz_pa_points_topo_150k": {"feature": 4}},
            }
        }
