import json
import subprocess

import pytest

from kart.sqlalchemy.gpkg import Db_GPKG
from kart.exceptions import NO_REPOSITORY
from kart.repo import KartRepoState, KartRepo
from kart.structs import CommitWithReference

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
    commit = jdict["kart.status/v2"]["commit"]
    abbrev_commit = jdict["kart.status/v2"]["abbrevCommit"]
    assert commit and abbrev_commit
    assert commit.startswith(abbrev_commit)
    return commit, abbrev_commit


def test_status(
    data_archive,
    data_working_copy,
    cli_runner,
    insert,
    tmp_path,
    request,
    disable_editor,
):
    with data_working_copy("points") as (path1, wc):
        assert text_status(cli_runner) == [
            "On branch main",
            "",
            "Nothing to commit, working copy clean",
        ]

        parts_status = {
            "tabular": {"location": str(wc), "type": "gpkg", "status": "ok"},
            "workdir": {"status": "notFound"},
        }
        assert json_status(cli_runner) == {
            "kart.status/v2": {
                "commit": H.POINTS.HEAD_SHA,
                "abbrevCommit": H.POINTS.HEAD_SHA[:7],
                "branch": "main",
                "upstream": None,
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }

        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r

        assert text_status(cli_runner) == [
            f"HEAD detached at {H.POINTS.HEAD1_SHA[:7]}",
            "",
            "Nothing to commit, working copy clean",
        ]
        assert json_status(cli_runner) == {
            "kart.status/v2": {
                "commit": H.POINTS.HEAD1_SHA,
                "abbrevCommit": H.POINTS.HEAD1_SHA[:7],
                "branch": None,
                "upstream": None,
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        with Db_GPKG.create_engine(wc).connect() as db:
            insert(db)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "main"])
        assert r.exit_code == 0, r

        assert text_status(cli_runner) == [
            "On branch main",
            "Your branch is up to date with 'myremote/main'.",
            "",
            "Nothing to commit, working copy clean",
        ]
        jdict = json_status(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "kart.status/v2": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "main",
                "upstream": {
                    "branch": "myremote/main",
                    "ahead": 0,
                    "behind": 0,
                },
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }

    with data_working_copy("points") as (path2, wc):
        engine = Db_GPKG.create_engine(wc)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["fetch", "myremote"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["branch", "--set-upstream-to=myremote/main"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-fetch")

        assert text_status(cli_runner) == [
            "On branch main",
            "Your branch is behind 'myremote/main' by 1 commit, and can be fast-forwarded.",
            '  (use "kart pull" to update your local branch)',
            "",
            "Nothing to commit, working copy clean",
        ]

        parts_status = {
            "tabular": {"location": str(wc), "type": "gpkg", "status": "ok"},
            "workdir": {"status": "notFound"},
        }
        assert json_status(cli_runner) == {
            "kart.status/v2": {
                "commit": H.POINTS.HEAD_SHA,
                "abbrevCommit": H.POINTS.HEAD_SHA[:7],
                "branch": "main",
                "upstream": {
                    "branch": "myremote/main",
                    "ahead": 0,
                    "behind": 1,
                },
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }

        # local commit
        with engine.connect() as db:
            insert(db, reset_index=100)

        H.git_graph(request, "post-commit")

        assert text_status(cli_runner) == [
            "On branch main",
            "Your branch and 'myremote/main' have diverged,",
            "and have 1 and 1 different commits each, respectively.",
            '  (use "kart pull" to merge the remote branch into yours)',
            "",
            "Nothing to commit, working copy clean",
        ]
        jdict = json_status(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "kart.status/v2": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "main",
                "upstream": {
                    "branch": "myremote/main",
                    "ahead": 1,
                    "behind": 1,
                },
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }

        r = cli_runner.invoke(["merge", "myremote/main"])
        assert r.exit_code == 0, r

        H.git_graph(request, "post-merge")

        assert text_status(cli_runner) == [
            "On branch main",
            "Your branch is ahead of 'myremote/main' by 2 commits.",
            '  (use "kart push" to publish your local commits)',
            "",
            "Nothing to commit, working copy clean",
        ]
        jdict = json_status(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "kart.status/v2": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "main",
                "upstream": {
                    "branch": "myremote/main",
                    "ahead": 2,
                    "behind": 0,
                },
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }

        # local edits
        with engine.connect() as db:
            insert(db, commit=False)
            db.execute(f"DELETE FROM {H.POINTS.LAYER} WHERE fid <= 2;")
            db.execute(f"UPDATE {H.POINTS.LAYER} SET name='test0' WHERE fid <= 5;")

        assert text_status(cli_runner) == [
            "On branch main",
            "Your branch is ahead of 'myremote/main' by 2 commits.",
            '  (use "kart push" to publish your local commits)',
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            f"  {H.POINTS.LAYER}:",
            "    feature:",
            "      1 inserts",
            "      3 updates",
            "      2 deletes",
        ]

        jdict = json_status(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert jdict == {
            "kart.status/v2": {
                "commit": commit,
                "abbrevCommit": abbrev_commit,
                "branch": "main",
                "upstream": {
                    "branch": "myremote/main",
                    "ahead": 2,
                    "behind": 0,
                },
                "spatialFilter": None,
                "workingCopy": {
                    "parts": parts_status,
                    "changes": {
                        "nz_pa_points_topo_150k": {
                            "feature": {
                                "inserts": 1,
                                "updates": 3,
                                "deletes": 2,
                            },
                        }
                    },
                },
            }
        }


def test_status_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "wiz"
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r

    with chdir(repo_path):
        assert text_status(cli_runner) == [
            "Empty repository.",
            '  (use "kart import" to add some data)',
        ]

        parts_status = {
            "tabular": {"location": "wiz.gpkg", "type": "gpkg", "status": "notFound"},
            "workdir": {"status": "notFound"},
        }
        assert json_status(cli_runner) == {
            "kart.status/v2": {
                "commit": None,
                "abbrevCommit": None,
                "branch": "main",
                "upstream": None,
                "spatialFilter": None,
                "workingCopy": {"parts": parts_status, "changes": {}},
            }
        }


def test_status_none(tmp_path, cli_runner, chdir):
    with chdir(tmp_path):
        r = cli_runner.invoke(["status"])
        assert r.exit_code == NO_REPOSITORY, r
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Current directory is not an existing Kart repository"
        )

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == NO_REPOSITORY, r
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Current directory is not an existing Kart repository"
        )


def test_status_merging(data_archive, cli_runner):
    with data_archive("conflicts/points.tgz") as repo_path:
        repo = KartRepo(repo_path)
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r

        assert repo.state == KartRepoState.MERGING
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
            "View conflicts with `kart conflicts` and resolve them with `kart resolve`.",
            "Once no conflicts remain, complete this merge with `kart merge --continue`.",
            "Or use `kart merge --abort` to return to the previous state.",
        ]

        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")
        assert json_status(cli_runner) == {
            "kart.status/v2": {
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
                "spatialFilter": None,
            }
        }


def test_status_untracked_tables(data_working_copy, cli_runner):
    new_table = "test_table"
    with data_working_copy("points") as (path, wc):
        repo = KartRepo(path)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                f"""CREATE TABLE IF NOT EXISTS {new_table} (test_id int, test_name text);"""
            )

        r = cli_runner.invoke(["status", "--list-untracked-tables", "-o", "json"])
        parts_status = {
            "tabular": {"location": str(wc), "type": "gpkg", "status": "ok"},
            "workdir": {"status": "notFound"},
            }
        
        assert json.loads(r.stdout) == {
        "kart.status/v2": {
            "commit": H.POINTS.HEAD_SHA,
            "abbrevCommit": H.POINTS.HEAD_SHA[:7],
            "branch": "main",
            "upstream": None,
            "spatialFilter": None,
            "workingCopy": {"parts": parts_status, "changes": {}, "untrackedTables": [new_table]}
            }            
        }
        
        r = cli_runner.invoke(["status", "--list-untracked-tables"])
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Nothing to commit, working copy clean",
            "",
            "Untracked tables:",
            f"  {new_table}"
        ]