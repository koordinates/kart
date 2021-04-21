import json
import subprocess

import pytest

from sno.exceptions import NO_REPOSITORY
from sno.repo import SnoRepo


H = pytest.helpers.helpers()


def text_branches(cli_runner):
    r = cli_runner.invoke(["branch"])
    assert r.exit_code == 0, r
    return r.stdout.splitlines()


def json_branches(cli_runner):
    r = cli_runner.invoke(["branch", "-o", "json"])
    assert r.exit_code == 0, r
    return json.loads(r.stdout)


def get_commit_ids(jdict):
    commit = jdict["kart.branch/v1"]["branches"]["main"]["commit"]
    abbrev_commit = jdict["kart.branch/v1"]["branches"]["main"]["abbrevCommit"]
    assert commit and abbrev_commit
    assert commit.startswith(abbrev_commit)
    return commit, abbrev_commit


def test_branches(
    data_archive, data_working_copy, cli_runner, insert, tmp_path, request
):
    with data_working_copy("points") as (path1, wc):
        assert text_branches(cli_runner) == ["* main"]
        assert json_branches(cli_runner) == {
            "kart.branch/v1": {
                "current": "main",
                "branches": {
                    "main": {
                        "commit": "0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                        "abbrevCommit": "0c64d82",
                        "branch": "main",
                        "upstream": None,
                    }
                },
            }
        }

        r = cli_runner.invoke(["checkout", "HEAD~1"])
        assert r.exit_code == 0, r

        assert text_branches(cli_runner) == ["* (HEAD detached at 7bc3b56)", "  main"]
        assert json_branches(cli_runner) == {
            "kart.branch/v1": {
                "current": None,
                "branches": {
                    "main": {
                        "commit": "0c64d8211c072a08d5fc6e6fe898cbb59fc83d16",
                        "abbrevCommit": "0c64d82",
                        "branch": "main",
                        "upstream": None,
                    }
                },
            }
        }

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r

        subprocess.run(["git", "init", "--bare", str(tmp_path)], check=True)

        r = cli_runner.invoke(["remote", "add", "myremote", tmp_path])
        assert r.exit_code == 0, r

        repo = SnoRepo(path1)
        with repo.working_copy.session() as sess:
            insert(sess)

        r = cli_runner.invoke(["push", "--set-upstream", "myremote", "main"])
        assert r.exit_code == 0, r

        assert text_branches(cli_runner) == ["* main"]
        jdict = json_branches(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert json_branches(cli_runner) == {
            "kart.branch/v1": {
                "current": "main",
                "branches": {
                    "main": {
                        "commit": commit,
                        "abbrevCommit": abbrev_commit,
                        "branch": "main",
                        "upstream": {
                            "branch": "myremote/main",
                            "ahead": 0,
                            "behind": 0,
                        },
                    }
                },
            }
        }

        r = cli_runner.invoke(["checkout", "-b", "newbie"])
        assert r.exit_code == 0, r

        assert text_branches(cli_runner) == ["  main", "* newbie"]
        jdict = json_branches(cli_runner)
        commit, abbrev_commit = get_commit_ids(jdict)  # This varies from run to run.
        assert json_branches(cli_runner) == {
            "kart.branch/v1": {
                "current": "newbie",
                "branches": {
                    "main": {
                        "commit": commit,
                        "abbrevCommit": abbrev_commit,
                        "branch": "main",
                        "upstream": {
                            "branch": "myremote/main",
                            "ahead": 0,
                            "behind": 0,
                        },
                    },
                    "newbie": {
                        "commit": commit,
                        "abbrevCommit": abbrev_commit,
                        "branch": "newbie",
                        "upstream": None,
                    },
                },
            }
        }


def test_branches_empty(tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "wiz"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0, r

    with chdir(repo_path):
        assert text_branches(cli_runner) == []

        assert json_branches(cli_runner) == {
            "kart.branch/v1": {"current": None, "branches": {}}
        }


def test_branches_none(tmp_path, cli_runner, chdir):
    with chdir(tmp_path):
        r = cli_runner.invoke(["branch"])
        assert r.exit_code == NO_REPOSITORY, r
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Current directory is not an existing Kart repository"
        )

        r = cli_runner.invoke(["branch", "-o", "json"])
        assert r.exit_code == NO_REPOSITORY, r
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Current directory is not an existing Kart repository"
        )
