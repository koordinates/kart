from pathlib import Path

import pygit2
import pytest


H = pytest.helpers.helpers()
patches = Path(__file__).parent / "data" / "patches"


def test_apply_invalid_patch(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["apply", patches / 'invalid.snopatch'])
        assert r.exit_code == 1, r
        assert 'Failed to parse JSON patch file' in r.stdout


def test_apply_empty_patch(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["apply", patches / 'points-empty.snopatch'])
        assert r.exit_code == 44, r
        assert 'No changes to commit' in r.stdout


def _test_apply_points(repo_dir, cli_runner):
    r = cli_runner.invoke(["apply", patches / 'updates-only.snopatch'])
    assert r.exit_code == 0, r

    repo = pygit2.Repository(str(repo_dir))
    commit = repo.head.peel(pygit2.Commit)

    # the author details all come from the patch, including timestamp
    assert commit.message == 'Change the Coromandel'
    assert commit.author.name == 'Someone'
    assert commit.author.time == 1561040913
    assert commit.author.offset == 60

    # the committer timestamp doesn't come from the patch
    assert commit.committer.time > commit.author.time
    return r


def test_apply_with_no_working_copy(data_archive, cli_runner):
    with data_archive("points") as repo_dir:
        r = _test_apply_points(repo_dir, cli_runner)
        bits = r.stdout.split()
        assert bits[0] == 'Commit'


def test_apply_with_working_copy(data_working_copy, geopackage, cli_runner):
    with data_working_copy("points") as (repo_dir, wc_path):
        r = _test_apply_points(repo_dir, cli_runner)
        bits = r.stdout.split()
        assert bits[0] == 'Commit'
        assert bits[2] == 'Updating'

        db = geopackage(wc_path)
        with db:
            cur = db.cursor()
            cur.execute(
                f"""
                SELECT name FROM {H.POINTS_LAYER} WHERE {H.POINTS_LAYER_PK} = 1095;
            """
            )
            name = cur.fetchone()[0]
            assert name is None
