import json
from pathlib import Path

import pygit2
import pytest


H = pytest.helpers.helpers()
patches = Path(__file__).parent / "data" / "patches"


@pytest.mark.parametrize('input', ['{}', 'this isnt json'])
def test_apply_invalid_patch(input, data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["apply", '-'], input=input)
        assert r.exit_code == 1, r
        assert 'Failed to parse JSON patch file' in r.stderr


def test_apply_empty_patch(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["apply", patches / 'points-empty.snopatch'])
        assert r.exit_code == 44, r
        assert 'No changes to commit' in r.stderr


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
                SELECT name FROM {H.POINTS.LAYER} WHERE {H.POINTS.LAYER_PK} = 1095;
                """
            )
            name = cur.fetchone()[0]
            assert name is None


def test_apply_with_no_working_copy_with_no_commit(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            ["apply", "--no-commit", patches / 'updates-only.snopatch']
        )
        assert r.exit_code == 45
        assert '--no-commit requires a working copy' in r.stderr


def test_apply_with_working_copy_with_no_commit(
    data_working_copy, geopackage, cli_runner
):
    with data_working_copy("points") as (repo_dir, wc_path):
        r = cli_runner.invoke(
            ["apply", "--no-commit", patches / 'updates-only.snopatch']
        )
        assert r.exit_code == 0
        assert r.stdout.startswith('Updating ')
        # check it was actually applied to the working copy
        db = geopackage(wc_path)
        with db:
            cur = db.cursor()
            cur.execute(
                f"""
                SELECT name FROM {H.POINTS.LAYER} WHERE {H.POINTS.LAYER_PK} = 1095;
                """
            )
            name = cur.fetchone()[0]
            assert name is None

        # Check that the working copy is now dirty, and that the `sno diff --json`
        # output is the same as our original patch file had.
        r = cli_runner.invoke(['diff', '--json'])
        assert r.exit_code == 0
        working_copy_diff = json.loads(r.stdout)['sno.diff/v1']

        with open(patches / 'updates-only.snopatch', encoding='utf-8') as patch_file:
            patch_diff = json.load(patch_file)['sno.diff/v1']

        assert working_copy_diff == patch_diff
