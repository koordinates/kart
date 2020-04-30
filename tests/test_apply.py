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


@pytest.mark.parametrize(
    'patch_filename,message,author',
    [
        (
            'updates-only.snopatch',
            'Change the Coromandel',
            {'name': 'Someone', 'time': 1561040913, 'offset': 60},
        ),
    ],
)
def test_apply_with_no_working_copy(
    data_archive, cli_runner, patch_filename, message, author
):
    with data_archive("points") as repo_dir:
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_dir))
        commit = repo.head.peel(pygit2.Commit)

        # the author details all come from the patch, including timestamp
        assert commit.message == message
        assert commit.author.name == author['name']
        assert commit.author.time == author['time']
        assert commit.author.offset == author['offset']

        # the committer timestamp doesn't come from the patch
        assert commit.committer.time > commit.author.time
        bits = r.stdout.split()
        assert bits[0] == 'Commit'

        # Check that the `sno show --json` output is the same as our original patch file had.
        r = cli_runner.invoke(['show', '--json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open('r', encoding='utf-8'))

        assert patch['sno.patch/v1'] == original_patch['sno.patch/v1']
        assert patch['sno.diff/v1'] == original_patch['sno.diff/v1']


@pytest.mark.parametrize(
    'patch_filename,message,author,workingcopy_verify_names',
    [
        (
            'updates-only.snopatch',
            'Change the Coromandel',
            {'name': 'Someone', 'time': 1561040913, 'offset': 60},
            {1095: None},
        )
    ],
)
def test_apply_with_working_copy(
    data_working_copy,
    geopackage,
    cli_runner,
    patch_filename,
    message,
    author,
    workingcopy_verify_names,
):
    with data_working_copy("points") as (repo_dir, wc_path):
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_dir))
        commit = repo.head.peel(pygit2.Commit)

        # the author details all come from the patch, including timestamp
        assert commit.message == message
        assert commit.author.name == author['name']
        assert commit.author.time == author['time']
        assert commit.author.offset == author['offset']

        # the committer timestamp doesn't come from the patch
        assert commit.committer.time > commit.author.time
        bits = r.stdout.split()
        assert bits[0] == 'Commit'
        assert bits[2] == 'Updating'

        db = geopackage(wc_path)
        with db:
            cur = db.cursor()
            ids = f"({','.join(str(x) for x in workingcopy_verify_names.keys())})"
            cur.execute(
                f"""
                SELECT {H.POINTS.LAYER_PK}, name FROM {H.POINTS.LAYER} WHERE {H.POINTS.LAYER_PK} IN {ids};
                """
            )
            names = dict(cur.fetchall())
            assert names == workingcopy_verify_names

        # Check that the `sno show --json` output is the same as our original patch file had.
        r = cli_runner.invoke(['show', '--json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open('r', encoding='utf-8'))

        assert patch['sno.patch/v1'] == original_patch['sno.patch/v1']
        assert patch['sno.diff/v1'] == original_patch['sno.diff/v1']


def test_apply_with_no_working_copy_with_no_commit(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(
            ["apply", "--no-commit", patches / 'updates-only.snopatch']
        )
        assert r.exit_code == 45
        assert '--no-commit requires a working copy' in r.stderr


@pytest.mark.parametrize(
    'patch_filename,message', [('updates-only.snopatch', 'Change the Coromandel',)],
)
def test_apply_with_working_copy_with_no_commit(
    data_working_copy, geopackage, cli_runner, patch_filename, message,
):
    with data_working_copy("points") as (repo_dir, wc_path):
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", "--no-commit", patch_path])
        assert r.exit_code == 0, r

        repo = pygit2.Repository(str(repo_dir))

        # no commit was made
        commit = repo.head.peel(pygit2.Commit)
        assert commit.message != message

        bits = r.stdout.split()
        assert bits[0] == 'Updating'

        # Check that the working copy diff is the same as the original patch file
        r = cli_runner.invoke(['diff', '--json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open('r', encoding='utf-8'))

        assert patch['sno.diff/v1'] == original_patch['sno.diff/v1']


def test_apply_multiple_dataset_patch_roundtrip(data_archive, cli_runner):
    with data_archive("au-census"):
        r = cli_runner.invoke(["show", "--json", "master"])
        assert r.exit_code == 0, r
        patch_text = r.stdout
        patch_json = json.loads(patch_text)
        assert set(patch_json['sno.diff/v1'].keys()) == {
            'census2016_sdhca_ot_ra_short',
            'census2016_sdhca_ot_sos_short',
        }

        # note: repo's current branch is 'branch1' which doesn't have the commit on it,
        # so the patch applies cleanly.
        r = cli_runner.invoke(["apply", "-"], input=patch_text)
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["show", "--json"])
        assert r.exit_code == 0, r
        new_patch_json = json.loads(r.stdout)

        assert new_patch_json == patch_json
