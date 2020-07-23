import json
from pathlib import Path

import pygit2
import pytest
from sno.exceptions import NO_TABLE, PATCH_DOES_NOT_APPLY, INVALID_OPERATION


H = pytest.helpers.helpers()
patches = Path(__file__).parent / "data" / "patches"


@pytest.mark.parametrize('input', ['{}', 'this isnt json'])
def test_apply_invalid_patch(input, data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["apply", '-'], input=input)
        assert r.exit_code == 1, r
        assert 'Failed to parse JSON patch file' in r.stderr


def test_apply_empty_patch(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["apply", patches / 'points-empty.snopatch'])
        assert r.exit_code == 44, r
        assert 'No changes to commit' in r.stderr


def test_apply_with_wrong_dataset_name(data_archive, cli_runner):
    patch_data = json.dumps(
        {
            'sno.diff/v1+hexwkb': {
                'wrong-name': {'featureChanges': [], 'metaChanges': [],}
            }
        }
    )
    with data_archive("points"):
        r = cli_runner.invoke(["apply", '-'], input=patch_data)
        assert r.exit_code == NO_TABLE, r
        assert (
            "Patch contains dataset 'wrong-name' which is not in this repository"
            in r.stderr
        )


def test_apply_twice(data_archive, cli_runner):
    patch_path = patches / 'points-1U-1D-1I.snopatch'
    with data_archive("points"):
        r = cli_runner.invoke(["apply", str(patch_path)])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["apply", str(patch_path)])
        assert r.exit_code == PATCH_DOES_NOT_APPLY

        assert (
            'nz_pa_points_topo_150k: Trying to delete nonexistent feature: 1241'
            in r.stdout
        )
        assert (
            'nz_pa_points_topo_150k: Trying to create feature that already exists: 9999'
            in r.stdout
        )
        assert (
            'nz_pa_points_topo_150k: Trying to update already-changed feature: 1795'
            in r.stdout
        )
        assert 'Patch does not apply' in r.stderr


def test_apply_with_no_working_copy(data_archive, cli_runner):
    patch_filename = 'updates-only.snopatch'
    message = 'Change the Coromandel'
    author = {'name': 'Someone', 'time': 1561040913, 'offset': 60}
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

        # Check that the `sno show -o json` output is the same as our original patch file had.
        r = cli_runner.invoke(['show', '-o', 'json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open('r', encoding='utf-8'))

        assert patch['sno.patch/v1'] == original_patch['sno.patch/v1']
        assert patch['sno.diff/v1+hexwkb'] == original_patch['sno.diff/v1+hexwkb']


def test_apply_meta_changes(data_archive, cli_runner):
    patch_file = json.dumps(
        {
            "sno.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "meta": {
                        "title": {"-": "NZ Pa Points (Topo, 1:50k)", "+": "new title:",}
                    }
                },
            },
            "sno.patch/v1": {
                "authorEmail": "robert@example.com",
                "authorName": "Robert Coup",
                "authorTime": "2019-06-20T14:28:33Z",
                "authorTimeOffset": "+01:00",
                "message": "Change the title",
            },
        }
    )
    with data_archive("points"):
        # this won't work, v1 doesn't support this patch
        r = cli_runner.invoke(["apply", '-'], input=patch_file,)
        assert r.exit_code == INVALID_OPERATION, r
    with data_archive("points2"):
        r = cli_runner.invoke(["apply", '-'], input=patch_file,)
        assert r.exit_code == 0, r

        # Check that the `sno show -o json` output is the same as our original patch file had.
        r = cli_runner.invoke(['show', '-o', 'json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        meta = patch['sno.diff/v1+hexwkb']['nz_pa_points_topo_150k']['meta']
        assert meta == {'title': {'+': 'new title:', '-': 'NZ Pa Points (Topo, 1:50k)'}}


def test_apply_with_working_copy(
    data_working_copy, geopackage, cli_runner,
):
    patch_filename = 'updates-only.snopatch'
    message = 'Change the Coromandel'
    author = {'name': 'Someone', 'time': 1561040913, 'offset': 60}
    workingcopy_verify_names = {1095: None}
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

        # Check that the `sno show -o json` output is the same as our original patch file had.
        r = cli_runner.invoke(['show', '-o', 'json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open('r', encoding='utf-8'))

        assert patch['sno.patch/v1'] == original_patch['sno.patch/v1']
        assert patch['sno.diff/v1+hexwkb'] == original_patch['sno.diff/v1+hexwkb']


def test_apply_with_no_working_copy_with_no_commit(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(
            ["apply", "--no-commit", patches / 'updates-only.snopatch']
        )
        assert r.exit_code == 45
        assert '--no-commit requires a working copy' in r.stderr


def test_apply_with_working_copy_with_no_commit(
    data_working_copy, geopackage, cli_runner
):
    patch_filename = 'updates-only.snopatch'
    message = 'Change the Coromandel'
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
        r = cli_runner.invoke(['diff', '-o', 'json'])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open('r', encoding='utf-8'))

        assert patch['sno.diff/v1+hexwkb'] == original_patch['sno.diff/v1+hexwkb']


def test_apply_multiple_dataset_patch_roundtrip(data_archive, cli_runner):
    with data_archive("au-census"):
        r = cli_runner.invoke(["show", "-o", "json", "master"])
        assert r.exit_code == 0, r
        patch_text = r.stdout
        patch_json = json.loads(patch_text)
        assert set(patch_json['sno.diff/v1+hexwkb'].keys()) == {
            'census2016_sdhca_ot_ra_short',
            'census2016_sdhca_ot_sos_short',
        }

        # note: repo's current branch is 'branch1' which doesn't have the commit on it,
        # so the patch applies cleanly.
        r = cli_runner.invoke(["apply", "-"], input=patch_text)
        assert r.exit_code == 0, r

        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r
        new_patch_json = json.loads(r.stdout)

        assert new_patch_json == patch_json


@pytest.mark.slow
def test_apply_benchmark(
    data_working_copy, geopackage, benchmark, cli_runner, monkeypatch
):
    from sno import apply

    with data_working_copy('points') as (repo_dir, wc_path):
        # Create a branch we can use later; don't switch to it
        r = cli_runner.invoke(["branch", "-c", "savepoint"])
        assert r.exit_code == 0, r

        # Generate a large change and commit it
        db = geopackage(wc_path)
        cursor = db.cursor()
        cursor.execute(
            "UPDATE nz_pa_points_topo_150k SET name = 'bulk_' || Coalesce(name, 'null')"
        )
        r = cli_runner.invoke(["commit", "-m", "rename everything"])
        assert r.exit_code == 0, r

        # Make it into a patch
        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r
        patch_text = r.stdout
        patch_json = json.loads(patch_text)
        assert patch_json['sno.patch/v1']['message'] == "rename everything"

        # Now switch to our savepoint branch and apply the patch
        r = cli_runner.invoke(["checkout", "savepoint"])
        assert r.exit_code == 0, r

        # wrap the apply command with benchmarking
        orig_apply_patch = apply.apply_patch

        def _benchmark_apply(*args, **kwargs):
            # one round/iteration isn't very statistical, but hopefully crude idea
            return benchmark.pedantic(
                orig_apply_patch, args=args, kwargs=kwargs, rounds=1, iterations=1
            )

        monkeypatch.setattr(apply, 'apply_patch', _benchmark_apply)

        cli_runner.invoke(["apply", "-"], input=patch_text)
