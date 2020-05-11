import json
import pytest

import pygit2

from sno.conflicts import ConflictIndex
from sno.exceptions import NOT_YET_IMPLEMENTED

H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points",),
        pytest.param(H.POLYGONS, id="polygons",),
        pytest.param(H.TABLE, id="table"),
    ],
)
@pytest.mark.parametrize(
    "output_format", ["text", "json"],
)
def test_merge_conflicts_dryrun(
    data, output_format, data_working_copy, geopackage, cli_runner, update, insert
):
    sample_pks = data.SAMPLE_PKS
    with data_working_copy(data.ARCHIVE) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        ancestor_commit_id = repo.head.target.hex

        # new branch
        r = cli_runner.invoke(["checkout", "-b", "alternate"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/alternate"

        db = geopackage(wc)
        update(db, sample_pks[0], "aaa")
        update(db, sample_pks[1], "aaa")
        update(db, sample_pks[2], "aaa")
        update(db, sample_pks[3], "aaa")
        update(db, sample_pks[4], "aaa")
        alternate_commit_id = insert(db, reset_index=1, insert_str="insert_aaa")

        assert repo.head.target.hex == alternate_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != alternate_commit_id

        update(db, sample_pks[1], "aaa")
        update(db, sample_pks[2], "mmm")
        update(db, sample_pks[3], "mmm")
        update(db, sample_pks[4], "mmm")
        update(db, sample_pks[5], "mmm")
        master_commit_id = insert(db, reset_index=1, insert_str="insert_mmm")
        assert repo.head.target.hex == master_commit_id

        r = cli_runner.invoke(["merge", "alternate", "--dry-run", f"--{output_format}"])

        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.split("\n") == [
                'Merging branch "alternate" into master',
                "Conflicts found:",
                "",
                f"{data.LAYER}:",
                "  Feature conflicts:",
                "    edit/edit: 3",
                "    other: 1",
                "",
                "(Not actually merging due to --dry-run)",
                "",
            ]
        else:
            jdict = json.loads(r.stdout)
            assert jdict == {
                "sno.merge/v1": {
                    "ancestor": {"commit": ancestor_commit_id},
                    "ours": {"branch": "master", "commit": master_commit_id,},
                    "theirs": {"branch": "alternate", "commit": alternate_commit_id,},
                    "dryRun": True,
                    "message": "Merge branch \"alternate\" into master",
                    "conflicts": {
                        data.LAYER: {"featureConflicts": {"edit/edit": 3, "other": 1}},
                    },
                },
            }


def test_conflict_index_roundtrip(data_working_copy, geopackage, cli_runner, update):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    sample_pks = H.POLYGONS.SAMPLE_PKS
    with data_working_copy(H.POLYGONS.ARCHIVE) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))
        repo = pygit2.Repository(str(repo_path))
        base_commit_id = repo.head.target.hex

        # new branch
        r = cli_runner.invoke(["checkout", "-b", "alternate"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/alternate"

        db = geopackage(wc)
        update(db, sample_pks[0], "aaa")
        update(db, sample_pks[1], "aaa")
        update(db, sample_pks[2], "aaa")
        update(db, sample_pks[3], "aaa")
        alternate_commit_id = update(db, sample_pks[4], "aaa")

        assert repo.head.target.hex == alternate_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != alternate_commit_id

        update(db, sample_pks[1], "aaa")
        update(db, sample_pks[2], "mmm")
        update(db, sample_pks[3], "mmm")
        update(db, sample_pks[4], "mmm")
        master_commit_id = update(db, sample_pks[5], "mmm")

        ancestor_id = repo.merge_base(alternate_commit_id, master_commit_id)
        assert ancestor_id.hex == base_commit_id

        merge_index = repo.merge_trees(
            repo[ancestor_id], repo[alternate_commit_id], repo[master_commit_id]
        )
        assert merge_index.conflicts

        # Create a ConflictIndex object, and roundtrip it into a tree and back.
        orig = ConflictIndex(merge_index)
        orig.write("conflict.index")
        r1 = ConflictIndex.read("conflict.index")
        assert r1 is not orig
        assert r1 == orig

        # Simulate resolving a conflict:
        key, conflict = next(iter(r1.conflicts.items()))
        r1.remove_conflict(key)
        r1.add(conflict.ours)  # Accept our change
        assert r1 != orig

        # Roundtrip again
        r1.write("conflict.index")
        r2 = ConflictIndex.read("conflict.index")
        assert r2 == r1
