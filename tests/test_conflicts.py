import pytest

import pygit2

from sno.conflicts import ConflictIndex
from sno.exceptions import NOT_YET_IMPLEMENTED, MERGE_CONFLICT

H = pytest.helpers.helpers()


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points",),
        pytest.param(H.POLYGONS, id="polygons",),
        pytest.param(H.TABLE, id="table"),
    ],
)
def test_merge_conflicts(data, data_working_copy, geopackage, cli_runner, update):
    sample_pks = data.SAMPLE_PKS
    with data_working_copy(data.ARCHIVE) as (repo_path, wc):
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

        r = cli_runner.invoke(["merge", "alternate"])

        assert r.exit_code == MERGE_CONFLICT, r
        assert "conflict" in r.stdout
        assert base_commit_id in r.stdout
        assert alternate_commit_id in r.stdout
        assert master_commit_id in r.stdout

        assert "Use an interactive terminal to resolve merge conflicts" in r.stderr

        def feature_name(pk):
            return f"{data.LAYER}:{data.LAYER_PK}={pk}"

        # Only modified in alternate:
        assert feature_name(sample_pks[0]) not in r.stdout

        # Modified exactly the same in both branches:
        assert feature_name(sample_pks[1]) not in r.stdout

        # These three are merge conflicts:
        assert feature_name(sample_pks[2]) in r.stdout
        assert feature_name(sample_pks[3]) in r.stdout
        assert feature_name(sample_pks[4]) in r.stdout

        # Only modified in master:
        assert feature_name(sample_pks[5]) not in r.stdout

        r = cli_runner.invoke(["merge", "alternate"], input="a\no\nt\n")
        assert r.exit_code == 0, r
        assert repo.head.target.hex != alternate_commit_id
        assert repo.head.target.hex != master_commit_id

        r = cli_runner.invoke(["diff", master_commit_id])
        assert r.exit_code == 0, r

        # Changed: their version is merged in from alternate automatically:
        assert feature_name(sample_pks[0]) in r.stdout

        # Not changed: our version is the same as their version:
        assert feature_name(sample_pks[1]) not in r.stdout
        # Changed: reverted to ancestor using "a":
        assert feature_name(sample_pks[2]) in r.stdout
        # Not changed: we kept our version using "o":
        assert feature_name(sample_pks[3]) not in r.stdout
        # Changed: accepted their version using "t":
        assert feature_name(sample_pks[4]) in r.stdout
        # Not changed: our version is merged in automatically:
        assert feature_name(sample_pks[5]) not in r.stdout


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(H.POINTS, id="points",),
        pytest.param(H.POLYGONS, id="polygons",),
        pytest.param(H.TABLE, id="table"),
    ],
)
def test_unsupported_merge_conflicts(
    data, data_working_copy, geopackage, cli_runner, insert
):
    with data_working_copy(data.ARCHIVE) as (repo_path, wc):
        repo = pygit2.Repository(str(repo_path))

        # new branch
        r = cli_runner.invoke(["checkout", "-b", "alternate"])
        assert r.exit_code == 0, r
        assert repo.head.name == "refs/heads/alternate"

        db = geopackage(wc)
        alternate_commit_id = insert(db, reset_index=1, insert_str="aaa")

        assert repo.head.target.hex == alternate_commit_id

        r = cli_runner.invoke(["checkout", "master"])
        assert r.exit_code == 0, r
        assert repo.head.target.hex != alternate_commit_id

        master_commit_id = insert(db, reset_index=1, insert_str="mmm")

        r = cli_runner.invoke(["merge", "alternate"])
        assert r.exit_code == NOT_YET_IMPLEMENTED
        assert (
            "resolving conflicts where features are added or removed isn't supported yet"
            in r.stderr
        )


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
