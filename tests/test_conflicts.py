import contextlib
import json
import pytest

import pygit2

from sno.conflicts import ConflictIndex, ConflictOutputFormat, list_conflicts
from sno.structs import CommitWithReference
from sno.repo_files import (
    MERGE_HEAD,
    MERGE_MSG,
    MERGE_LABELS,
    MERGE_INDEX,
    repo_file_exists,
    repo_file_path,
    read_repo_file,
)

H = pytest.helpers.helpers()


@pytest.fixture
def create_conflicts(data_working_copy, geopackage, cli_runner, update, insert):
    @contextlib.contextmanager
    def ctx(data):
        with data_working_copy(data.ARCHIVE) as (repo_path, wc):
            repo = pygit2.Repository(str(repo_path))
            sample_pks = data.SAMPLE_PKS

            cli_runner.invoke(["checkout", "-b", "ancestor_branch"])
            cli_runner.invoke(["checkout", "-b", "theirs_branch"])

            db = geopackage(wc)
            update(db, sample_pks[0], "theirs_version")
            update(db, sample_pks[1], "ours_theirs_version")
            update(db, sample_pks[2], "theirs_version")
            update(db, sample_pks[3], "theirs_version")
            update(db, sample_pks[4], "theirs_version")
            insert(db, reset_index=1, insert_str="insert_theirs")

            cli_runner.invoke(["checkout", "ancestor_branch"])
            cli_runner.invoke(["checkout", "-b", "ours_branch"])

            update(db, sample_pks[1], "ours_theirs_version")
            update(db, sample_pks[2], "ours_version")
            update(db, sample_pks[3], "ours_version")
            update(db, sample_pks[4], "ours_version")
            update(db, sample_pks[5], "ours_version")
            insert(db, reset_index=1, insert_str="insert_ours")

            yield repo

    return ctx


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
@pytest.mark.parametrize(
    "dry_run", [pytest.param(False, id="",), pytest.param(True, id="dryrun",),],
)
def test_merge_conflicts(
    data, output_format, dry_run, create_conflicts, cli_runner,
):
    with create_conflicts(data) as repo:
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        cmd = ["merge", "theirs_branch", f"--{output_format}"]
        if dry_run:
            cmd += ["--dry-run"]

        r = cli_runner.invoke(cmd)
        assert r.exit_code == 0, r

        if output_format == "text":
            dry_run_message = (
                ["(Not actually merging due to --dry-run)", ""] if dry_run else [""]
            )

            assert (
                r.stdout.split("\n")
                == [
                    'Merging branch "theirs_branch" into ours_branch',
                    "Conflicts found:",
                    "",
                    f"{data.LAYER}:",
                    "  Feature conflicts:",
                    "    add/add: 1",
                    "    edit/edit: 3",
                    "",
                ]
                + dry_run_message
            )

        else:
            jdict = json.loads(r.stdout)
            assert jdict == {
                "sno.merge/v1": {
                    "branch": "ours_branch",
                    "ancestor": ancestor.id.hex,
                    "ours": ours.id.hex,
                    "theirs": theirs.id.hex,
                    "dryRun": dry_run,
                    "message": "Merge branch \"theirs_branch\" into ours_branch",
                    "conflicts": {
                        data.LAYER: {
                            "featureConflicts": {"add/add": 1, "edit/edit": 3}
                        },
                    },
                },
            }

        if not dry_run:
            assert read_repo_file(repo, MERGE_HEAD) == theirs.id.hex + "\n"
            assert (
                read_repo_file(repo, MERGE_MSG)
                == "Merge branch \"theirs_branch\" into ours_branch\n"
            )
            assert (
                read_repo_file(repo, MERGE_LABELS)
                == f'({ancestor.id.hex})\n"ours_branch" ({ours.id.hex})\n"theirs_branch" ({theirs.id.hex})\n'
            )
            conflict_index = ConflictIndex.read(repo_file_path(repo, MERGE_INDEX))
            assert len(conflict_index.conflicts) == 4
            cli_runner.invoke(["merge", "--abort"])

        assert not repo_file_exists(repo, MERGE_HEAD)
        assert not repo_file_exists(repo, MERGE_MSG)
        assert not repo_file_exists(repo, MERGE_LABELS)
        assert not repo_file_exists(repo, MERGE_INDEX)


def test_conflict_index_roundtrip(create_conflicts, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with create_conflicts(H.POLYGONS) as repo:
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        ancestor_id = repo.merge_base(ours.id, theirs.id)
        assert ancestor_id.hex == ancestor.id.hex

        merge_index = repo.merge_trees(ancestor.tree, ours.tree, theirs.tree)
        assert merge_index.conflicts

        # Create a ConflictIndex object, and roundtrip it into a tree and back.
        orig = ConflictIndex(merge_index)
        orig.write("test.conflict.index")
        r1 = ConflictIndex.read("test.conflict.index")
        assert r1 is not orig
        assert r1 == orig

        # Simulate resolving a conflict:
        key, conflict = next(iter(r1.conflicts.items()))
        r1.remove_conflict(key)
        r1.add(conflict.ours)  # Accept our change
        assert r1 != orig

        # Roundtrip again
        r1.write("test.conflict.index")
        r2 = ConflictIndex.read("test.conflict.index")
        assert r2 == r1


def test_list_conflicts(create_conflicts, cli_runner):
    f = ConflictOutputFormat

    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with create_conflicts(H.POLYGONS) as repo:
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        merge_index = repo.merge_trees(
            ancestor=ancestor.tree, ours=ours.tree, theirs=theirs.tree
        )
        cindex = ConflictIndex(merge_index)

        kwargs = {"ancestor": ancestor, "ours": ours, "theirs": theirs}
        short_summary = list_conflicts(repo, cindex, f.SHORT_SUMMARY, **kwargs)

        assert short_summary == {
            "nz_waca_adjustments": {"featureConflicts": {"add/add": 1, "edit/edit": 3}},
        }

        flat_short_summary = list_conflicts(
            repo, cindex, f.SHORT_SUMMARY, **kwargs, flat=True
        )
        assert flat_short_summary == 4

        summary = list_conflicts(repo, cindex, f.SUMMARY, **kwargs,)
        assert summary == {
            "nz_waca_adjustments": {
                "featureConflicts": {
                    "add/add": ["nz_waca_adjustments:id=98001"],
                    "edit/edit": [
                        "nz_waca_adjustments:id=1452332",
                        "nz_waca_adjustments:id=1456853",
                        "nz_waca_adjustments:id=1456912",
                    ],
                }
            },
        }

        flat_summary = list_conflicts(repo, cindex, f.SUMMARY, **kwargs, flat=True)
        assert flat_summary == [
            "nz_waca_adjustments:id=98001",
            "nz_waca_adjustments:id=1452332",
            "nz_waca_adjustments:id=1456853",
            "nz_waca_adjustments:id=1456912",
        ]
