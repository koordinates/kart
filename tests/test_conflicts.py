import json
import pytest

from sno.merge_util import MergeIndex, MergedOursTheirs
from sno.structs import CommitWithReference
from sno.repo_files import (
    MERGE_HEAD,
    MERGE_BRANCH,
    MERGE_MSG,
    MERGE_INDEX,
    repo_file_exists,
    read_repo_file,
)

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
@pytest.mark.parametrize(
    "dry_run", [pytest.param(False, id=""), pytest.param(True, id="dryrun")],
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
            merging_state_message = (
                ["(Not actually merging due to --dry-run)", ""]
                if dry_run
                else [
                    'Repository is now in "merging" state.',
                    "View conflicts with `sno conflicts` and resolve them with `sno resolve`.",
                    "Once no conflicts remain, complete this merge with `sno merge --continue`.",
                    "Or use `sno merge --abort` to return to the previous state.",
                    "",
                ]
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
                + merging_state_message
            )

        else:
            jdict = json.loads(r.stdout)
            assert jdict == {
                "sno.merge/v1": {
                    "branch": "ours_branch",
                    "commit": ours.id.hex,
                    "merging": {
                        "ancestor": {
                            "commit": ancestor.id.hex,
                            "abbrevCommit": ancestor.short_id,
                        },
                        "ours": {
                            "branch": "ours_branch",
                            "commit": ours.id.hex,
                            "abbrevCommit": ours.short_id,
                        },
                        "theirs": {
                            "branch": "theirs_branch",
                            "commit": theirs.id.hex,
                            "abbrevCommit": theirs.short_id,
                        },
                    },
                    "dryRun": dry_run,
                    "message": "Merge branch \"theirs_branch\" into ours_branch",
                    "conflicts": {
                        data.LAYER: {
                            "featureConflicts": {"add/add": 1, "edit/edit": 3}
                        },
                    },
                    "state": "merging",
                },
            }

        if not dry_run:
            assert read_repo_file(repo, MERGE_HEAD) == theirs.id.hex + "\n"
            assert read_repo_file(repo, MERGE_BRANCH) == "theirs_branch\n"
            assert (
                read_repo_file(repo, MERGE_MSG)
                == "Merge branch \"theirs_branch\" into ours_branch\n"
            )

            merge_index = MergeIndex.read_from_repo(repo)
            assert len(merge_index.conflicts) == 4
            cli_runner.invoke(["merge", "--abort"])

        assert not repo_file_exists(repo, MERGE_HEAD)
        assert not repo_file_exists(repo, MERGE_BRANCH)
        assert not repo_file_exists(repo, MERGE_MSG)
        assert not repo_file_exists(repo, MERGE_INDEX)


def test_merge_index_roundtrip(create_conflicts, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with create_conflicts(H.POLYGONS) as repo:
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        ancestor_id = repo.merge_base(ours.id, theirs.id)
        assert ancestor_id.hex == ancestor.id.hex

        index = repo.merge_trees(ancestor.tree, ours.tree, theirs.tree)
        assert index.conflicts

        # Create a MergeIndex object, and roundtrip it into a tree and back.
        orig = MergeIndex.from_pygit2_index(index)
        assert len(orig.entries) == 242
        assert len(orig.conflicts) == 4
        assert len(orig.resolves) == 0
        assert len(orig.unresolved_conflicts) == 4

        orig.write("test.conflict.index")
        r1 = MergeIndex.read("test.conflict.index")
        assert r1 is not orig
        assert r1 == orig

        # Simulate resolving some conflicts:
        items = list(r1.conflicts.items())
        key, conflict = items[0]
        # Resolve conflict 0 by accepting our version.
        r1.add_resolve(key, MergedOursTheirs.partial(merged=conflict.ours))
        # Resolve conflict 1 by deleting it entirely.
        key, conflict = items[1]
        r1.add_resolve(key, MergedOursTheirs.EMPTY)
        assert r1 != orig
        assert len(r1.entries) == 242
        assert len(r1.conflicts) == 4
        assert len(r1.resolves) == 2
        assert len(r1.unresolved_conflicts) == 2

        # Roundtrip again
        r1.write("test.conflict.index")
        r2 = MergeIndex.read("test.conflict.index")
        assert r2 == r1


def test_summarise_conflicts(create_conflicts, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with create_conflicts(H.POLYGONS) as repo:
        r = cli_runner.invoke(["merge", "theirs_branch"])

        r = cli_runner.invoke(["conflicts", "-s"])
        assert r.exit_code == 0, r
        assert r.stdout.split("\n") == [
            'nz_waca_adjustments:',
            '  Feature conflicts:',
            '    add/add:',
            '      nz_waca_adjustments:id=98001',
            '    edit/edit:',
            '      nz_waca_adjustments:id=1452332',
            '      nz_waca_adjustments:id=1456853',
            '      nz_waca_adjustments:id=1456912',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-s", "--flat"])
        assert r.exit_code == 0, r
        assert r.stdout.split("\n") == [
            'nz_waca_adjustments:id=98001',
            'nz_waca_adjustments:id=1452332',
            'nz_waca_adjustments:id=1456853',
            'nz_waca_adjustments:id=1456912',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-s", "--json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {
                "nz_waca_adjustments": {
                    "featureConflicts": {
                        "add/add": ["nz_waca_adjustments:id=98001"],
                        "edit/edit": [
                            "nz_waca_adjustments:id=1452332",
                            "nz_waca_adjustments:id=1456853",
                            "nz_waca_adjustments:id=1456912",
                        ],
                    }
                }
            }
        }

        r = cli_runner.invoke(["conflicts", "-s", "--flat", "--json"])
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": [
                "nz_waca_adjustments:id=98001",
                "nz_waca_adjustments:id=1452332",
                "nz_waca_adjustments:id=1456853",
                "nz_waca_adjustments:id=1456912",
            ]
        }

        r = cli_runner.invoke(["conflicts", "-ss"])
        assert r.exit_code == 0, r
        assert r.stdout.split("\n") == [
            'nz_waca_adjustments:',
            '  Feature conflicts:',
            '    add/add: 1',
            '    edit/edit: 3',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-ss", "--flat"])
        assert r.exit_code == 0, r
        assert r.stdout.strip() == "4"

        r = cli_runner.invoke(["conflicts", "-ss", "--json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {
                "nz_waca_adjustments": {
                    "featureConflicts": {"add/add": 1, "edit/edit": 3}
                }
            },
        }

        r = cli_runner.invoke(["conflicts", "-ss", "--flat", "--json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {"sno.conflicts/v1": 4}


def test_list_conflicts(create_conflicts, cli_runner):
    with create_conflicts(H.POINTS) as repo:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        # Resolve all but one conflict to make the output a bit shorter.
        merge_index = MergeIndex.read_from_repo(repo)
        merge_index.conflicts = {"0": merge_index.conflicts["0"]}
        merge_index.write_to_repo(repo)

        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == 0, r
        assert r.stdout.split("\n") == [
            'nz_pa_points_topo_150k:',
            '  Feature conflicts:',
            '    edit/edit:',
            '      nz_pa_points_topo_150k:fid=4:',
            '        ancestor:',
            '                                     fid = 4',
            '                                    geom = POINT(...)',
            '                              macronated = N',
            '                                    name = ␀',
            '                              name_ascii = ␀',
            '                                 t50_fid = 2426274',
            '        ours:',
            '                                     fid = 4',
            '                                    geom = POINT(...)',
            '                              macronated = N',
            '                                    name = ours_version',
            '                              name_ascii = ␀',
            '                                 t50_fid = 2426274',
            '        theirs:',
            '                                     fid = 4',
            '                                    geom = POINT(...)',
            '                              macronated = N',
            '                                    name = theirs_version',
            '                              name_ascii = ␀',
            '                                 t50_fid = 2426274',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "--json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {
                "nz_pa_points_topo_150k": {
                    "featureConflicts": {
                        "edit/edit": {
                            "nz_pa_points_topo_150k:fid=4": {
                                "ancestor": {
                                    "geometry": "0101000000E699C7FE092966404E7743C1B50B43C0",
                                    "properties": {
                                        "fid": 4,
                                        "macronated": "N",
                                        "name": None,
                                        "name_ascii": None,
                                        "t50_fid": 2426274,
                                    },
                                    "id": 4,
                                },
                                "ours": {
                                    "geometry": "0101000000E699C7FE092966404E7743C1B50B43C0",
                                    "properties": {
                                        "fid": 4,
                                        "t50_fid": 2426274,
                                        "name_ascii": None,
                                        "macronated": "N",
                                        "name": "ours_version",
                                    },
                                    "id": 4,
                                },
                                "theirs": {
                                    "geometry": "0101000000E699C7FE092966404E7743C1B50B43C0",
                                    "properties": {
                                        "fid": 4,
                                        "t50_fid": 2426274,
                                        "name_ascii": None,
                                        "macronated": "N",
                                        "name": "theirs_version",
                                    },
                                    "id": 4,
                                },
                            }
                        }
                    }
                }
            }
        }

        r = cli_runner.invoke(["conflicts", "--geojson"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {
                "nz_pa_points_topo_150k": {
                    "featureConflicts": {
                        "edit/edit": {
                            "nz_pa_points_topo_150k:fid=4": {
                                "ancestor": {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.28247012123683,
                                            -38.09148422044983,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 4,
                                        "macronated": "N",
                                        "name": None,
                                        "name_ascii": None,
                                        "t50_fid": 2426274,
                                    },
                                    "id": 4,
                                },
                                "ours": {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.28247012123683,
                                            -38.09148422044983,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 4,
                                        "t50_fid": 2426274,
                                        "name_ascii": None,
                                        "macronated": "N",
                                        "name": "ours_version",
                                    },
                                    "id": 4,
                                },
                                "theirs": {
                                    "type": "Feature",
                                    "geometry": {
                                        "type": "Point",
                                        "coordinates": [
                                            177.28247012123683,
                                            -38.09148422044983,
                                        ],
                                    },
                                    "properties": {
                                        "fid": 4,
                                        "t50_fid": 2426274,
                                        "name_ascii": None,
                                        "macronated": "N",
                                        "name": "theirs_version",
                                    },
                                    "id": 4,
                                },
                            }
                        }
                    }
                }
            }
        }
