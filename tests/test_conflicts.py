import contextlib
import json
import pytest

import pygit2

from sno.conflicts import (
    ConflictIndex,
    list_conflicts,
)
from sno.structs import AncestorOursTheirs, CommitWithReference
from sno.structure import RepositoryStructure
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
        cindex = ConflictIndex.read(repo_file_path(repo, MERGE_INDEX))
        cindex.conflicts = {"0": cindex.conflicts["0"]}
        cindex.write(repo_file_path(repo, MERGE_INDEX))

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
