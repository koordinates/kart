import json
import pytest

from sno.merge_util import MergeIndex
from sno.structs import CommitWithReference

H = pytest.helpers.helpers()


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
        r1.add_resolve(key, [conflict.ours])
        # Resolve conflict 1 by deleting it entirely.
        key, conflict = items[1]
        r1.add_resolve(key, [])
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
            '    nz_waca_adjustments:feature:',
            '        nz_waca_adjustments:feature:98001',
            '        nz_waca_adjustments:feature:1452332',
            '        nz_waca_adjustments:feature:1456853',
            '        nz_waca_adjustments:feature:1456912',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-s", "-o", "json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {
                "nz_waca_adjustments": {"feature": [98001, 1452332, 1456853, 1456912]}
            }
        }

        r = cli_runner.invoke(["conflicts", "-ss"])
        assert r.exit_code == 0, r
        assert r.stdout.split("\n") == [
            'nz_waca_adjustments:',
            '    nz_waca_adjustments:feature: 4 conflicts',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-ss", "-o", "json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {"nz_waca_adjustments": {"feature": 4}},
        }


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
            '    nz_pa_points_topo_150k:feature:',
            '        nz_pa_points_topo_150k:feature:4:',
            '            nz_pa_points_topo_150k:feature:4:ancestor:',
            '                                     fid = 4',
            '                                    geom = POINT(...)',
            '                              macronated = N',
            '                                    name = ␀',
            '                              name_ascii = ␀',
            '                                 t50_fid = 2426274',
            '            nz_pa_points_topo_150k:feature:4:ours:',
            '                                     fid = 4',
            '                                    geom = POINT(...)',
            '                              macronated = N',
            '                                    name = ours_version',
            '                              name_ascii = ␀',
            '                                 t50_fid = 2426274',
            '            nz_pa_points_topo_150k:feature:4:theirs:',
            '                                     fid = 4',
            '                                    geom = POINT(...)',
            '                              macronated = N',
            '                                    name = theirs_version',
            '                              name_ascii = ␀',
            '                                 t50_fid = 2426274',
            '',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-o", "json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {
                "nz_pa_points_topo_150k": {
                    "feature": {
                        "4": {
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

        r = cli_runner.invoke(["conflicts", "-o", "geojson"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            'features': [
                {
                    'geometry': {
                        'coordinates': [177.28247012123683, -38.09148422044983],
                        'type': 'Point',
                    },
                    'id': 'nz_pa_points_topo_150k:feature:4:ancestor',
                    'properties': {
                        'fid': 4,
                        'macronated': 'N',
                        'name': None,
                        'name_ascii': None,
                        't50_fid': 2426274,
                    },
                    'type': 'Feature',
                },
                {
                    'geometry': {
                        'coordinates': [177.28247012123683, -38.09148422044983],
                        'type': 'Point',
                    },
                    'id': 'nz_pa_points_topo_150k:feature:4:ours',
                    'properties': {
                        'fid': 4,
                        'macronated': 'N',
                        'name': 'ours_version',
                        'name_ascii': None,
                        't50_fid': 2426274,
                    },
                    'type': 'Feature',
                },
                {
                    'geometry': {
                        'coordinates': [177.28247012123683, -38.09148422044983],
                        'type': 'Point',
                    },
                    'id': 'nz_pa_points_topo_150k:feature:4:theirs',
                    'properties': {
                        'fid': 4,
                        'macronated': 'N',
                        'name': 'theirs_version',
                        'name_ascii': None,
                        't50_fid': 2426274,
                    },
                    'type': 'Feature',
                },
            ],
            'type': 'FeatureCollection',
        }
