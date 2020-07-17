import json
import pytest

from sno.merge_util import MergeIndex
from sno.structs import CommitWithReference

H = pytest.helpers.helpers()


V1_OR_V2 = ("structure_version", ["1", "2"])


@pytest.mark.parametrize(*V1_OR_V2)
def test_merge_index_roundtrip(structure_version, create_conflicts, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with create_conflicts(H.POLYGONS, structure_version) as repo:
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


@pytest.mark.parametrize(*V1_OR_V2)
def test_summarise_conflicts(structure_version, create_conflicts, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with create_conflicts(H.POLYGONS, structure_version) as repo:
        r = cli_runner.invoke(["merge", "theirs_branch"])

        r = cli_runner.invoke(["conflicts", "-s"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            'nz_waca_adjustments:',
            '    nz_waca_adjustments:feature:',
            '        nz_waca_adjustments:feature:98001',
            '        nz_waca_adjustments:feature:1452332',
            '        nz_waca_adjustments:feature:1456853',
            '        nz_waca_adjustments:feature:1456912',
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
        assert r.stdout.splitlines() == [
            'nz_waca_adjustments:',
            '    nz_waca_adjustments:feature: 4 conflicts',
            '',
        ]

        r = cli_runner.invoke(["conflicts", "-ss", "-o", "json"])
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "sno.conflicts/v1": {"nz_waca_adjustments": {"feature": 4}},
        }


@pytest.mark.parametrize(*V1_OR_V2)
def test_list_conflicts(structure_version, create_conflicts, cli_runner):
    with create_conflicts(H.POINTS, structure_version) as repo:
        r = cli_runner.invoke(["merge", "theirs_branch"])

        expected_text = [
            'nz_pa_points_topo_150k:',
            '    nz_pa_points_topo_150k:feature:',
            '        nz_pa_points_topo_150k:feature:3:',
            '            nz_pa_points_topo_150k:feature:3:ancestor:',
            '                                     fid = 3',
            '                                    geom = POINT(...)',
            '                                 t50_fid = 2426273',
            '                              name_ascii = Tauwhare Pa',
            '                              macronated = N',
            '                                    name = Tauwhare Pa',
            '            nz_pa_points_topo_150k:feature:3:ours:',
            '                                     fid = 3',
            '                                    geom = POINT(...)',
            '                                 t50_fid = 2426273',
            '                              name_ascii = Tauwhare Pa',
            '                              macronated = N',
            '                                    name = ours_version',
            '            nz_pa_points_topo_150k:feature:3:theirs:',
            '                                     fid = 3',
            '                                    geom = POINT(...)',
            '                                 t50_fid = 2426273',
            '                              name_ascii = Tauwhare Pa',
            '                              macronated = N',
            '                                    name = theirs_version',
        ]
        r = cli_runner.invoke(["conflicts", "nz_pa_points_topo_150k:feature:3"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == expected_text + ['']

        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[: len(expected_text)] == expected_text

        expected_json = {
            "sno.conflicts/v1": {
                "nz_pa_points_topo_150k": {
                    "feature": {
                        "4": {
                            "ancestor": {
                                "geom": "0101000000E699C7FE092966404E7743C1B50B43C0",
                                "fid": 4,
                                "macronated": "N",
                                "name": None,
                                "name_ascii": None,
                                "t50_fid": 2426274,
                            },
                            "ours": {
                                "geom": "0101000000E699C7FE092966404E7743C1B50B43C0",
                                "fid": 4,
                                "t50_fid": 2426274,
                                "name_ascii": None,
                                "macronated": "N",
                                "name": "ours_version",
                            },
                            "theirs": {
                                "geom": "0101000000E699C7FE092966404E7743C1B50B43C0",
                                "fid": 4,
                                "t50_fid": 2426274,
                                "name_ascii": None,
                                "macronated": "N",
                                "name": "theirs_version",
                            },
                        }
                    }
                }
            }
        }
        r = cli_runner.invoke(
            ["conflicts", "-o", "json", "nz_pa_points_topo_150k:feature:4"]
        )
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == expected_json
        r = cli_runner.invoke(["conflicts", "-o", "json"])
        assert r.exit_code == 0, r
        full_json = json.loads(r.stdout)
        features_json = full_json["sno.conflicts/v1"][H.POINTS.LAYER]["feature"]
        assert len(features_json) == 4
        assert (
            features_json["4"]
            == expected_json["sno.conflicts/v1"][H.POINTS.LAYER]["feature"]["4"]
        )

        expected_geojson = {
            'features': [
                {
                    'geometry': {
                        'coordinates': [177.2757807736718, -38.08491506728025],
                        'type': 'Point',
                    },
                    'id': 'nz_pa_points_topo_150k:feature:5:ancestor',
                    'properties': {
                        'fid': 5,
                        'macronated': 'N',
                        'name': None,
                        'name_ascii': None,
                        't50_fid': 2426275,
                    },
                    'type': 'Feature',
                },
                {
                    'geometry': {
                        'coordinates': [177.2757807736718, -38.08491506728025],
                        'type': 'Point',
                    },
                    'id': 'nz_pa_points_topo_150k:feature:5:ours',
                    'properties': {
                        'fid': 5,
                        'macronated': 'N',
                        'name': 'ours_version',
                        'name_ascii': None,
                        't50_fid': 2426275,
                    },
                    'type': 'Feature',
                },
                {
                    'geometry': {
                        'coordinates': [177.2757807736718, -38.08491506728025],
                        'type': 'Point',
                    },
                    'id': 'nz_pa_points_topo_150k:feature:5:theirs',
                    'properties': {
                        'fid': 5,
                        'macronated': 'N',
                        'name': 'theirs_version',
                        'name_ascii': None,
                        't50_fid': 2426275,
                    },
                    'type': 'Feature',
                },
            ],
            'type': 'FeatureCollection',
        }
        r = cli_runner.invoke(
            ["conflicts", "-o", "geojson", "nz_pa_points_topo_150k:feature:5"]
        )
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == expected_geojson
        r = cli_runner.invoke(["conflicts", "-o", "geojson"])
        assert r.exit_code == 0, r
        full_geojson = json.loads(r.stdout)
        features_geojson = full_geojson["features"]
        assert len(features_geojson) == 11  # 3 ancestors, 4 ours, 4 theirs.
        for version in expected_geojson["features"]:
            assert version in features_geojson
