import json
import pytest

from kart.merge_util import MergeIndex
from kart.repo import KartRepo
from kart.structs import CommitWithReference

H = pytest.helpers.helpers()
CONFLICTS_OUTPUT_FORMATS = ["text", "geojson", "json", "quiet"]


def test_merge_index_roundtrip(data_archive, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with data_archive("conflicts/polygons.tgz") as repo_path:
        repo = KartRepo(repo_path)
        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        ancestor_id = repo.merge_base(ours.id, theirs.id)
        assert ancestor_id.hex == ancestor.id.hex

        index = repo.merge_trees(ancestor.tree, ours.tree, theirs.tree)
        assert index.conflicts

        # Create a MergeIndex object, and roundtrip it into a tree and back.
        orig = MergeIndex.from_pygit2_index(index)
        assert len(orig.entries) == 237
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
        assert len(r1.entries) == 237
        assert len(r1.conflicts) == 4
        assert len(r1.resolves) == 2
        assert len(r1.unresolved_conflicts) == 2

        # Roundtrip again
        r1.write("test.conflict.index")
        r2 = MergeIndex.read("test.conflict.index")
        assert r2 == r1


def test_summarise_conflicts(data_archive, cli_runner):
    # Difficult to create conflict indexes directly - easier to create them by doing a merge:
    with data_archive("conflicts/polygons.tgz") as _:
        r = cli_runner.invoke(["merge", "theirs_branch"])

        r = cli_runner.invoke(["conflicts", "-s"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "nz_waca_adjustments:",
            "    nz_waca_adjustments:feature:",
            "        nz_waca_adjustments:feature:98001",
            "        nz_waca_adjustments:feature:1452332",
            "        nz_waca_adjustments:feature:1456853",
            "        nz_waca_adjustments:feature:1456912",
            "",
        ]

        r = cli_runner.invoke(
            [
                "conflicts",
                "-s",
                "-o",
                "json",
            ]
        )
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "kart.conflicts/v1": {
                "nz_waca_adjustments": {"feature": [98001, 1452332, 1456853, 1456912]}
            }
        }

        r = cli_runner.invoke(
            [
                "conflicts",
                "-ss",
            ]
        )
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == [
            "nz_waca_adjustments:",
            "    nz_waca_adjustments:feature: 4 conflicts",
            "",
        ]

        r = cli_runner.invoke(
            [
                "conflicts",
                "-ss",
                "-o",
                "json",
            ]
        )
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == {
            "kart.conflicts/v1": {"nz_waca_adjustments": {"feature": 4}},
        }


def test_list_conflicts(data_archive, cli_runner):
    with data_archive("conflicts/points.tgz") as _:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        expected_text = [
            "nz_pa_points_topo_150k:",
            "    nz_pa_points_topo_150k:feature:",
            "        nz_pa_points_topo_150k:feature:3:",
            "            nz_pa_points_topo_150k:feature:3:ancestor:",
            "                                     fid = 3",
            "                                    geom = POINT(...)",
            "                                 t50_fid = 2426273",
            "                              name_ascii = Tauwhare Pa",
            "                              macronated = N",
            "                                    name = Tauwhare Pa",
            "            nz_pa_points_topo_150k:feature:3:ours:",
            "                                     fid = 3",
            "                                    geom = POINT(...)",
            "                                 t50_fid = 2426273",
            "                              name_ascii = Tauwhare Pa",
            "                              macronated = N",
            "                                    name = ours_version",
            "            nz_pa_points_topo_150k:feature:3:theirs:",
            "                                     fid = 3",
            "                                    geom = POINT(...)",
            "                                 t50_fid = 2426273",
            "                              name_ascii = Tauwhare Pa",
            "                              macronated = N",
            "                                    name = theirs_version",
        ]
        r = cli_runner.invoke(["conflicts", "nz_pa_points_topo_150k:feature:3"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines() == expected_text + [""]

        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == 0, r
        assert r.stdout.splitlines()[: len(expected_text)] == expected_text

        expected_json = {
            "kart.conflicts/v1": {
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
            [
                "conflicts",
                "-o",
                "json",
                "nz_pa_points_topo_150k:feature:4",
            ]
        )
        assert r.exit_code == 0, r
        assert json.loads(r.stdout) == expected_json
        r = cli_runner.invoke(["conflicts", "-o", "json"])
        assert r.exit_code == 0, r
        full_json = json.loads(r.stdout)
        features_json = full_json["kart.conflicts/v1"][H.POINTS.LAYER]["feature"]
        assert len(features_json) == 4
        assert (
            features_json["4"]
            == expected_json["kart.conflicts/v1"][H.POINTS.LAYER]["feature"]["4"]
        )

        expected_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [177.2757807736718, -38.08491506728025],
                    },
                    "properties": {
                        "fid": 5,
                        "t50_fid": 2426275,
                        "name_ascii": None,
                        "macronated": "N",
                        "name": None,
                    },
                    "id": "nz_pa_points_topo_150k:feature:5:ancestor",
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [177.2757807736718, -38.08491506728025],
                    },
                    "properties": {
                        "fid": 5,
                        "t50_fid": 2426275,
                        "name_ascii": None,
                        "macronated": "N",
                        "name": "ours_version",
                    },
                    "id": "nz_pa_points_topo_150k:feature:5:ours",
                },
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Point",
                        "coordinates": [177.2757807736718, -38.08491506728025],
                    },
                    "properties": {
                        "fid": 5,
                        "t50_fid": 2426275,
                        "name_ascii": None,
                        "macronated": "N",
                        "name": "theirs_version",
                    },
                    "id": "nz_pa_points_topo_150k:feature:5:theirs",
                },
            ],
        }
        r = cli_runner.invoke(
            [
                "conflicts",
                "-o",
                "geojson",
                "nz_pa_points_topo_150k:feature:5",
            ]
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


def test_list_conflicts_transform_crs(data_archive, cli_runner):
    with data_archive("conflicts/points") as _:
        r = cli_runner.invoke(["merge", "theirs_branch"])

        r = cli_runner.invoke(
            [
                "conflicts",
                "-o",
                "json",
                "nz_pa_points_topo_150k:feature:4",
            ]
        )
        assert r.exit_code == 0, r
        json_layer = json.loads(r.stdout)["kart.conflicts/v1"][H.POINTS.LAYER]
        assert (
            json_layer["feature"]["4"]["ancestor"]["geom"]
            == "0101000000E699C7FE092966404E7743C1B50B43C0"
        )

        r = cli_runner.invoke(
            [
                "conflicts",
                "-o",
                "json",
                "nz_pa_points_topo_150k:feature:4",
                "--crs",
                "EPSG:2193",
            ]
        )
        assert r.exit_code == 0, r
        json_layer = json.loads(r.stdout)["kart.conflicts/v1"][H.POINTS.LAYER]
        assert (
            json_layer["feature"]["4"]["ancestor"]["geom"]
            == "01010000006B88290F36253E41B8AD226F01085641"
        )

        r = cli_runner.invoke(
            ["conflicts", "-o", "geojson", "nz_pa_points_topo_150k:feature:4"]
        )
        assert r.exit_code == 0, r
        coords = json.loads(r.stdout)["features"][0]["geometry"]["coordinates"]
        assert coords == [177.28247012123683, -38.09148422044983]

        r = cli_runner.invoke(
            [
                "conflicts",
                "-o",
                "geojson",
                "nz_pa_points_topo_150k:feature:4",
                "--crs",
                "EPSG:2193",
            ]
        )
        assert r.exit_code == 0, r
        coords = json.loads(r.stdout)["features"][0]["geometry"]["coordinates"]
        assert coords == [1975606.0592274915, 5775365.736491613]


def test_find_renames(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)

        cli_runner.invoke(["checkout", "-b", "ancestor_branch"])
        cli_runner.invoke(["checkout", "-b", "theirs_branch"])
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(
                f"""UPDATE {H.POINTS.LAYER} SET fid = 9998 where fid = 1"""
            )
            assert r.rowcount == 1

        cli_runner.invoke(["commit", "-m", "theirs_commit"])

        cli_runner.invoke(["checkout", "ancestor_branch"])
        cli_runner.invoke(["checkout", "-b", "ours_branch"])

        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(
                f"""UPDATE {H.POINTS.LAYER} SET fid = 9999 where fid = 1"""
            )
            assert r.rowcount == 1

        cli_runner.invoke(["commit", "-m", "ours_commit"])

        ancestor = CommitWithReference.resolve(repo, "ancestor_branch")
        ours = CommitWithReference.resolve(repo, "ours_branch")
        theirs = CommitWithReference.resolve(repo, "theirs_branch")

        index = repo.merge_trees(ancestor.tree, ours.tree, theirs.tree)

        assert index.conflicts

        index = repo.merge_trees(
            ancestor.tree, ours.tree, theirs.tree, flags={"find_renames": False}
        )

        assert not index.conflicts


def test_meta_item_conflicts_as_geojson(data_working_copy, cli_runner):
    with data_working_copy("points") as (repo_path, wc):
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["checkout", "-b", "theirs_branch"])
        r = cli_runner.invoke(["branch", "ours_branch"])
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(f"""ALTER TABLE {H.POINTS.LAYER} ADD COLUMN new_col1""")
            assert r.rowcount == -1
        r = cli_runner.invoke(["commit", "-m", "theirs_commit"])

        r = cli_runner.invoke(["checkout", "ours_branch"])
        with repo.working_copy.tabular.session() as sess:
            r = sess.execute(f"""ALTER TABLE {H.POINTS.LAYER} ADD COLUMN new_col2""")
            assert r.rowcount == -1
        r = cli_runner.invoke(["commit", "-m", "ours_commit"])

        r = cli_runner.invoke(["merge", "theirs_branch"])
        r = cli_runner.invoke(["conflicts", "-o", "geojson"])
        assert r.exit_code == 0, r


@pytest.mark.parametrize(
    "output_format", [o for o in CONFLICTS_OUTPUT_FORMATS if o not in {"quiet"}]
)
def test_conflicts_output_to_file(output_format, data_archive, cli_runner):
    with data_archive("conflicts/polygons.tgz") as repo_path:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r

        r = cli_runner.invoke(
            ["conflicts", f"--output-format={output_format}", "--output=out"]
        )
        assert r.exit_code == 0, r
        assert (repo_path / "out").exists()


def test_conflicts_geojson_usage(data_archive, cli_runner, tmp_path):
    with data_archive("conflicts/polygons.tgz") as repo_path:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r

        # output to stdout
        r = cli_runner.invoke(
            [
                "conflicts",
                "--output-format=geojson",
            ]
        )
        assert r.exit_code == 0, r.stderr
        # output to stdout (by default)
        r = cli_runner.invoke(["conflicts", "--output-format=geojson"])
        assert r.exit_code == 0, r.stderr

        # output to a directory that doesn't yet exist
        r = cli_runner.invoke(
            [
                "conflicts",
                "--output-format=geojson",
                f"--output={tmp_path / 'abc'}",
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert {p.name for p in (tmp_path / "abc").iterdir()} == {
            "nz_waca_adjustments.geojson"
        }

        # output to a directory that does exist
        d = tmp_path / "def"
        d.mkdir()
        # this gets left alone
        (d / "empty.file").write_bytes(b"")
        # this gets deleted.
        (d / "some.geojson").write_bytes(b"{}")
        r = cli_runner.invoke(
            [
                "conflicts",
                "--output-format=geojson",
                f"--output={d}",
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert {p.name for p in d.iterdir()} == {
            "nz_waca_adjustments.geojson",
            "empty.file",
        }

        # can't import in merge state, so abort
        r = cli_runner.invoke(["merge", "--abort"])
        assert r.exit_code == 0, r.stdout

        # adding datasets
        with data_archive("gpkg-3d-points") as src:
            src_gpkg_path = src / "points-3d.gpkg"
            r = cli_runner.invoke(["-C", repo_path, "import", src_gpkg_path])
            assert r.exit_code == 0, r.stderr

        with data_archive("gpkg-points") as data:
            with data_archive("polygons"):
                src_gpkg_path = data / "nz-pa-points-topo-150k.gpkg"
                r = cli_runner.invoke(["-C", repo_path, "import", src_gpkg_path])
                assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r

        # file/stdout output isn't allowed when there are multiple datasets
        r = cli_runner.invoke(
            [
                "conflicts",
                "--output-format=geojson",
            ]
        )
        assert r.exit_code == 2, r.stderr
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Invalid value for --output: Need to specify a directory via --output for GeoJSON with more than one dataset"
        )

        # Can't specify an (existing) regular file either
        myfile = tmp_path / "ghi"
        myfile.write_bytes(b"")
        assert myfile.exists()
        r = cli_runner.invoke(
            [
                "conflicts",
                "--output-format=geojson",
                f"--output={myfile}",
            ]
        )
        assert r.exit_code == 2, r.stderr
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Invalid value for --output: Output path should be a directory for GeoJSON format."
        )
