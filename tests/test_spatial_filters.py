import json
import pytest

from kart.repo import KartRepo
from kart.exceptions import INVALID_ARGUMENT, NO_SPATIAL_FILTER, INVALID_OPERATION

H = pytest.helpers.helpers()


def ring_as_wkt(*points):
    return "(" + ",".join(f"{x} {y}" for x, y in points) + ")"


def bbox_as_wkt_polygon(min_x, max_x, min_y, max_y):
    return (
        "POLYGON("
        + ring_as_wkt(
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        )
        + ")"
    )


SPATIAL_FILTER_GEOMETRY = {
    # A long skinny spatial filter on an angle - makes sure our filter envelopes and filter geometries are working.
    "points": (
        "MULTIPOLYGON(("
        + ring_as_wkt(
            (172.948, -35.1211),
            (173.4355, -35.7368),
            (173.8331, -36.1794),
            (174.1795, -36.545),
            (174.4488, -37.1094),
            (174.7695, -37.8277),
            (174.9876, -37.8983),
            (175.071, -37.7828),
            (174.4296, -36.654),
            (173.955, -35.9549),
            (173.5445, -35.5957),
            (173.3970, -35.2879),
            (172.9929, -35.0441),
            (172.948, -35.1211),
        )
        + "))"
    ),
    # Whereas this spatial filter is targeted on some but not all of the changes of the most recent points commit.
    "points-edit": bbox_as_wkt_polygon(175.8, 175.9, -36.9, -37.1),
    "polygons": (
        "POLYGON("
        + ring_as_wkt(
            (174.879, -37.8277),
            (175.0235, -37.9783),
            (175.2506, -37.9771),
            (175.3853, -37.8399),
            (175.3878, -37.642),
            (175.2396, -37.4999),
            (175.0235, -37.4987),
            (174.8839, -37.6359),
            (174.879, -37.8277),
        )
        + ")"
    ),
    "polygons-with-reprojection": (
        "POLYGON("
        + ring_as_wkt(
            (2675607, 6373321),
            (2687937, 6356327),
            (2707884, 6355974),
            (2720124, 6370883),
            (2720939, 6392831),
            (2708268, 6408939),
            (2689170, 6409537),
            (2676500, 6394592),
            (2675607, 6373321),
        )
        + ")"
    ),
}

SPATIAL_FILTER_CRS = {
    "points": "EPSG:4326",
    "polygons": "EPSG:4167",
    "polygons-with-reprojection": "EPSG:27200",
}


def test_init_with_spatial_filter(cli_runner, tmp_path):
    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    repo_path = tmp_path / "inline_test"
    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter={crs};{geom}"])
    assert r.exit_code == 0, r.stderr

    repo = KartRepo(repo_path)
    assert repo.config["kart.spatialfilter.geometry"].startswith(
        "POLYGON ((174.879 -37.8277,"
    )
    assert repo.config["kart.spatialfilter.crs"] == crs

    repo_path = tmp_path / "file_test"
    file_path = tmp_path / "spatialfilter.txt"
    file_path.write_text(f"{crs}\n\n{geom}\n", encoding="utf-8")
    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter=@{file_path}"])
    assert r.exit_code == 0, r.stderr

    repo = KartRepo(repo_path)
    assert repo.config["kart.spatialfilter.geometry"].startswith(
        "POLYGON ((174.879 -37.8277,"
    )
    assert repo.config["kart.spatialfilter.crs"] == crs


def test_init_with_invalid_spatial_filter(cli_runner, tmp_path):
    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    # The validity of the geometry and CRS should be checked immediately, before the repo is created:
    repo_path = tmp_path / "invalid_test"
    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter={crs};foobar"])
    assert r.exit_code == INVALID_ARGUMENT
    assert "Invalid geometry" in r.stderr
    assert not repo_path.exists()

    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter=ABCD:1234;{geom}"])
    assert r.exit_code == INVALID_ARGUMENT
    assert "Invalid or unknown coordinate reference system" in r.stderr
    assert not repo_path.exists()

    r = cli_runner.invoke(
        ["init", repo_path, f"--spatial-filter={crs};POINT(174.879 -37.8277)"]
    )
    assert r.exit_code == INVALID_ARGUMENT
    assert "Expected geometry for spatial filter of type POLYGON|MULTIPOLYGON but found: POINT"
    assert not repo_path.exists()


def test_clone_with_reference_spatial_filter(data_archive, cli_runner, tmp_path):
    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    file_path = tmp_path / "spatialfilter.txt"
    file_path.write_text(f"{crs}\n\n{geom}\n", encoding="utf-8")

    with data_archive("polygons") as repo1_path:
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Add spatial filter",
                f"spatialfilter.txt=@{file_path}",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["git", "hash-object", file_path])
        assert r.exit_code == 0, r.stderr
        blob_sha = r.stdout.strip()
        r = cli_runner.invoke(["git", "update-ref", "refs/filters/octagon", blob_sha])
        assert r.exit_code == 0, r.stderr

        # Clone repo using spatial filter reference
        repo2_path = tmp_path / "repo2"
        r = cli_runner.invoke(
            ["clone", repo1_path, repo2_path, "--spatial-filter=octagon"]
        )
        assert r.exit_code == 0, r.stderr
        repo2 = KartRepo(repo2_path)
        assert repo2.config["kart.spatialfilter.reference"] == "refs/filters/octagon"
        assert repo2.config["kart.spatialfilter.objectid"] == blob_sha

        with repo2.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == 44

        # Clone repo using spatial filter object ID
        repo3_path = tmp_path / "repo3"
        r = cli_runner.invoke(
            ["clone", repo1_path, repo3_path, f"--spatial-filter={blob_sha}"]
        )
        assert r.exit_code == 0, r.stderr
        repo3 = KartRepo(repo3_path)
        assert repo3.config["kart.spatialfilter.geometry"].startswith(
            "POLYGON ((174.879 -37.8277,"
        )
        assert repo3.config["kart.spatialfilter.crs"] == crs

        with repo3.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == 44

        # Missing spatial filter:
        repo4_path = tmp_path / "repo4"
        r = cli_runner.invoke(
            ["clone", repo1_path, repo4_path, "--spatial-filter=dodecahedron"]
        )
        assert r.exit_code == NO_SPATIAL_FILTER, r.stderr


@pytest.mark.parametrize(
    "archive,table,filter_key",
    [
        pytest.param("points", H.POINTS.LAYER, "points", id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, "polygons", id="polygons"),
        pytest.param(
            "polygons",
            H.POLYGONS.LAYER,
            "polygons-with-reprojection",
            id="polygons-with-reprojection",
        ),
        # Use polygons spatial filter config for table archive too - doesn't matter exactly what it is.
        pytest.param("table", H.TABLE.LAYER, "polygons", id="table"),
    ],
)
def test_spatial_filtered_workingcopy(
    archive, table, filter_key, data_archive, cli_runner
):
    """ Checkout a working copy to edit """
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        matching_features = {
            "points": 302,
            "polygons": 44,
            "table": H.TABLE.ROWCOUNT,  # All rows from table.tgz should be present, unaffected by spatial filtering.
        }

        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY[filter_key]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS[filter_key]

        r = cli_runner.invoke(["checkout"])
        assert r.exit_code == 0, r

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, table) == matching_features[archive]


def test_reset_wc_with_spatial_filter(data_archive, cli_runner):
    # This spatial filter matches 2 of the 5 possible changes between main^ and main.

    with data_archive("points.tgz") as repo_path:
        # Without a spatial filter - checking out main^ then restoring main results in 5 uncommitted changes,
        # the difference between main^ and main.
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        r = cli_runner.invoke(["checkout", "main^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["restore", "-s", "main"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        status = json.loads(r.stdout)["kart.status/v1"]
        assert (
            status["workingCopy"]["changes"][H.POINTS.LAYER]["feature"]["updates"] == 5
        )

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT

        # With the spatial filter - checking out main^ then restoring main results in 2 uncommitted changes,
        # the difference between main^ and main that matches the spatial filter.
        H.clear_working_copy()
        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY[
            "points-edit"
        ]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS["points"]

        r = cli_runner.invoke(["checkout", "main^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["restore", "-s", "main"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        status = json.loads(r.stdout)["kart.status/v1"]
        assert (
            status["workingCopy"]["changes"][H.POINTS.LAYER]["feature"]["updates"] == 2
        )

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == 13


def test_diff_commits_with_spatial_filter(data_archive, cli_runner, insert):
    with data_archive("points.tgz") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()
        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY[
            "points-edit"
        ]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS["points"]

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr

        # 13 of the features in the initial commit match the spatial filter.
        r = cli_runner.invoke(["show", "HEAD^", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 13

        # Of those, 2 have edits in the subsequent commit.
        r = cli_runner.invoke(["show", "HEAD", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 2

        with repo.working_copy.session() as sess:
            for i in range(5):
                insert(sess, commit=False)

        # All 5 WC edits are shown, regardless of whether they match the spatial filter.
        r = cli_runner.invoke(["diff", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 5

        # The 2 commit-commit edits that match the filter plus the 5 WC edits are shown.
        r = cli_runner.invoke(["diff", "HEAD^", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 7


def test_change_spatial_filter(data_archive, cli_runner, insert):
    with data_archive("polygons.tgz") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == H.POLYGONS.ROWCOUNT

        geom = SPATIAL_FILTER_GEOMETRY["polygons"]
        crs = SPATIAL_FILTER_CRS["polygons"]
        r = cli_runner.invoke(["checkout", "main", f"--spatial-filter={crs};{geom}"])
        assert r.exit_code == 0, r.stderr

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == 44

        geom = SPATIAL_FILTER_GEOMETRY["polygons"]
        crs = SPATIAL_FILTER_CRS["polygons"]
        r = cli_runner.invoke(["checkout", "main", "--spatial-filter="])
        assert r.exit_code == 0, r.stderr

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == H.POLYGONS.ROWCOUNT
            insert(sess, commit=False)

        r = cli_runner.invoke(["checkout", "main", f"--spatial-filter={crs};{geom}"])
        assert r.exit_code == INVALID_OPERATION
        assert "You have uncommitted changes in your working copy" in r.stderr
