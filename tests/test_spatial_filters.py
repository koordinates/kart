import json
import pytest

from kart.repo import KartRepo


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
    spatial_filter_geom = bbox_as_wkt_polygon(175.8, 175.9, -36.9, -37.1)

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
        repo.config["kart.spatialfilter.geometry"] = spatial_filter_geom
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
