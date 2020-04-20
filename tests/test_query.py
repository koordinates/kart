import contextlib
import json
from pathlib import Path

import pytest


H = pytest.helpers.helpers()


@pytest.fixture
def indexed_dataset(data_archive, cli_runner):
    @contextlib.contextmanager
    def _indexed_dataset(archive, table):
        with data_archive(archive):
            r = cli_runner.invoke(["query", table, "index"])
            assert r.exit_code == 0
            yield table

    return _indexed_dataset


@pytest.mark.parametrize(
    "archive,table",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons"),
    ],
)
def test_build_spatial_index(archive, table, data_archive, cli_runner):
    with data_archive(archive) as repo_dir:
        for p in Path(repo_dir).glob(f"{table}.sno-idx*"):
            p.unlink()

        r = cli_runner.invoke(["query", table, "index"])
        assert r.exit_code == 0

        assert (Path(repo_dir) / f"{table}.sno-idxi").exists()
        assert (Path(repo_dir) / f"{table}.sno-idxd").exists()


def test_query_cli_get(indexed_dataset, cli_runner):
    with indexed_dataset("points", H.POINTS.LAYER):
        r = cli_runner.invoke(["query", H.POINTS.LAYER, "get", "1"])
        assert r.exit_code == 0, r

        assert json.loads(r.stdout) == {
            "fid": 1,
            "geom": {
                "coordinates": [177.0959629713586, -38.00433803621768],
                "type": "Point",
            },
            "macronated": "N",
            "name": None,
            "name_ascii": None,
            "t50_fid": 2426271,
        }


def test_query_cli_geo_nearest(indexed_dataset, cli_runner):
    with indexed_dataset("points", H.POINTS.LAYER):
        r = cli_runner.invoke(["query", H.POINTS.LAYER, "geo-nearest", "177,-38"])
        assert r.exit_code == 0, r

        data = json.loads(r.stdout)
        assert isinstance(data, list)
        assert len(data) == 1
        EXPECTED = {
            "fid": 147,
            "geom": {
                "coordinates": [177.00752621610505, -37.95363544873043],
                "type": "Point",
            },
            "macronated": "N",
            "name": None,
            "name_ascii": None,
            "t50_fid": 2426254,
        }
        assert data[0] == EXPECTED

        r = cli_runner.invoke(["query", H.POINTS.LAYER, "geo-nearest", "177,-38", "4"])
        assert r.exit_code == 0, r
        data = json.loads(r.stdout)
        assert isinstance(data, list)
        assert len(data) == 4
        assert data[0] == EXPECTED


def test_query_cli_geo_count(indexed_dataset, cli_runner):
    with indexed_dataset("points", H.POINTS.LAYER):
        r = cli_runner.invoke(
            ["query", H.POINTS.LAYER, "geo-count", "177,-38,177.1,-37.9"]
        )
        assert r.exit_code == 0, r

        assert r.stdout == "6"


def test_query_cli_geo_intersects(indexed_dataset, cli_runner):
    x0, y0, x1, y1 = 177, -38, 177.1, -37.9

    with indexed_dataset("points", H.POINTS.LAYER):
        r = cli_runner.invoke(
            ["query", H.POINTS.LAYER, "geo-intersects", f"{x0},{y0},{x1},{y1}"]
        )
        assert r.exit_code == 0, r

        data = json.loads(r.stdout)
        assert isinstance(data, list)
        assert len(data) == 6
        for i, o in enumerate(data):
            x, y = o["geom"]["coordinates"]
            intersects = x >= x0 and x <= x1 and y >= y0 and y <= y1
            assert (
                intersects
            ), f"No intersection found for idx {i}/{len(data)-1}: {json.dumps(o)}"
