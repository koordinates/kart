import json
from pathlib import Path

import pytest

import pygit2

from snowdrop.structure import RepositoryStructure

H = pytest.helpers.helpers()


@pytest.fixture
def indexed_dataset(data_archive):
    with data_archive("points2") as repo_dir:
        repo = pygit2.Repository(str(repo_dir))

        rs = RepositoryStructure(repo)
        ds = rs[H.POINTS2_LAYER]
        ds.build_spatial_index(ds.name)

        yield (ds, repo)


def test_build_spatial_index(indexed_dataset):
    ds, repo = indexed_dataset

    assert (Path(repo.path) / f"{ds.name}.sno-idxi").exists()
    assert (Path(repo.path) / f"{ds.name}.sno-idxd").exists()


def test_query_cli_get(indexed_dataset, cli_runner):
    ds, repo = indexed_dataset

    r = cli_runner.invoke(["query", ds.name, "get", "1"])
    assert r.exit_code == 0, r
    assert json.loads(r.stdout) == {
        'fid': 1,
        'geom': {
            'coordinates': [177.0959629713586, -38.00433803621768],
            'type': 'Point'
        },
        'macronated': 'N',
        'name': None,
        'name_ascii': None,
        't50_fid': 2426271,
    }


def test_query_cli_geo_nearest(indexed_dataset, cli_runner):
    ds, repo = indexed_dataset

    r = cli_runner.invoke(["query", ds.name, "geo-nearest", "177,-38"])
    assert r.exit_code == 0, r
    data = json.loads(r.stdout)
    assert isinstance(data, list)
    assert len(data) == 1
    EXPECTED = {
        'fid': 147,
        'geom': {
            'coordinates': [177.00752621610505, -37.95363544873043],
            'type': 'Point'
        },
        'macronated': 'N',
        'name': None,
        'name_ascii': None,
        't50_fid': 2426254,
    }
    assert data[0] == EXPECTED

    r = cli_runner.invoke(["query", ds.name, "geo-nearest", "177,-38", "4"])
    assert r.exit_code == 0, r
    data = json.loads(r.stdout)
    assert isinstance(data, list)
    assert len(data) == 4
    assert data[0] == EXPECTED


def test_query_cli_geo_count(indexed_dataset, cli_runner):
    ds, repo = indexed_dataset

    r = cli_runner.invoke(["query", ds.name, "geo-count", "177,-38,177.1,-37.9"])
    assert r.exit_code == 0, r
    assert r.stdout == "6"


def test_query_cli_geo_intersects(indexed_dataset, cli_runner):
    ds, repo = indexed_dataset

    x0,y0,x1,y1 = 177, -38, 177.1, -37.9

    r = cli_runner.invoke(["query", ds.name, "geo-intersects", f"{x0},{y0},{x1},{y1}"])
    assert r.exit_code == 0, r
    data = json.loads(r.stdout)
    assert isinstance(data, list)
    assert len(data) == 6
    for i, o in enumerate(data):
        x, y = o['geom']['coordinates']
        intersects = (x >= x0 and x <= x1 and y >= y0 and y <= y1)
        assert intersects, f"No intersection found for idx {i}/{len(data)-1}: {json.dumps(o)}"
