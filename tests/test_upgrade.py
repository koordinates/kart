import json
import subprocess
from pathlib import Path

import pytest

from kart.cli import get_version
from kart.exceptions import UNSUPPORTED_VERSION
from kart.repo import KartRepo


H = pytest.helpers.helpers()

POINTS_UPGRADE_RESULT = [
    "commit 1582725544d9122251acd4b3fc75b5c88ac3fd17",
    "Author: Robert Coup <robert@coup.net.nz>",
    "Date:   Thu Jun 20 15:28:33 2019 +0100",
    "",
    "    Improve naming on Coromandel East coast",
    "",
    "commit 6e2984a28150330a6c51019a70f9e8fcfe405e8c",
    "Author: Robert Coup <robert@coup.net.nz>",
    "Date:   Tue Jun 11 12:03:58 2019 +0100",
    "",
    "    Import from nz-pa-points-topo-150k.gpkg",
]


def check_points(cli_runner):
    r = cli_runner.invoke(["meta", "get", "nz_pa_points_topo_150k", "schema.json"])
    assert r.exit_code == 0, r.stderr
    assert r.stdout.splitlines() == [
        "nz_pa_points_topo_150k",
        "    schema.json",
        "        [",
        "          {",
        '            "id": "e97b4015-2765-3a33-b174-2ece5c33343b",',
        '            "name": "fid",',
        '            "dataType": "integer",',
        '            "primaryKeyIndex": 0,',
        '            "size": 64',
        "          },",
        "          {",
        '            "id": "f488ae9b-6e15-1fe3-0bda-e0d5d38ea69e",',
        '            "name": "geom",',
        '            "dataType": "geometry",',
        '            "geometryType": "POINT",',
        '            "geometryCRS": "EPSG:4326"',
        "          },",
        "          {",
        '            "id": "4a1c7a86-c425-ea77-7f1a-d74321a10edc",',
        '            "name": "t50_fid",',
        '            "dataType": "integer",',
        '            "size": 32',
        "          },",
        "          {",
        '            "id": "d2a62351-a66d-bde2-ce3e-356fec9641e9",',
        '            "name": "name_ascii",',
        '            "dataType": "text",',
        '            "length": 75',
        "          },",
        "          {",
        '            "id": "c3389414-a511-5385-7dcd-891c4ead1663",',
        '            "name": "macronated",',
        '            "dataType": "text",',
        '            "length": 1',
        "          },",
        "          {",
        '            "id": "45b00eaa-5700-662d-8a21-9614e40c437b",',
        '            "name": "name",',
        '            "dataType": "text",',
        '            "length": 75',
        "          }",
        "        ]",
    ]


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points0.snow.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons0.snow.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table0.snow.tgz", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_v0(archive, layer, data_archive_readonly, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v0" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 0,
            "localconfig.branding": "sno",
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == UNSUPPORTED_VERSION
        assert "This Kart repo uses Datasets v0" in r.stderr
        assert f"Kart {get_version()} only supports Datasets v2" in r.stderr

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table.tgz", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_v1(archive, layer, data_archive_readonly, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v1" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 1,
            "localconfig.branding": "sno",
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == UNSUPPORTED_VERSION
        assert "This Kart repo uses Datasets v1" in r.stderr
        assert f"Kart {get_version()} only supports Datasets v2" in r.stderr

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            check_points(cli_runner)
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table.tgz", H.TABLE.LAYER, id="table"),
    ],
)
@pytest.mark.parametrize(
    "branding",
    [
        pytest.param("sno"),
        pytest.param("kart"),
    ],
)
def test_upgrade_v2(
    branding, archive, layer, data_archive_readonly, cli_runner, tmp_path, chdir
):
    archive_path = Path("upgrade") / f"v2.{branding}" / archive
    with data_archive_readonly(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 2,
            "localconfig.branding": branding,
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0  # V2 is still supported

        r = cli_runner.invoke(["upgrade", source_path, tmp_path / "dest"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

    with chdir(tmp_path / "dest"):
        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            check_points(cli_runner)
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT

        r = cli_runner.invoke(["status", "--output-format=json"])
        assert r.exit_code == 0, r


@pytest.mark.slow
@pytest.mark.parametrize(
    "archive,layer",
    [
        pytest.param("points.tgz", H.POINTS.LAYER, id="points"),
        pytest.param("polygons.tgz", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table.tgz", H.TABLE.LAYER, id="table"),
    ],
)
def test_upgrade_v2_in_place(archive, layer, data_archive, cli_runner, tmp_path, chdir):
    archive_path = Path("upgrade") / "v2.kart" / archive
    with data_archive(archive_path) as source_path:
        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 2,
            "localconfig.branding": "kart",
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0  # V2 is still supported

        r = cli_runner.invoke(["upgrade", "--in-place", source_path, source_path])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

        r = cli_runner.invoke(["data", "version", "--output-format=json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout) == {
            "repostructure.version": 3,
            "localconfig.branding": "kart",
        }

        r = cli_runner.invoke(["log"])
        assert r.exit_code == 0, r.stderr

        if layer == H.POINTS.LAYER:
            assert r.stdout.splitlines() == POINTS_UPGRADE_RESULT


def test_upgrade_preserves_refs(data_archive, cli_runner, tmp_path):
    with data_archive("upgrade/v2.kart/points") as source_path:
        # first make a new branch, and remove 'main'
        subprocess.check_call(["git", "branch", "-m", "main", "newbranch"])

        # upgrade it
        dest = tmp_path / "dest"
        r = cli_runner.invoke(["upgrade", source_path, dest])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[-1] == "Upgrade complete"

        # check that the refs are the same as before
        repo = KartRepo(dest)
        assert set(repo.references) == {"refs/heads/newbranch"}
