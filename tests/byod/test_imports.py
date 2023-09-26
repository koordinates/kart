import json
import os

import pytest


@pytest.mark.slow
def test_byod_point_cloud_import(
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_point_cloud,
):
    # Using postgres here because it has the best type preservation
    repo_path = tmp_path / "point-cloud-repo"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0

    with chdir(repo_path):
        r = cli_runner.invoke(
            [
                "byod-point-cloud-import",
                s3_test_data_point_cloud,
                "--dataset-path=auckland",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["data", "ls"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["auckland"]

        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        output = json.loads(r.stdout)
        auckland = output["kart.diff/v1+hexwkb"]["auckland"]
        assert auckland["meta"]["schema.json"]["+"] == [
            {"name": "X", "dataType": "float", "size": 64},
            {"name": "Y", "dataType": "float", "size": 64},
            {"name": "Z", "dataType": "float", "size": 64},
            {"name": "Intensity", "dataType": "integer", "size": 16},
            {"name": "ReturnNumber", "dataType": "integer", "size": 8},
            {"name": "NumberOfReturns", "dataType": "integer", "size": 8},
            {"name": "ScanDirectionFlag", "dataType": "integer", "size": 8},
            {"name": "EdgeOfFlightLine", "dataType": "integer", "size": 8},
            {"name": "Classification", "dataType": "integer", "size": 8},
            {"name": "ScanAngleRank", "dataType": "float", "size": 32},
            {"name": "UserData", "dataType": "integer", "size": 8},
            {"name": "PointSourceId", "dataType": "integer", "size": 16},
            {"name": "GpsTime", "dataType": "float", "size": 64},
            {"name": "Red", "dataType": "integer", "size": 16},
            {"name": "Green", "dataType": "integer", "size": 16},
            {"name": "Blue", "dataType": "integer", "size": 16},
        ]

        tile_0_url = os.path.join(
            s3_test_data_point_cloud.split("*")[0], "auckland_0_0.laz"
        )

        assert auckland["tile"][0]["+"] == {
            "name": "auckland_0_0.laz",
            "crs84Extent": "POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
            "format": "laz-1.2",
            "nativeExtent": "1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "pointCount": 4231,
            "url": tile_0_url,
            "oid": "sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "size": 51489,
        }
