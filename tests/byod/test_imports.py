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


@pytest.mark.slow
def test_byod_raster_import(
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_raster,
):
    repo_path = tmp_path / "point-cloud-repo"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0

    with chdir(repo_path):
        r = cli_runner.invoke(
            [
                "byod-raster-import",
                s3_test_data_raster,
                "--dataset-path=erorisk_si",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["data", "ls"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["erorisk_si"]

        r = cli_runner.invoke(["show", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        output = json.loads(r.stdout)
        erorisk_si = output["kart.diff/v1+hexwkb"]["erorisk_si"]
        assert erorisk_si["meta"]["schema.json"]["+"] == [
            {
                "dataType": "integer",
                "size": 8,
                "description": "erorisk_si",
                "interpretation": "palette",
                "unsigned": True,
            }
        ]
        assert erorisk_si["meta"]["band/1/categories.json"]["+"] == {
            "1": "High landslide risk - delivery to stream",
            "2": "High landslide risk - non-delivery to steam",
            "3": "Moderate earthflow risk",
            "4": "Severe earthflow risk",
            "5": "Gully risk",
        }

        tile_url = os.path.join(
            s3_test_data_raster.split("*")[0], "erorisk_silcdb4.tif"
        )

        assert erorisk_si["tile"][0]["+"] == {
            "name": "erorisk_silcdb4.tif",
            "crs84Extent": "POLYGON((172.6754107 -43.7555641,172.6748326 -43.8622096,172.8170036 -43.8625257,172.8173289 -43.755879,172.6754107 -43.7555641,172.6754107 -43.7555641))",
            "dimensions": "762x790",
            "format": "geotiff/cog",
            "nativeExtent": "POLYGON((1573869.73 5155224.347,1573869.73 5143379.674,1585294.591 5143379.674,1585294.591 5155224.347,1573869.73 5155224.347))",
            "url": tile_url,
            "oid": "sha256:c4bbea4d7cfd54f4cdbca887a1b358a81710e820a6aed97cdf3337fd3e14f5aa",
            "size": 604652,
            "pamName": "erorisk_silcdb4.tif.aux.xml",
            "pamOid": "sha256:d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            "pamSize": 36908,
        }
