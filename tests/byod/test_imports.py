import json
import os

import pytest

from kart.repo import KartRepo


@pytest.mark.slow
def test_byod_point_cloud_import(
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_point_cloud,
    check_lfs_hashes,
    check_tile_is_reflinked,
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
                "--no-checkout",
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
            {"name": "X", "dataType": "integer", "size": 32},
            {"name": "Y", "dataType": "integer", "size": 32},
            {"name": "Z", "dataType": "integer", "size": 32},
            {"name": "Intensity", "dataType": "integer", "size": 16, "unsigned": True},
            {
                "name": "Return Number",
                "dataType": "integer",
                "size": 3,
                "unsigned": True,
            },
            {
                "name": "Number of Returns",
                "dataType": "integer",
                "size": 3,
                "unsigned": True,
            },
            {"name": "Scan Direction Flag", "dataType": "integer", "size": 1},
            {"name": "Edge of Flight Line", "dataType": "integer", "size": 1},
            {
                "name": "Classification",
                "dataType": "integer",
                "size": 5,
                "unsigned": True,
            },
            {"name": "Synthetic", "dataType": "integer", "size": 1},
            {"name": "Key-Point", "dataType": "integer", "size": 1},
            {"name": "Withheld", "dataType": "integer", "size": 1},
            {"name": "Scan Angle Rank", "dataType": "integer", "size": 8},
            {"name": "User Data", "dataType": "integer", "size": 8, "unsigned": True},
            {
                "name": "Point Source ID",
                "dataType": "integer",
                "size": 16,
                "unsigned": True,
            },
            {"name": "GPS Time", "dataType": "float", "size": 64},
            {"name": "Red", "dataType": "integer", "size": 16, "unsigned": True},
            {"name": "Green", "dataType": "integer", "size": 16, "unsigned": True},
            {"name": "Blue", "dataType": "integer", "size": 16, "unsigned": True},
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

        # Fetching LFS files in a bare-repo doesn't make much sense, but it's not (currently) disallowed.
        # (These tests will change once we support importing with --no-checkout instead of using bare repos).
        r = cli_runner.invoke(["lfs+", "fetch", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[:6] == [
            "Running fetch with --dry-run:",
            "  Found 16 blobs to fetch from specific URLs",
            "",
            "LFS blob OID:                                                    (Pointer file OID):",
            "03e3d4dc6fc8e75c65ffdb39b630ffe26e4b95982b9765c919e34fb940e66fc0 (ecb9c281c7e8cc354600d41e88d733faf2e991e1) → s3://kart-bring-your-own-data-poc/auckland-small-laz1.2/auckland_3_2.laz",
            "06bd15fbb6616cf63a4a410c5ba4666dab76177a58cb99c3fa2afb46c9dd6379 (f9ad3012492840d3c51b9b029a81c1cdbb11eef2) → s3://kart-bring-your-own-data-poc/auckland-small-laz1.2/auckland_1_3.laz",
        ]

        r = cli_runner.invoke(["checkout", "--dataset=auckland"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        check_lfs_hashes(repo, expected_file_count=16)
        for x in range(4):
            for y in range(4):
                assert (repo_path / "auckland" / f"auckland_{x}_{y}.laz").is_file()
                check_tile_is_reflinked(
                    repo_path / "auckland" / f"auckland_{x}_{y}.laz", repo
                )

        r = cli_runner.invoke(["lfs+", "fetch", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run:",
            "  Found nothing to fetch",
        ]


@pytest.mark.slow
def test_byod_raster_import(
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_raster,
    check_lfs_hashes,
    check_tile_is_reflinked,
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
                "--no-checkout",
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

        # TODO - improve tests once we support per-dataset no-checkout flags.
        r = cli_runner.invoke(["lfs+", "fetch", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run:",
            "  Found 2 blobs to fetch from specific URLs",
            "",
            "LFS blob OID:                                                    (Pointer file OID):",
            "c4bbea4d7cfd54f4cdbca887a1b358a81710e820a6aed97cdf3337fd3e14f5aa (6864fc3291a79b2ce9e4c89004172aa698b84d7c) → s3://kart-bring-your-own-data-poc/erorisk_si/erorisk_silcdb4.tif",
            "d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca (5f50b7e893da8782d5877177fab2e9a3b20fa9dc) → s3://kart-bring-your-own-data-poc/erorisk_si/erorisk_silcdb4.tif.aux.xml",
        ]

        r = cli_runner.invoke(["checkout", "--dataset=erorisk_si"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        check_lfs_hashes(repo, expected_file_count=2)
        for file in ("erorisk_silcdb4.tif", "erorisk_silcdb4.tif.aux.xml"):
            assert (repo_path / "erorisk_si" / file).is_file()
            check_tile_is_reflinked(repo_path / "erorisk_si" / file, repo)

        r = cli_runner.invoke(["lfs+", "fetch", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run:",
            "  Found nothing to fetch",
        ]
