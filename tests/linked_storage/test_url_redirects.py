import json

from kart.repo import KartRepo


def test_s3_url_redirects(
    data_archive,
    cli_runner,
    s3_test_data_point_cloud,
    check_lfs_hashes,
    check_tile_is_reflinked,
):
    with data_archive("linked-dataset") as repo_path:
        r = cli_runner.invoke(["lfs+", "fetch", "HEAD", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[:8] == [
            "Running fetch with --dry-run:",
            "  Found 16 LFS blobs (373KiB) to fetch from specific URLs",
            "",
            "LFS blob OID:                                                    (Pointer file OID):",
            "03e3d4dc6fc8e75c65ffdb39b630ffe26e4b95982b9765c919e34fb940e66fc0 (8d2362d8f14ea34aaebdede6602dcca0bcdd8297) → s3://example-bucket/example-path/auckland_3_2.laz",
            "06bd15fbb6616cf63a4a410c5ba4666dab76177a58cb99c3fa2afb46c9dd6379 (f129df999b5aea453ace9d4fcd1496dcebf97fe1) → s3://example-bucket/example-path/auckland_1_3.laz",
            "09701813661e369395d088a9a44f1201200155e652a8b6e291e71904f45e32a6 (553775bcbaa9c067e8ad611270d53d4f37ac37da) → s3://example-bucket/example-path/auckland_3_0.laz",
            "111579edfe022ebfd3388cc47d911c16c72c7ebd84c32a7a0c1dab6ed9ec896a (76cff04b9c7ffb01bb99ac42a6e94612fdea605f) → s3://example-bucket/example-path/auckland_0_2.laz",
        ]

        s3_test_data_point_cloud_prefix = s3_test_data_point_cloud.split("*")[0]

        linked_storage_json = {
            "urlRedirects": {
                "s3://example-bucket/example-path/": s3_test_data_point_cloud_prefix
            }
        }
        r = cli_runner.invoke(
            [
                "meta",
                "set",
                "auckland",
                f"linked-storage.json={json.dumps(linked_storage_json)}",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["lfs+", "fetch", "HEAD", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[:8] == [
            "Running fetch with --dry-run:",
            "  Found 16 LFS blobs (373KiB) to fetch from specific URLs",
            "",
            "LFS blob OID:                                                    (Pointer file OID):",
            f"03e3d4dc6fc8e75c65ffdb39b630ffe26e4b95982b9765c919e34fb940e66fc0 (8d2362d8f14ea34aaebdede6602dcca0bcdd8297) → {s3_test_data_point_cloud_prefix}auckland_3_2.laz",
            f"06bd15fbb6616cf63a4a410c5ba4666dab76177a58cb99c3fa2afb46c9dd6379 (f129df999b5aea453ace9d4fcd1496dcebf97fe1) → {s3_test_data_point_cloud_prefix}auckland_1_3.laz",
            f"09701813661e369395d088a9a44f1201200155e652a8b6e291e71904f45e32a6 (553775bcbaa9c067e8ad611270d53d4f37ac37da) → {s3_test_data_point_cloud_prefix}auckland_3_0.laz",
            f"111579edfe022ebfd3388cc47d911c16c72c7ebd84c32a7a0c1dab6ed9ec896a (76cff04b9c7ffb01bb99ac42a6e94612fdea605f) → {s3_test_data_point_cloud_prefix}auckland_0_2.laz",
        ]

        r = cli_runner.invoke(["checkout", "--dataset=auckland"])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        check_lfs_hashes(repo, expected_file_count=16)
        for x in range(4):
            for y in range(4):
                check_tile_is_reflinked(
                    repo_path / "auckland" / f"auckland_{x}_{y}.laz", repo
                )
