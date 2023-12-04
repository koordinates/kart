from kart.repo import KartRepo
from kart.exceptions import INVALID_OPERATION


def test_read_only_linked_datasets(
    data_archive,
    cli_runner,
    check_lfs_hashes,
    check_tile_is_reflinked,
):
    # Currently, we don't allow users to edit linked datasets except by doing a full linked re-import.
    # This avoids a confusing situation where linked datasets are gradually replaced with unlinked tiles sourced locally.
    with data_archive("linked-dataset-with-tiles") as repo_path:
        r = cli_runner.invoke(["lfs+", "fetch", "HEAD", "--dry-run"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Running fetch with --dry-run:",
            "  Found nothing to fetch",
        ]
        repo = KartRepo(repo_path)
        check_lfs_hashes(repo, expected_file_count=16)

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        (repo_path / "auckland" / "auckland_0_0.laz").rename(
            repo_path / "auckland" / "new.laz"
        )

        r = cli_runner.invoke(["diff"])
        assert (
            r.stderr.splitlines()[0]
            == "Warning: changes to linked datasets cannot be committed."
        )

        r = cli_runner.invoke(["commit", "-m", "yolo"])
        assert r.exit_code == INVALID_OPERATION
        assert (
            r.stderr.splitlines()[-1]
            == "Error: Aborting commit due to changes to linked datasets."
        )


def test_linked_non_checkout_datasets(
    data_archive,
    tmp_path,
    chdir,
    cli_runner,
    s3_test_data_point_cloud,
    s3_test_data_raster,
    check_lfs_hashes,
    check_tile_is_reflinked,
):
    repo_path = tmp_path / "linked-dataset-repo"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0

    with chdir(repo_path):
        r = cli_runner.invoke(
            [
                "import",
                "--link",
                "--no-checkout",
                s3_test_data_point_cloud,
                "--dataset-path=auckland",
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(
            [
                "import",
                "--link",
                "--no-checkout",
                s3_test_data_raster,
                "--dataset-path=erorisk_si",
            ]
        )
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        check_lfs_hashes(repo, expected_file_count=0)

        # Make sure we can checkout a single dataset without fetching the other.
        r = cli_runner.invoke(["checkout", "--dataset=auckland"])
        assert r.exit_code == 0, r.stderr

        check_lfs_hashes(repo, expected_file_count=16)
        assert (repo_path / "auckland").is_dir()
        assert not (repo_path / "erorisk_si").exists()

        for x in range(4):
            for y in range(4):
                assert (repo_path / "auckland" / f"auckland_{x}_{y}.laz").is_file()
                check_tile_is_reflinked(
                    repo_path / "auckland" / f"auckland_{x}_{y}.laz", repo
                )
