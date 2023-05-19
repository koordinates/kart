EL_ONLY_FILTER = "EPSG:2193;POLYGON((1770472 5935376, 1774360 5935376, 1774360 5922016, 1770472 5922016, 1770472 5935376))"


def test_spatial_filtered_checkout(cli_runner, data_archive, requires_git_lfs):
    with data_archive("raster/elevation.tgz") as repo_path:
        r = cli_runner.invoke(["checkout", f"--spatial-filter={EL_ONLY_FILTER}"])
        assert r.exit_code == 0
        assert (
            "(of 2 tiles read, wrote 1 matching tiles to the working copy due to spatial filter)"
            in r.stdout.splitlines()
        )

        # Only files matching the spatial filter are checked out:
        files = [
            f.name
            for f in (repo_path / "elevation").glob("*.tif")
            if not f.name.startswith(".")
        ]
        assert files == ["EL.tif"]

        # Only tiles matching the spatial filter are shown in commit<>commit diffs:
        r = cli_runner.invoke(["show"])
        tile_lines = [l for l in r.stdout.splitlines() if "elevation:tile" in l]
        assert tile_lines == ["+++ elevation:tile:EL"]


def test_spatial_filtered_checkout__pam_files(
    cli_runner, data_archive, requires_git_lfs
):
    with data_archive("raster/erosion.tgz") as repo_path:
        # This spatial-filter doesn't match the tile and therefore the associated PAM
        # shouldn't be checked out either.
        r = cli_runner.invoke(["checkout", f"--spatial-filter={EL_ONLY_FILTER}"])
        assert r.exit_code == 0, r.stderr
        assert (
            "(of 1 tiles read, wrote 0 matching tiles to the working copy due to spatial filter)"
            in r.stdout.splitlines()
        )

        files = [
            f.name
            for f in (repo_path / "erorisk_si").glob("*.tif*")
            if not f.name.startswith(".")
        ]
        assert files == []

        # Only tiles matching the spatial filter are shown in commit<>commit diffs:
        r = cli_runner.invoke(["show"])
        tile_lines = [l for l in r.stdout.splitlines() if "erorisk_si:tile" in l]
        assert tile_lines == []

        r = cli_runner.invoke(["checkout", "--spatial-filter=none"])
        assert r.exit_code == 0, r.stderr

        # Both tile and PAM are now checked out.
        files = [
            f.name
            for f in (repo_path / "erorisk_si").glob("*.tif*")
            if not f.name.startswith(".")
        ]
        assert set(files) == {"erorisk_silcdb4.tif", "erorisk_silcdb4.tif.aux.xml"}

        r = cli_runner.invoke(["show"])
        tile_lines = [l for l in r.stdout.splitlines() if "erorisk_si:tile" in l]
        assert tile_lines == ["+++ erorisk_si:tile:erorisk_silcdb4"]
