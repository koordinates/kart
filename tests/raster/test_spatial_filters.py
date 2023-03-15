from .fixtures import requires_gdal_info  # noqa


EL_ONLY_FILTER = "EPSG:2193;POLYGON((1770472 5935376, 1774360 5935376, 1774360 5922016, 1770472 5922016, 1770472 5935376))"


def test_spatial_filtered_checkout(
    cli_runner, data_archive, requires_gdal_info, requires_git_lfs
):
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
