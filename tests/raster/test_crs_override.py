from osgeo import gdal

from kart.repo import KartRepo


def test_raster_import_with_crs_override(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_git_lfs,
):
    """Test that --override-crs actually rewrites the CRS in imported GeoTIFF files."""
    with data_archive_readonly("raster/tif-aerial.tgz") as aerial:
        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            # Import a tile with CRS override
            r = cli_runner.invoke(
                [
                    "raster-import",
                    f"{aerial}/aerial.tif",
                    "--dataset-path=aerial",
                    "--override-crs=EPSG:4326",
                    "--convert-to-cog",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Verify dataset CRS metadata is set to overridden value
            r = cli_runner.invoke(["meta", "get", "aerial", "crs.wkt"])
            assert r.exit_code == 0, r.stderr
            dataset_crs = r.stdout.strip()
            assert "4326" in dataset_crs

            # Check out the working copy to get the actual file
            r = cli_runner.invoke(["checkout"])
            assert r.exit_code == 0, r.stderr

            # check the CRS in the actual GeoTIFF file
            tif_files = list((repo_path / "aerial").glob("*.tif"))
            assert len(tif_files) == 1

            gdalinfo_output = gdal.Info(str(tif_files[0]))
            assert 'ID["EPSG",4326]' in gdalinfo_output
            check_lfs_hashes(repo, 1)


def test_raster_import_with_crs_override_preserve_format(
    tmp_path,
    chdir,
    cli_runner,
    data_archive_readonly,
    check_lfs_hashes,
    requires_git_lfs,
):
    """Test that --override-crs works with --preserve-format and preserves compression/tiling."""
    with data_archive_readonly("raster/tif-aerial.tgz") as aerial:
        repo_path = tmp_path / "raster-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            # Import a tile with CRS override but preserve format
            r = cli_runner.invoke(
                [
                    "raster-import",
                    f"{aerial}/aerial.tif",
                    "--dataset-path=aerial",
                    "--override-crs=EPSG:4326",
                    "--preserve-format",
                ]
            )
            assert r.exit_code == 0, r.stderr

            # Verify dataset CRS metadata is set to overridden value
            r = cli_runner.invoke(["meta", "get", "aerial", "crs.wkt"])
            assert r.exit_code == 0, r.stderr
            dataset_crs = r.stdout.strip()
            assert "4326" in dataset_crs

            # Check out the working copy to get the actual file
            r = cli_runner.invoke(["checkout"])
            assert r.exit_code == 0, r.stderr

            # check the CRS in the actual GeoTIFF file
            tif_files = list((repo_path / "aerial").glob("*.tif"))
            assert len(tif_files) == 1

            gdalinfo_output = gdal.Info(str(tif_files[0]))
            assert 'ID["EPSG",4326]' in gdalinfo_output
            # all of these are preserved
            assert "COMPRESSION=LZW" in gdalinfo_output
            assert "INTERLEAVE=PIXEL" in gdalinfo_output
            assert "PREDICTOR=2" in gdalinfo_output
            check_lfs_hashes(repo, 1)
