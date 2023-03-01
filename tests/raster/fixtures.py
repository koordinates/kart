import os
import pytest

import click

from kart import is_darwin

# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def requires_gdal_info(data_archive_readonly):
    from osgeo import gdal

    # TODO - figure out what's wrong with gdal.Info in our macos build.
    # We can't rely on the try/except block below since it completely crashes python.
    if is_darwin and "CI" in os.environ:
        raise pytest.skip("gdal.Info is not working on macOS on CI")

    with data_archive_readonly("raster/tif-aerial.tgz") as aerial:
        try:
            gdal.Info(f"{aerial}/aerial.tif")
            has_gdal_info = True
        except Exception as e:
            click.echo(e, err=True)
            has_gdal_info = False

    pytest.helpers.feature_assert_or_skip(
        "gdal.Info is available",
        "KART_EXPECT_GDAL_INFO",
        has_gdal_info,
        ci_require=False,
    )
