import pytest

import click

# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def requires_gdal_info():
    try:
        from osgeo import gdal

        assert gdal.Info is not None
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
