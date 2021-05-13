from osgeo.osr import SpatialReference

from kart import crs_util

# This is EPSG:4326 but with explicit axes in the order EAST, NORTH.
# It exists in the wild and it doesn't work well with SpatialReference.
# We just want to parse it without losing parts of it.
TEST_WKT = """
GEOGCS["WGS 84",
    DATUM["WGS_1984",
        SPHEROID["WGS 84", 6378137, 298.257223563,
            AUTHORITY["EPSG", "7030"]],
        AUTHORITY["EPSG", "6326"]],
    PRIMEM["Greenwich", 0,
        AUTHORITY["EPSG", "8901"]],
    UNIT["degree", 0.0174532925199433,
        AUTHORITY["EPSG", "9122"]],
    AXIS["Longitude", EAST],
    AXIS["Latitude", NORTH],
    AUTHORITY["EPSG", "4326"]]
"""


AXIS_LAST_WKT = """
GEOGCS["WGS 84",
    DATUM["WGS_1984",
        SPHEROID["WGS 84", 6378137, 298.257223563,
            AUTHORITY["EPSG", "7030"]],
        AUTHORITY["EPSG", "6326"]],
    PRIMEM["Greenwich", 0,
        AUTHORITY["EPSG", "8901"]],
    UNIT["degree", 0.0174532925199433,
        AUTHORITY["EPSG", "9122"]],
    AUTHORITY["EPSG", "4326"],
    AXIS["Longitude", EAST],
    AXIS["Latitude", NORTH]]
"""


def test_parse():
    assert crs_util.parse_name(TEST_WKT) == "WGS 84"
    assert crs_util.parse_authority(TEST_WKT) == ("EPSG", "4326")

    assert crs_util.parse_name(AXIS_LAST_WKT) == "WGS 84"
    assert crs_util.parse_authority(AXIS_LAST_WKT) == ("EPSG", "4326")

    # Strangely this doesn't entirely work using osgeo:
    spatial_ref = SpatialReference(TEST_WKT)
    assert spatial_ref.GetName() == "WGS 84"
    assert spatial_ref.GetAuthorityName(None) is None
    assert spatial_ref.GetAuthorityCode(None) is None


MYSQL_COMPLIANT_WKT = (
    'GEOGCS["WGS 84", DATUM["WGS_1984", SPHEROID["WGS 84", 6378137, 298.257223563, AUTHORITY["EPSG", "7030"]],'
    ' AUTHORITY["EPSG", "6326"]], PRIMEM["Greenwich", 0, AUTHORITY["EPSG", "8901"]],'
    ' UNIT["degree", 0.0174532925199433, AUTHORITY["EPSG", "9122"]],'
    ' AXIS["Longitude", EAST], AXIS["Latitude", NORTH], AUTHORITY["EPSG", "4326"]]'
)


def test_mysql_compliant_wkt():
    assert crs_util.mysql_compliant_wkt(TEST_WKT) == MYSQL_COMPLIANT_WKT
    assert crs_util.mysql_compliant_wkt(AXIS_LAST_WKT) == MYSQL_COMPLIANT_WKT
