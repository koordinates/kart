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


def test_parse():
    assert crs_util.parse_name(TEST_WKT) == "WGS 84"
    assert crs_util.parse_authority(TEST_WKT) == ("EPSG", "4326")

    # Strangely this doesn't entirely work using osgeo:
    spatial_ref = SpatialReference(TEST_WKT)
    assert spatial_ref.GetName() == "WGS 84"
    assert spatial_ref.GetAuthorityName(None) is None
    assert spatial_ref.GetAuthorityCode(None) is None
