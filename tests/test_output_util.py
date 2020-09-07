from sno.output_util import format_wkt_for_output

NZGD_2000 = """
PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",
    GEOGCS["NZGD2000",
        DATUM["New_Zealand_Geodetic_Datum_2000",
            SPHEROID["GRS 1980",6378137,298.257222101,
                AUTHORITY["EPSG","7019"]],
            TOWGS84[0,0,0,0,0,0,0],
            AUTHORITY["EPSG","6167"]],
        PRIMEM["Greenwich",0,
            AUTHORITY["EPSG","8901"]],
        UNIT["degree",0.01745329251994328,
            AUTHORITY["EPSG","9122"]],
        AUTHORITY["EPSG","4167"]],
    UNIT["metre",1,
        AUTHORITY["EPSG","9001"]],
    PROJECTION["Transverse_Mercator"],
    PARAMETER["latitude_of_origin",0],
    PARAMETER["central_meridian",173],
    PARAMETER["scale_factor",0.9996],
    PARAMETER["false_easting",1600000],
    PARAMETER["false_northing",10000000],
    AUTHORITY["EPSG","2193"],
    AXIS["Easting",EAST],
    AXIS["Northing",NORTH]]
"""


NZGD_2000 = "".join([line.strip() for line in NZGD_2000.splitlines()])


def test_format_wkt_for_output():
    assert format_wkt_for_output(NZGD_2000, None).splitlines() == [
        'PROJCS["NZGD2000 / New Zealand Transverse Mercator 2000",',
        '    GEOGCS["NZGD2000",',
        '        DATUM["New_Zealand_Geodetic_Datum_2000",',
        '            SPHEROID["GRS 1980", 6378137, 298.257222101,',
        '                AUTHORITY["EPSG", "7019"]],',
        "            TOWGS84[0, 0, 0, 0, 0, 0, 0],",
        '            AUTHORITY["EPSG", "6167"]],',
        '        PRIMEM["Greenwich", 0,',
        '            AUTHORITY["EPSG", "8901"]],',
        '        UNIT["degree", 0.01745329251994328,',
        '            AUTHORITY["EPSG", "9122"]],',
        '        AUTHORITY["EPSG", "4167"]],',
        '    UNIT["metre", 1,',
        '        AUTHORITY["EPSG", "9001"]],',
        '    PROJECTION["Transverse_Mercator"],',
        '    PARAMETER["latitude_of_origin", 0],',
        '    PARAMETER["central_meridian", 173],',
        '    PARAMETER["scale_factor", 0.9996],',
        '    PARAMETER["false_easting", 1600000],',
        '    PARAMETER["false_northing", 10000000],',
        '    AUTHORITY["EPSG", "2193"],',
        '    AXIS["Easting", EAST],',
        '    AXIS["Northing", NORTH]]',
    ]
