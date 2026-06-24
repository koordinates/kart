import os
import sys

from kart.output_util import InputMode, format_wkt_for_output, resolve_output_path

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


def _mock_pager_file(mocker):
    """Patches click.get_pager_file to yield a sentinel, and returns (mock, sentinel)."""
    sentinel = object()
    pager_cm = mocker.MagicMock()
    pager_cm.__enter__.return_value = sentinel
    mock = mocker.patch("kart.output_util.click.get_pager_file", return_value=pager_cm)
    return mock, sentinel


def test_resolve_output_path_pages_when_interactive(mocker):
    mocker.patch.dict(os.environ, {"KART_PAGER": "cat"})
    mocker.patch("kart.output_util.get_input_mode", return_value=InputMode.INTERACTIVE)
    get_pager_file, sentinel = _mock_pager_file(mocker)

    with resolve_output_path("-") as fp:
        assert fp is sentinel
    get_pager_file.assert_called_once()


def test_resolve_output_path_no_pager_when_disabled(mocker):
    mocker.patch("kart.output_util.get_input_mode", return_value=InputMode.INTERACTIVE)
    get_pager_file, _ = _mock_pager_file(mocker)

    with resolve_output_path("-", allow_pager=False) as fp:
        assert fp is sys.stdout
    get_pager_file.assert_not_called()


def test_resolve_output_path_no_pager_when_not_interactive(mocker):
    mocker.patch("kart.output_util.get_input_mode", return_value=InputMode.NO_INPUT)
    get_pager_file, _ = _mock_pager_file(mocker)

    with resolve_output_path("-") as fp:
        assert fp is sys.stdout
    get_pager_file.assert_not_called()
