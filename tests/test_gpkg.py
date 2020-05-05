import re

import pytest
from osgeo import ogr, osr

from sno.gpkg import (
    hex_wkb_to_gpkg_geom,
    gpkg_geom_to_hex_wkb,
    ogr_to_gpkg_geom,
    gpkg_geom_to_ogr,
)

SRID_RE = re.compile(r'^SRID=(\d+);(.*)$')


def ewkt_to_ogr(wkt):
    """
    Creates an OGR geometry, optionally with spatial reference,
    from some EWKT.
    """
    m = SRID_RE.match(wkt)
    srid = None
    if m:
        srid, wkt = m.groups()
        srid = int(srid)
    g = ogr.CreateGeometryFromWkt(wkt)
    if srid:
        srs = osr.SpatialReference()
        srs.ImportFromEPSG(srid)
        g.AssignSpatialReference(srs)
    return g


@pytest.mark.parametrize(
    'wkt',
    [
        'POINT(1 2)',
        'POINT(1 2 3)',
        'POINT(1 2 3 4)',
        'POINT EMPTY',
        'SRID=4326;POINT(1 2)',
        'SRID=4326;POINT(1 2 3)',
        'SRID=4326;POINT(1 2 3 4)',
        'SRID=4326;POINT EMPTY',
        'MULTIPOINT EMPTY',
        'MULTIPOINT (1 2)',
        'GEOMETRYCOLLECTION EMPTY',
        'GEOMETRYCOLLECTION (POINT(1 2),MULTIPOINT EMPTY)',
        'TRIANGLE((0 0 0,0 1 0,1 1 0,0 0 0))',
        'TIN (((0 0 0, 0 0 1, 0 1 0, 0 0 0)), ((0 0 0, 0 1 0, 1 1 0, 0 0 0)))',
    ],
)
def test_wkt_gpkg_wkt_roundtrip(wkt):
    orig_ogr_geom = ewkt_to_ogr(wkt)
    gpkg_geom = ogr_to_gpkg_geom(orig_ogr_geom)
    ogr_geom = gpkg_geom_to_ogr(gpkg_geom, parse_srs=True)
    assert ogr_geom.Equals(
        orig_ogr_geom
    ), f'{ogr_geom.ExportToWkt()} != {orig_ogr_geom.ExportToWkt()}'

    orig_srs = orig_ogr_geom.GetSpatialReference()
    srs = ogr_geom.GetSpatialReference()

    if srs or orig_srs:
        assert (
            srs and orig_srs
        ), f'{srs and srs.ExportToProj4()} != {orig_srs and orig_srs.ExportToProj4()}'
        assert srs.ExportToProj4() == orig_srs.ExportToProj4()


@pytest.mark.parametrize(
    'big_endian,little_endian',
    [
        pytest.param(
            '000000000100000000000000000000000000000000',
            '010100000000000000000000000000000000000000',
            id='point-empty',
        ),
        pytest.param(
            '000000000700000000', '010700000000000000', id='geometrycollection-empty'
        ),
        pytest.param(
            '00000003F9000000010000000400000000000000000000000000000000000000000000000000000000000000003FF000000000000000000000000000003FF00000000000003FF00000000000000000000000000000000000000000000000000000000000000000000000000000',
            '01F903000001000000040000000000000000000000000000000000000000000000000000000000000000000000000000000000F03F0000000000000000000000000000F03F000000000000F03F0000000000000000000000000000000000000000000000000000000000000000',
            id='triangle',
        ),
        pytest.param(
            '0000000008000000033FF0000000000000401400000000000040180000000000004000000000000000401C0000000000004008000000000000',
            '010800000003000000000000000000F03F0000000000001440000000000000184000000000000000400000000000001C400000000000000840',
            id='circularstring',
        ),
    ],
)
def test_wkb_gpkg_wkb_roundtrip(big_endian, little_endian):
    gpkg_geom = hex_wkb_to_gpkg_geom(big_endian)

    assert gpkg_geom_to_hex_wkb(gpkg_geom) == little_endian


@pytest.mark.parametrize(
    'input',
    [
        pytest.param(
            b'GP\x00\x01\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
            id='point-empty',
        ),
        pytest.param(
            b'GP\x00\x01\x00\x00\x00\x00\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x14@\x00\x00\x00\x00\x00\x00\x14@',
            id='point',
        ),
    ],
)
@pytest.mark.parametrize('little_endian', [False, True])
@pytest.mark.parametrize('little_endian_wkb', [False, True])
@pytest.mark.parametrize('with_envelope', [False, True])
def test_gpkg_wkb_gpkg_roundtrip(
    input, little_endian, little_endian_wkb, with_envelope
):
    hex_wkb = gpkg_geom_to_hex_wkb(input)
    assert hex_wkb.startswith('01'), "gpkg_geom_to_hex_wkb produced big-endian WKB"

    # Produce a GPKG geom in LE/BE variants of both the GPKG headers and the WKB itself.
    gpkg_geom_intermediate = hex_wkb_to_gpkg_geom(
        hex_wkb,
        _little_endian=little_endian,
        _little_endian_wkb=little_endian_wkb,
        _add_envelope=with_envelope,
    )
    if little_endian and little_endian_wkb and not with_envelope:
        assert gpkg_geom_intermediate == input
        return

    if with_envelope:
        # If we're adding an envelope, the geometry should have gotten bigger...
        assert len(gpkg_geom_intermediate) > len(input)

    # Now re-roundtrip to convert it back to the original
    # (little-endian, no envelope)
    hex_wkb_2 = gpkg_geom_to_hex_wkb(gpkg_geom_intermediate)
    gpkg_geom = hex_wkb_to_gpkg_geom(hex_wkb_2)

    assert gpkg_geom == input
