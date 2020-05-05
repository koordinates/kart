import re

import pytest
from osgeo import ogr, osr

from sno.gpkg import ogr_to_gpkg_geom, gpkg_geom_to_ogr

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
def test_roundtrip_geometry_conversion(wkt):
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
