import binascii
import json
import math
import struct

from osgeo import ogr, osr


class Geometry(bytes):
    """
    Contains a geometry in Sno's chosen format - StandardGeoPackageBinary.
    See "Geometry encoding" section in DATASETS_v2.md for more information.
    """

    @classmethod
    def of(cls, bytes_):
        return Geometry(bytes_) if bytes_ else None

    def __str__(self):
        return "G" + super().__str__()[1:]

    def __repr__(self):
        return f"Geometry({super().__str__()})"

    def __json__(self):
        return self.to_hex_wkb()

    def to_gpkg_geom(self):
        return bytes(self)

    def to_wkb(self):
        return gpkg_geom_to_wkb(self)

    def to_hex_wkb(self):
        return gpkg_geom_to_hex_wkb(self)

    def to_ogr(self):
        return gpkg_geom_to_ogr(self)


def make_crs(crs_text):
    """
    Creates an OGR SpatialReference object from the given string.
    Accepted input is very flexible.
    see https://gdal.org/api/ogrspatialref.html#classOGRSpatialReference_1aec3c6a49533fe457ddc763d699ff8796
    """
    crs = osr.SpatialReference()
    crs.SetFromUserInput(crs_text)
    crs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return crs


def _validate_gpkg_geom(gpkg_geom):
    """
    Validates some basic things about the given GPKG geometry.
    Returns the `flags` byte.
    http://www.geopackage.org/spec/#gpb_format
    """
    if not isinstance(gpkg_geom, bytes):
        raise TypeError("Expected bytes")

    if gpkg_geom[0:2] != b"GP":  # 0x4750
        raise ValueError("Expected GeoPackage Binary Geometry")
    (version, flags) = struct.unpack_from("BB", gpkg_geom, 2)
    if version != 0:
        raise NotImplementedError("Expected GeoPackage v1 geometry, got %d", version)

    if flags & (0b00100000):  # GeoPackageBinary type
        raise NotImplementedError("ExtendedGeoPackageBinary")
    return flags


def _gpkg_envelope_size(flags):
    envelope_typ = (flags & 0b00001110) >> 1
    if envelope_typ == 1:
        # 2d envelope
        return 32
    elif envelope_typ in (2, 3):
        # 3d envelope (XYZ, XYM)
        return 48
    elif envelope_typ == 4:
        # 4d envelope (XYZM)
        return 64
    elif envelope_typ == 0:
        # no envelope
        return 0
    else:
        raise ValueError("Invalid envelope contents indicator")


def _wkb_endianness_and_geometry_type(buf, wkb_offset=0):
    """
    Given a buffer containing some WKB at the given offset,
    returns a two-tuple:
        * is_little_endian (bool)
        * WKB geometry type as an integer.
    """
    (is_le,) = struct.unpack_from('b', buf, offset=wkb_offset)
    (typ,) = struct.unpack_from(f'{"<" if is_le else ">"}I', buf, offset=wkb_offset + 1)
    return is_le, typ


def normalise_gpkg_geom(gpkg_geom):
    """
    Checks to see if the given gpkg geometry:
        * is little-endian
        * has little-endian WKB
        * has an envelope
    If so, returns the geometry unmodified.
    Otherwise, returns a little-endian geometry with an envelope attached.
    """
    if gpkg_geom is None:
        return None
    flags = _validate_gpkg_geom(gpkg_geom)

    # http://www.geopackage.org/spec/#flags_layout
    is_le = bool(flags & 0x0001) != 0
    want_envelope = None
    if is_le:
        envelope_size = _gpkg_envelope_size(flags)
        has_envelope = bool(envelope_size)

        # is this a point? if so, we don't *want* an envelope
        wkb_offset = 8 + envelope_size
        wkb_is_le, geom_type = _wkb_endianness_and_geometry_type(
            gpkg_geom, wkb_offset=wkb_offset
        )
        want_envelope = geom_type != ogr.wkbPoint

        if wkb_is_le and has_envelope == want_envelope:
            # everything is fine, no need to roundtrip via OGR
            return gpkg_geom

    # roundtrip it, the envelope and LE-ness are done by ogr_to_gpkg_geom
    return ogr_to_gpkg_geom(
        gpkg_geom_to_ogr(gpkg_geom, parse_crs=True), _add_envelope=want_envelope
    )


def gpkg_geom_to_wkb(gpkg_geom):
    """
    Parse GeoPackage geometry values.

    Returns little-endian ISO WKB (as bytes), or `None` if gpkg_geom is `None`.
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None
    flags = _validate_gpkg_geom(gpkg_geom)

    wkb_offset = 8 + _gpkg_envelope_size(flags)
    wkb = gpkg_geom[wkb_offset:]

    if wkb[0] == 0:
        # Force little-endian
        geom = ogr.CreateGeometryFromWkb(wkb)
        wkb = geom.ExportToIsoWkb(ogr.wkbNDR)
    return wkb


def gpkg_geom_to_hex_wkb(gpkg_geom):
    """
    Returns the hex-encoded little-endian WKB for the given geometry.
    """
    wkb = gpkg_geom_to_wkb(gpkg_geom)
    if wkb is None:
        return None
    else:
        return binascii.hexlify(wkb).decode("ascii").upper()


def gpkg_geom_to_ogr(gpkg_geom, parse_crs=False):
    """
    Parse GeoPackage geometry values to an OGR Geometry object
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    flags = _validate_gpkg_geom(gpkg_geom)
    is_le = (flags & 0b0000001) != 0  # Endian-ness

    wkb_offset = 8 + _gpkg_envelope_size(flags)
    wkb = gpkg_geom[wkb_offset:]

    # note: the GPKG spec represents 'POINT EMPTY' as 'POINT(nan nan)' (in WKB form)
    # However, OGR loads the WKB for POINT(nan nan) as an empty geometry.
    # It has the WKB of `POINT(nan nan)` but the WKT of `POINT EMPTY`.
    # We just leave it as-is.
    geom = ogr.CreateGeometryFromWkb(wkb)

    if parse_crs:
        crs_id = struct.unpack_from(f"{'<' if is_le else '>'}I", gpkg_geom, 4)[0]
        if crs_id > 0:
            spatial_ref = osr.SpatialReference()
            spatial_ref.ImportFromEPSG(crs_id)
            geom.AssignSpatialReference(spatial_ref)

    return geom


def wkb_to_gpkg_geom(wkb, **kwargs):
    ogr_geom = ogr.CreateGeometryFromWkb(wkb)
    return ogr_to_gpkg_geom(ogr_geom, **kwargs)


def hex_wkb_to_gpkg_geom(hex_wkb, **kwargs):
    if hex_wkb is None:
        return None

    wkb = binascii.unhexlify(hex_wkb)
    return wkb_to_gpkg_geom(wkb, **kwargs)


def wkb_to_ogr(wkb):
    return ogr.CreateGeometryFromWkb(wkb)


def hex_wkb_to_ogr(hex_wkb):
    if hex_wkb is None:
        return None

    wkb = binascii.unhexlify(hex_wkb)
    return wkb_to_ogr(wkb)


# can't represent 'POINT EMPTY' in WKB.
# The GPKG spec says we should use POINT(NaN, NaN) instead.
# Here's the WKB of that.
# We can't use WKT here: https://github.com/OSGeo/gdal/issues/2472
WKB_POINT_EMPTY_LE = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF8\x7F\x00\x00\x00\x00\x00\x00\xF8\x7F"


def ogr_to_hex_wkb(ogr_geom):
    wkb = ogr_geom.ExportToIsoWkb(ogr.wkbNDR)
    return binascii.hexlify(wkb).decode("ascii").upper()


def ogr_to_gpkg_geom(
    ogr_geom, *, _little_endian=True, _little_endian_wkb=True, _add_envelope=None
):
    """
    Given an OGR geometry object, construct a GPKG geometry value.
    http://www.geopackage.org/spec/#gpb_format

    Normally:
        * this only produces little-endian geometries.
        * All geometries include envelopes, except points.

    Underscore-prefixed kwargs are for use by the tests, don't use them elsewhere.
    """
    if ogr_geom is None:
        return None

    # Flags
    # always produce little endian
    flags = 0x1 if _little_endian else 0x0
    if _add_envelope is None:
        # don't bother adding bboxes to points.
        # it makes them significantly bigger (29 --> 61 bytes)
        # and is unnecessary - any optimisation that can use a bbox
        # can just trivially parse the point itself
        _add_envelope = ogr.GT_Flatten(ogr_geom.GetGeometryType()) != ogr.wkbPoint
    if _add_envelope:
        flags |= 0x2

    srs_id = 0
    spatial_ref = ogr_geom.GetSpatialReference()
    if spatial_ref:
        spatial_ref.AutoIdentifyEPSG()
        srs_id = int(spatial_ref.GetAuthorityCode(None) or 0)

    wkb = ogr_geom.ExportToIsoWkb(ogr.wkbNDR if _little_endian_wkb else ogr.wkbXDR)

    header = struct.pack(
        f'{"<" if _little_endian else ">"}ccBBi', b"G", b"P", 0, flags, srs_id
    )
    envelope = b""
    if _add_envelope:
        envelope = struct.pack(
            f'{"<" if _little_endian else ">"}dddd', *ogr_geom.GetEnvelope()
        )

    return header + envelope + wkb


def geojson_to_gpkg_geom(geojson, **kwargs):
    """Given a GEOJSON geometry, construct a GPKG geometry value."""
    if not isinstance(geojson, str):
        json_ogr = json.dumps(geojson)

    ogr_geom = ogr.CreateGeometryFromJson(json_ogr)
    return ogr_to_gpkg_geom(ogr_geom, **kwargs)


def geom_envelope(gpkg_geom):
    """
    Parse GeoPackage geometry to a 2D envelope.
    This is a shortcut to avoid instantiating a full OGR geometry if possible.

    Returns a 4-tuple (minx, maxx, miny, maxy), or None if the geometry is empty.

    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    if not isinstance(gpkg_geom, bytes):
        raise TypeError("Expected bytes")

    if gpkg_geom[0:2] != b"GP":  # 0x4750
        raise ValueError("Expected GeoPackage Binary Geometry")
    (version, flags) = struct.unpack_from("BB", gpkg_geom, 2)
    if version != 0:
        raise NotImplementedError("Expected GeoPackage v1 geometry, got %d", version)

    is_le = (flags & 0b0000001) != 0  # Endian-ness

    if flags & (0b00100000):  # GeoPackageBinary type
        raise NotImplementedError("ExtendedGeoPackageBinary")

    if flags & (0b00010000):  # Empty geometry
        return None

    envelope_typ = (flags & 0b000001110) >> 1
    # E: envelope contents indicator code (3-bit unsigned integer)
    # 0: no envelope (space saving slower indexing option), 0 bytes
    # 1: envelope is [minx, maxx, miny, maxy], 32 bytes
    # 2: envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
    # 3: envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
    # 4: envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
    # 5-7: invalid

    if envelope_typ == 0:
        # parse the full geometry then get it's envelope
        ogr_geom = gpkg_geom_to_ogr(gpkg_geom)
        if ogr_geom.IsEmpty():
            # envelope is apparently (0, 0, 0, 0), thanks OGR :/
            return None
        else:
            return ogr_geom.GetEnvelope()
    elif envelope_typ <= 4:
        # we only care about 2D envelopes here
        envelope = struct.unpack_from(f"{'<' if is_le else '>'}dddd", gpkg_geom, 8)
        if any(math.isnan(c) for c in envelope):
            return None
        else:
            return envelope
    else:
        raise ValueError("Invalid envelope contents indicator")
