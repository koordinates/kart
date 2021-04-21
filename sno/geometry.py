import binascii
import json
import math
import struct

from osgeo import ogr, osr

# http://www.geopackage.org/spec/#gpb_format
_GPKG_EMPTY_BIT = 0b10000
_GPKG_LE_BIT = 0b1
_GPKG_ENVELOPE_BITS = 0b1110

GPKG_ENVELOPE_NONE = 0
GPKG_ENVELOPE_XY = 1
GPKG_ENVELOPE_XYZ = 2
GPKG_ENVELOPE_XYM = 3
GPKG_ENVELOPE_XYZM = 4


class Geometry(bytes):
    """
    Contains a geometry in Kart's chosen format - StandardGeoPackageBinary.
    See "Geometry encoding" section in DATASETS_v2.md for more information.
    """

    @classmethod
    def of(cls, bytes_):
        if isinstance(bytes_, Geometry):
            return bytes_
        return Geometry(bytes_) if bytes_ else None

    def __init__(self, b):
        bytes.__init__(b)
        if not self.startswith(b"GP"):
            raise ValueError(
                "Invalid StandardGeoPackageBinary geometry: {}".format(self[:100])
            )

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

    def to_ewkb(self):
        return gpkg_geom_to_ewkb(self)

    def to_ogr(self):
        return gpkg_geom_to_ogr(self)

    def with_crs_id(self, crs_id):
        crs_id_bytes = struct.pack("<i", crs_id)
        return Geometry.of(self[:4] + crs_id_bytes + self[8:])

    @property
    def crs_id(self):
        """
        Returns the CRS ID as it is embedded in the GPKG header - before the WKB.
        Note that datasets V2 zeroes this field before committing,
        so will return zero when called on Geometry where it has been zeroed.
        """
        wkb_offset, is_le, crs_id = parse_gpkg_geom(self)
        return crs_id

    @classmethod
    def from_wkt(cls, wkt):
        return wkt_to_gpkg_geom(wkt)

    @classmethod
    def from_wkb(cls, wkb):
        return wkb_to_gpkg_geom(wkb)

    @classmethod
    def from_hex_wkb(cls, wkb):
        return hex_wkb_to_gpkg_geom(wkb)

    @classmethod
    def from_hex_ewkb(cls, hex_ewkb):
        return hex_ewkb_to_gpkg_geom(hex_ewkb)


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


def gpkg_envelope_size(flags):
    envelope_typ = (flags & _GPKG_ENVELOPE_BITS) >> 1
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
    (is_le,) = struct.unpack_from("b", buf, offset=wkb_offset)
    (typ,) = struct.unpack_from(f'{"<" if is_le else ">"}I', buf, offset=wkb_offset + 1)
    return is_le, typ


def _desired_gpkg_envelope_type(flags, wkb_buffer, wkb_offset=0):
    """
    Given some GPKG geometry flags and some WKB,
    returns a constant indicating the type of envelope a geometry should have.

    The rules are:
      * Points never get envelopes
      * Empty geometries never get envelopes
      * Any M components are not included in envelopes
      * XY and XYM geometries get XY envelopes
      * XYZ and XYZM geometries get XYZ envelopes
    """
    if flags & _GPKG_EMPTY_BIT:
        # no need to add envelopes to empties
        return GPKG_ENVELOPE_NONE

    wkb_is_le, geom_type = _wkb_endianness_and_geometry_type(
        wkb_buffer, wkb_offset=wkb_offset
    )
    flat_geom_type = ogr.GT_Flatten(geom_type)
    if flat_geom_type == ogr.wkbPoint:
        # is this a point? if so, we don't *want* an envelope
        # it makes them significantly bigger (29 --> 61 bytes)
        # and is unnecessary - any optimisation that can use a bbox
        # can just trivially parse the point itself
        return GPKG_ENVELOPE_NONE
    else:
        has_z = ogr.GT_HasZ(geom_type)
        if has_z:
            return GPKG_ENVELOPE_XYZ
        else:
            return GPKG_ENVELOPE_XY


def normalise_gpkg_geom(gpkg_geom):
    """
    Checks to see if the given gpkg geometry:
        * is little-endian
        * has little-endian WKB
        * has an envelope
        * has srs_id set to 0.
    If so, returns the geometry unmodified.
    Otherwise, returns a little-endian geometry with an envelope attached and srs_id=0.
    """
    if gpkg_geom is None:
        return None
    flags = _validate_gpkg_geom(gpkg_geom)
    want_envelope_type = None

    # http://www.geopackage.org/spec/#flags_layout
    is_le = bool(flags & _GPKG_LE_BIT) != 0
    if is_le:
        envelope_size = gpkg_envelope_size(flags)

        wkb_offset = 8 + envelope_size
        wkb_is_le, geom_type = _wkb_endianness_and_geometry_type(
            gpkg_geom, wkb_offset=wkb_offset
        )
        envelope_size = gpkg_envelope_size(flags)

        want_envelope_type = _desired_gpkg_envelope_type(
            flags, gpkg_geom, wkb_offset=8 + envelope_size
        )
        envelope_type = (flags & _GPKG_ENVELOPE_BITS) >> 1
        if wkb_is_le and envelope_type == want_envelope_type:
            # everything is fine, no need to roundtrip via OGR
            # just need to set srs_id to zero if it's not already
            if gpkg_geom[4:8] == b"\x00\x00\x00\x00":
                return Geometry.of(gpkg_geom)
            else:
                return Geometry.of(gpkg_geom[:4] + b"\x00\x00\x00\x00" + gpkg_geom[8:])

    # roundtrip it, the envelope and LE-ness are done by ogr_to_gpkg_geom
    return ogr_to_gpkg_geom(
        gpkg_geom_to_ogr(gpkg_geom, parse_crs=True),
        _add_envelope_type=want_envelope_type,
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

    wkb_offset = 8 + gpkg_envelope_size(flags)
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


def parse_gpkg_geom(gpkg_geom):
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

    wkb_offset = 8 + gpkg_envelope_size(flags)

    crs_id = struct.unpack_from(f"{_bo(is_le)}i", gpkg_geom, 4)[0]

    return wkb_offset, is_le, crs_id


def gpkg_geom_to_ogr(gpkg_geom, parse_crs=False):
    """
    Parse GeoPackage geometry values to an OGR Geometry object
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    wkb_offset, is_le, crs_id = parse_gpkg_geom(gpkg_geom)

    geom = ogr.CreateGeometryFromWkb(gpkg_geom[wkb_offset:])
    assert geom is not None

    if parse_crs and crs_id > 0:
        spatial_ref = osr.SpatialReference()
        spatial_ref.ImportFromEPSG(crs_id)
        geom.AssignSpatialReference(spatial_ref)

    return geom


def wkt_to_gpkg_geom(wkt, **kwargs):
    """Given a well-known-text string, returns a GPKG Geometry object."""
    if wkt is None:
        return None

    ogr_geom = ogr.CreateGeometryFromWkt(wkt)
    return ogr_to_gpkg_geom(ogr_geom, **kwargs)


def wkb_to_gpkg_geom(wkb, **kwargs):
    """Given a well-known-binary bytestring, returns a GPKG Geometry object."""
    if wkb is None:
        return None

    ogr_geom = ogr.CreateGeometryFromWkb(wkb)
    return ogr_to_gpkg_geom(ogr_geom, **kwargs)


def hex_wkb_to_gpkg_geom(hex_wkb, **kwargs):
    """Given a hex-encoded well-known-binary bytestring, returns a GPKG Geometry object."""
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
    ogr_geom,
    *,
    _little_endian=True,
    _little_endian_wkb=True,
    _add_envelope_type=None,
    _add_srs_id=False,
):
    """
    Given an OGR geometry object, construct a GPKG geometry value.
    http://www.geopackage.org/spec/#gpb_format

    Normally:
        * this only produces little-endian geometries.
        * All geometries include envelopes, except points.
        * The `srs_id` field of the geometry is always 0.

    Underscore-prefixed kwargs are for use by the tests, don't use them elsewhere.
    """
    if ogr_geom is None:
        return None

    wkb = ogr_geom.ExportToIsoWkb(ogr.wkbNDR if _little_endian_wkb else ogr.wkbXDR)

    # Flags
    # always produce little endian
    flags = _GPKG_LE_BIT if _little_endian else 0
    if ogr_geom.IsEmpty():
        flags |= _GPKG_EMPTY_BIT

    if _add_envelope_type is None:
        _add_envelope_type = _desired_gpkg_envelope_type(flags, wkb)
    flags |= _add_envelope_type << 1

    srs_id = 0
    if _add_srs_id:
        spatial_ref = ogr_geom.GetSpatialReference()
        if spatial_ref:
            spatial_ref.AutoIdentifyEPSG()
            srs_id = int(spatial_ref.GetAuthorityCode(None) or 0)

    header = struct.pack(
        f'{"<" if _little_endian else ">"}ccBBi', b"G", b"P", 0, flags, srs_id
    )
    envelope = b""
    if _add_envelope_type:
        if _add_envelope_type == GPKG_ENVELOPE_XY:
            fmt = "dddd"
            envelope = ogr_geom.GetEnvelope()
        elif _add_envelope_type == GPKG_ENVELOPE_XYZ:
            fmt = "dddddd"
            envelope = ogr_geom.GetEnvelope3D()

        envelope = struct.pack(f'{"<" if _little_endian else ">"}{fmt}', *envelope)

    return Geometry(header + envelope + wkb)


def geojson_to_gpkg_geom(geojson, **kwargs):
    """Given a GEOJSON geometry, construct a GPKG geometry value."""
    if not isinstance(geojson, str):
        json_ogr = json.dumps(geojson)

    ogr_geom = ogr.CreateGeometryFromJson(json_ogr)
    return ogr_to_gpkg_geom(ogr_geom, **kwargs)


def _bo(is_le):
    """Returns the byte order instruction for struct.pack"""
    return "<" if is_le else ">"


def gpkg_geom_to_ewkb(gpkg_geom):
    """
    Parse GeoPackage geometry values to a PostGIS EWKB value
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    wkb_offset, is_le, crs_id = parse_gpkg_geom(gpkg_geom)
    wkb_is_le = struct.unpack_from("B", gpkg_geom, wkb_offset)[0]
    bo = _bo(wkb_is_le)

    wkb_type = struct.unpack_from(f"{bo}I", gpkg_geom, wkb_offset + 1)[0]
    wkb_geom_type = (wkb_type & 0xFFFF) % 1000
    iso_zm = (wkb_type & 0xFFFF) // 1000
    has_z = iso_zm in (1, 3)
    has_m = iso_zm in (2, 3)

    ewkb_geom_type = wkb_geom_type
    ewkb_geom_type |= 0x80000000 * has_z
    ewkb_geom_type |= 0x40000000 * has_m
    ewkb_geom_type |= 0x20000000 * (crs_id > 0)

    ewkb = struct.pack(f"{bo}BI", int(wkb_is_le), ewkb_geom_type)

    if crs_id > 0:
        ewkb += struct.pack(f"{bo}I", crs_id)

    ewkb += gpkg_geom[(wkb_offset + 5) :]

    return ewkb


def hex_ewkb_to_gpkg_geom(hex_ewkb):
    """
    Parse PostGIS Hex EWKB to GeoPackage geometry
    https://github.com/postgis/postgis/blob/master/doc/ZMSgeoms.txt
    """
    if hex_ewkb is None:
        return None

    ewkb = bytes.fromhex(hex_ewkb)
    is_le = struct.unpack_from("B", ewkb)[0]
    bo = _bo(is_le)

    ewkb_type = struct.unpack_from(f"{bo}I", ewkb, 1)[0]
    has_z = bool(ewkb_type & 0x80000000)
    has_m = bool(ewkb_type & 0x40000000)
    has_srid = bool(ewkb_type & 0x20000000)

    geom_type = ewkb_type & 0xFFFF
    wkb_type = geom_type + 1000 * has_z + 2000 * has_m

    data_offset = 5
    if has_srid:
        srid = struct.unpack_from(f"{bo}I", ewkb, data_offset)[0]
        data_offset += 4
    else:
        srid = 0

    if wkb_type % 1000 == 1:
        # detect POINT[ZM] EMPTY
        px, py = struct.unpack_from(f"{bo}dd", ewkb, data_offset)
        is_empty = math.isnan(px) and math.isnan(py)
    else:
        wkb_num = struct.unpack_from(f"{bo}I", ewkb, data_offset)[
            0
        ]  # num(Points|Rings|Polygons|...)
        is_empty = wkb_num == 0

    flags = 0
    if is_le:
        flags |= 1
    if is_empty:
        flags |= 0b00010000

    gpkg_geom = (
        struct.pack(
            f"{bo}ccBBiBI",
            b"G",
            b"P",
            0,
            flags,
            srid,
            int(is_le),
            wkb_type,  # version
        )
        + ewkb[data_offset:]
    )

    # TODO: Construct normalised GPKG geometry in one go.
    return normalise_gpkg_geom(gpkg_geom)


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

    is_le = (flags & _GPKG_LE_BIT) != 0  # Endian-ness

    if flags & (0b00100000):  # GeoPackageBinary type
        raise NotImplementedError("ExtendedGeoPackageBinary")

    if flags & _GPKG_EMPTY_BIT:  # Empty geometry
        return None

    envelope_typ = (flags & _GPKG_ENVELOPE_BITS) >> 1
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
