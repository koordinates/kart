import binascii
import collections
import json
import math
import struct

import apsw
from osgeo import ogr, osr

from sno import spatialite_path


def ident(identifier):
    """ Sqlite identifier replacement """
    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def param_str(value):
    """
    Sqlite parameter string replacement.

    Generally don't use this. Needed for creating triggers/etc though.
    """
    if value is None:
        return "NULL"
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


class Row(tuple):
    def __new__(cls, cursor, row):
        return super(Row, cls).__new__(cls, row)

    def __init__(self, cursor, row):
        self._desc = tuple(d for d, _ in cursor.getdescription())

    def keys(self):
        return tuple(self._desc)

    def items(self):
        return ((k, super().__getitem__(i)) for i, k in enumerate(self._desc))

    def values(self):
        return self

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                i = self._desc.index(key)
                return super().__getitem__(i)
            except ValueError:
                raise KeyError(key)
        else:
            return super().__getitem__(key)


def db(path, **kwargs):
    db = apsw.Connection(str(path), **kwargs)
    db.setrowtrace(Row)
    dbcur = db.cursor()
    dbcur.execute("PRAGMA foreign_keys = ON;")

    current_journal = dbcur.execute("PRAGMA journal_mode").fetchone()[0]
    if current_journal.lower() == "delete":
        dbcur.execute("PRAGMA journal_mode = TRUNCATE;")  # faster

    db.config(apsw.SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 1)
    db.loadextension(spatialite_path)
    dbcur.execute("SELECT EnableGpkgMode();")
    return db


def get_meta_info(db, layer, exclude_keys=()):
    """
    Returns metadata from the gpkg_* tables about this GPKG.
    Keep this in sync with OgrImporter.build_meta_info for other datasource types.
    """
    dbcur = db.cursor()
    table = layer

    QUERIES = {
        "gpkg_contents": (
            # we ignore dynamic fields (last-change, min_x, min_y, max_x, max_y)
            f"SELECT table_name, data_type, identifier, description, srs_id FROM gpkg_contents WHERE table_name=?;",
            (table,),
            dict,
        ),
        "gpkg_geometry_columns": (
            f"SELECT table_name, column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name=?;",
            (table,),
            dict,
        ),
        "sqlite_table_info": (f"PRAGMA table_info({ident(table)});", (), list),
        "gpkg_metadata_reference": (
            """
            SELECT MR.*
            FROM gpkg_metadata_reference MR
                INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
            WHERE
                MR.table_name=?
                AND MR.column_name IS NULL
                AND MR.row_id_value IS NULL;
            """,
            (table,),
            list,
        ),
        "gpkg_metadata": (
            """
            SELECT M.*
            FROM gpkg_metadata_reference MR
                INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
            WHERE
                MR.table_name=?
                AND MR.column_name IS NULL
                AND MR.row_id_value IS NULL;
            """,
            (table,),
            list,
        ),
        "gpkg_spatial_ref_sys": (
            """
            SELECT DISTINCT SRS.*
            FROM gpkg_spatial_ref_sys SRS
                LEFT OUTER JOIN gpkg_contents C ON (C.srs_id = SRS.srs_id)
                LEFT OUTER JOIN gpkg_geometry_columns G ON (G.srs_id = SRS.srs_id)
            WHERE
                (C.table_name=? OR G.table_name=?)
            """,
            (table, table),
            list,
        ),
    }
    try:
        for key, (sql, params, rtype) in QUERIES.items():
            if key in exclude_keys:
                continue
            # check table exists, the metadata ones may not
            if not key.startswith("sqlite_"):
                dbcur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                    (key,),
                )
                if not dbcur.fetchone():
                    continue

            dbcur.execute(sql, params)
            value = [
                collections.OrderedDict(sorted(zip(row.keys(), row))) for row in dbcur
            ]
            if rtype is dict:
                value = value[0] if len(value) else None
            yield (key, value)
    except Exception:
        print(f"Error building meta/{key}")
        raise


def pk(db, table):
    """ Find the primary key for a GeoPackage table """

    # Requirement 150:
    # A feature table or view SHALL have a column that uniquely identifies the
    # row. For a feature table, the column SHOULD be a primary key. If there
    # is no primary key column, the first column SHALL be of type INTEGER and
    # SHALL contain unique values for each row.

    q = db.cursor().execute(f"PRAGMA table_info({ident(table)});")
    fields = []
    for field in q:
        if field["pk"]:
            return field["name"]
        fields.append(field)

    if fields[0]["type"] == "INTEGER":
        return fields[0]["name"]
    else:
        raise ValueError("No valid GeoPackage primary key field found")


def geom_cols(db, table):
    q = db.cursor().execute(
        """
            SELECT column_name
            FROM gpkg_geometry_columns
            WHERE table_name=?
            ORDER BY column_name;
        """,
        (table,),
    )
    return tuple(r[0] for r in q)


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


def gpkg_geom_to_wkb(gpkg_geom):
    """
    Parse GeoPackage geometry values.

    Returns little-endian ISO WKB (as bytes), or `None` if gpkg_geom is `None`.
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None
    flags = _validate_gpkg_geom(gpkg_geom)

    envelope_typ = (flags & 0b00001110) >> 1
    wkb_offset = 8
    if envelope_typ == 1:
        wkb_offset += 32
    elif envelope_typ in (2, 3):
        wkb_offset += 48
    elif envelope_typ == 4:
        wkb_offset += 64
    elif envelope_typ == 0:
        pass
    else:
        raise ValueError("Invalid envelope contents indicator")

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


def gpkg_geom_to_ogr(gpkg_geom, parse_srs=False):
    """
    Parse GeoPackage geometry values to an OGR Geometry object
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    flags = _validate_gpkg_geom(gpkg_geom)
    is_le = (flags & 0b0000001) != 0  # Endian-ness

    envelope_typ = (flags & 0b000001110) >> 1
    wkb_offset = 8
    if envelope_typ == 1:
        wkb_offset += 32
    elif envelope_typ in (2, 3):
        wkb_offset += 48
    elif envelope_typ == 4:
        wkb_offset += 64
    elif envelope_typ == 0:
        pass
    else:
        raise ValueError("Invalid envelope contents indicator")

    wkb = gpkg_geom[wkb_offset:]

    # note: the GPKG spec represents 'POINT EMPTY' as 'POINT(nan nan)' (in WKB form)
    # However, OGR loads the WKB for POINT(nan nan) as an empty geometry.
    # It has the WKB of `POINT(nan nan)` but the WKT of `POINT EMPTY`.
    # We just leave it as-is.
    geom = ogr.CreateGeometryFromWkb(wkb)

    if parse_srs:
        srid = struct.unpack_from(f"{'<' if is_le else '>'}i", gpkg_geom, 4)[0]
        if srid > 0:
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(srid)
            geom.AssignSpatialReference(srs)

    return geom


def wkb_to_gpkg_geom(wkb, **kwargs):
    ogr_geom = ogr.CreateGeometryFromWkb(wkb)
    return ogr_to_gpkg_geom(ogr_geom, **kwargs)


def hex_wkb_to_gpkg_geom(hex_wkb, **kwargs):
    if hex_wkb is None:
        return None

    wkb = binascii.unhexlify(hex_wkb)
    return wkb_to_gpkg_geom(wkb, **kwargs)


# can't represent 'POINT EMPTY' in WKB.
# The GPKG spec says we should use POINT(NaN, NaN) instead.
# Here's the WKB of that.
# We can't use WKT here: https://github.com/OSGeo/gdal/issues/2472
WKB_POINT_EMPTY_LE = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF8\x7F\x00\x00\x00\x00\x00\x00\xF8\x7F"


def ogr_to_gpkg_geom(
    ogr_geom, *, _little_endian=True, _little_endian_wkb=True, _add_envelope=False
):
    """
    Given an OGR geometry object, construct a GPKG geometry value.
    http://www.geopackage.org/spec/#gpb_format

    Normally:
        * this only produces little-endian geometries.
        * Geometries produced don't include envelopes.

    Underscore-prefixed kwargs are for use by the tests, don't use them elsewhere.
    """
    if ogr_geom is None:
        return None

    # Flags
    # always produce little endian
    flags = 0x1
    if _add_envelope:
        flags |= 0x2

    # srs_id
    srid = 0
    srs = ogr_geom.GetSpatialReference()
    if srs:
        srs.AutoIdentifyEPSG()
        if srs.IsProjected():
            srid = int(srs.GetAuthorityCode("PROJCS"))
        elif srs.IsGeographic():
            srid = int(srs.GetAuthorityCode("GEOGCS"))

    wkb = ogr_geom.ExportToIsoWkb(ogr.wkbNDR if _little_endian_wkb else ogr.wkbXDR)

    header = struct.pack(
        f'{"<" if _little_endian else ">"}ccBBi', b"G", b"P", 0, flags, srid
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
