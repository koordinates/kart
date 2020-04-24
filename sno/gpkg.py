import collections
import math
import struct
import sys
from pathlib import Path

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

    db.enableloadextension(True)
    dbcur.execute("SELECT load_extension(?)", (spatialite_path,))
    dbcur.execute("SELECT EnableGpkgMode();")
    return db


def get_meta_info(db, layer, repo_version="0.0.1"):
    yield ("version", {"version": repo_version})

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
        for filename, (sql, params, rtype) in QUERIES.items():
            # check table exists, the metadata ones may not
            if not filename.startswith("sqlite_"):
                dbcur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
                    (filename,),
                )
                if not dbcur.fetchone():
                    continue

            dbcur.execute(sql, params)
            value = [
                collections.OrderedDict(sorted(zip(row.keys(), row))) for row in dbcur
            ]
            if rtype is dict:
                value = value[0] if len(value) else None
            yield (filename, value)
    except Exception:
        print(f"Error building meta/{filename}")
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


def geom_to_ogr(gpkg_geom, parse_srs=False):
    """
    Parse GeoPackage geometry values to an OGR Geometry object
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
    geom = ogr.CreateGeometryFromWkb(wkb)

    if geom.GetGeometryType() == ogr.wkbPoint:
        nan = float('nan')
        if geom.GetX() == nan and geom.GetY() == nan:
            # spec uses POINT(nan nan) to represent POINT EMPTY
            geom = ogr.CreateGeometryFromWkt('POINT EMPTY')

    if parse_srs:
        srid = struct.unpack_from(f"{'<' if is_le else '>'}i", gpkg_geom, 4)[0]
        if srid > 0:
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(srid)
            geom.AssignSpatialReference(srs)

    return geom


# can't represent 'POINT EMPTY' in WKB.
# The GPKG spec says we should use POINT(NaN, NaN) instead.
# Here's the WKB of that.
GPKG_WKB_POINT_EMPTY = b'\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xF8\x7F\x00\x00\x00\x00\x00\x00\xF8\x7F'


def ogr_to_geom(ogr_geom):
    """
    Given an OGR geometry object,
    construct a GPKG geometry value.
    http://www.geopackage.org/spec/#gpb_format

    Arbitrarily, this only produces little-endian geometries.
    """
    wkb = ogr_geom.ExportToWkb()

    # always produce little endian
    flags = 1
    ogr_geom_type = ogr_geom.GetGeometryType()

    # magic number and version
    pieces = [b'GP\x00']

    # Flags
    empty = ogr_geom.IsEmpty()
    has_z = ogr.GT_HasZ(ogr_geom_type)
    if empty:
        # no envelope.
        flags |= 0x10
    elif has_z:
        # XYZ envelope
        flags |= 0x04
    else:
        # XY envelope
        flags |= 0x02
    pieces.append(struct.pack('<B', flags))

    # srs_id
    srid = 0
    srs = ogr_geom.GetSpatialReference()
    if srs:
        srs.AutoIdentifyEPSG()
        if srs.IsProjected():
            srid = int(srs.GetAuthorityCode("PROJCS"))
        elif srs.IsGeographic():
            srid = int(srs.GetAuthorityCode("GEOGCS"))
    pieces.append(struct.pack('<i', srid))

    # TODO: XYZM/XYM envelope.
    # not sure how to sanely get envelopes with M values from OGR :/
    # so we just write a XY/XYZ envelope for now.
    # NOTE: if you change the logic here, change flags above!
    if empty:
        # don't write an envelope.
        # note: ogr_geom.GetEnvelope() seems to return (0, 0, 0, 0) for empty geometries,
        # so if we decide to change this, be wary of that.
        pass
    elif has_z:
        pieces.append(struct.pack('<dddddd', *ogr_geom.GetEnvelope3D()))
    else:
        pieces.append(struct.pack('<dddd', *ogr_geom.GetEnvelope()))

    if empty and ogr_geom_type == ogr.wkbPoint:
        wkb = GPKG_WKB_POINT_EMPTY
    else:
        # force little-endian
        wkb = ogr_geom.ExportToWkb(ogr.wkbNDR)

    pieces.append(wkb)

    return b''.join(pieces)


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
        ogr_geom = geom_to_ogr(gpkg_geom)
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
