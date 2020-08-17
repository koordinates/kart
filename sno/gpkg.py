import collections

import apsw

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


def get_meta_info(db, layer, keys=None):
    """
    Returns metadata from the gpkg_* tables about this GPKG.
    Keep this in sync with OgrImporter.iter_gpkg_meta for other datasource types.
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
            if keys is not None and key not in keys:
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
