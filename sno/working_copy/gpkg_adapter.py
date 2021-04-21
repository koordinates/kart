import collections
from datetime import datetime
import re

from sno import crs_util
from sno.meta_items import META_ITEM_NAMES as V2_META_ITEM_NAMES
from sno.schema import Schema, ColumnSchema
from sno.timestamps import datetime_to_iso8601_utc

from sqlalchemy.dialects.sqlite.base import SQLiteIdentifierPreparer, SQLiteDialect

# Given a dict of all "gpkg_meta_items", generates some or all v2 meta items.
# See generate_v2_meta_item, all_v2_meta_items.
# Given a "v2_obj" which supports get_meta_item, adapts it to support get_gpkg_meta_item.
# See generate_gpkg_meta_item, all_gpkg_meta_items.


_PREPARER = SQLiteIdentifierPreparer(SQLiteDialect())


def quote(ident):
    return _PREPARER.quote(ident)


GPKG_META_ITEM_NAMES = (
    "gpkg_contents",
    "gpkg_geometry_columns",
    "gpkg_spatial_ref_sys",
    "sqlite_table_info",
    "gpkg_metadata",
    "gpkg_metadata_reference",
)


def is_gpkg_meta_item(name):
    return name in GPKG_META_ITEM_NAMES


def generate_gpkg_meta_item(v2_obj, name, table_name):
    """Generate the requested gpkg_meta_item, given a V2 object that supports get_meta_item."""
    if not is_gpkg_meta_item(name):
        raise KeyError(f"Not a gpkg meta_item: {name}")

    if name == "gpkg_contents":
        return generate_gpkg_contents(v2_obj, table_name)
    elif name == "gpkg_geometry_columns":
        return generate_gpkg_geometry_columns(v2_obj, table_name)
    elif name == "gpkg_spatial_ref_sys":
        return generate_gpkg_spatial_ref_sys(v2_obj)
    elif name == "sqlite_table_info":
        return generate_sqlite_table_info(v2_obj)
    elif name in ("gpkg_metadata", "gpkg_metadata_reference"):
        is_reference = name == "gpkg_metadata_reference"
        return generate_gpkg_metadata(v2_obj, table_name, reference=is_reference)


def all_gpkg_meta_items(v2_obj, table_name):
    for name in GPKG_META_ITEM_NAMES:
        gpkg_meta_item = generate_gpkg_meta_item(v2_obj, name, table_name)
        if gpkg_meta_item is not None:
            yield name, gpkg_meta_item


def is_v2_meta_item(path):
    return path in V2_META_ITEM_NAMES or path.startswith("crs/")


def generate_v2_meta_item(gpkg_meta_items, path, id_salt=None):
    """
    Generate the requested meta_item, given a gpkg object that supports get_gpkg_meta_item.
    Varying the id_salt varies the ids that are generated for the schema.json item.
    """

    if not is_v2_meta_item(path):
        raise KeyError(f"Not a v2 meta_item: {path}")

    if path == "title":
        return extract_title(gpkg_meta_items["gpkg_contents"])

    elif path == "description":
        description = gpkg_meta_items["gpkg_contents"].get("description")
        return description

    elif path == "schema.json":
        return gpkg_to_v2_schema(
            gpkg_meta_items.get("sqlite_table_info"),
            gpkg_meta_items.get("gpkg_geometry_columns"),
            gpkg_meta_items.get("gpkg_spatial_ref_sys"),
            id_salt or get_table_name(gpkg_meta_items["gpkg_contents"]),
        ).to_column_dicts()
    elif path == "metadata/dataset.json":
        return gpkg_metadata_to_json(
            gpkg_meta_items.get("gpkg_metadata"),
            gpkg_meta_items.get("gpkg_metadata_reference"),
        )
    elif path == "metadata.xml":
        return gpkg_metadata_to_xml(
            gpkg_meta_items.get("gpkg_metadata"),
            gpkg_meta_items.get("gpkg_metadata_reference"),
        )

    elif path.startswith("crs/"):
        gpkg_spatial_ref_sys = gpkg_meta_items.get("gpkg_spatial_ref_sys") or ()
        for gsrs in gpkg_spatial_ref_sys:
            definition = gsrs["definition"]
            if not definition or definition == "undefined":
                continue
            if wkt_to_v2_name(definition) == path:
                return definition
        raise KeyError(f"No CRS found for {path}")


def all_v2_meta_items(gpkg_meta_items, id_salt=None):
    for path in V2_META_ITEM_NAMES:
        result = generate_v2_meta_item(gpkg_meta_items, path, id_salt=id_salt)
        if result is not None:
            yield path, result

    for identifier, definition in all_v2_crs_definitions(gpkg_meta_items):
        yield f"crs/{identifier}.wkt", definition


def all_v2_crs_definitions(gpkg_meta_items):
    gpkg_spatial_ref_sys = gpkg_meta_items.get("gpkg_spatial_ref_sys")
    for gsrs in gpkg_spatial_ref_sys:
        d = gsrs["definition"]
        if not d or d == "undefined":
            continue
        yield crs_util.get_identifier_str(d), crs_util.normalise_wkt(d)


def get_table_name(gpkg_contents):
    return gpkg_contents.get("table_name", "")


def extract_title(gpkg_contents):
    """Extract the dataset title from a v1 dataset."""
    identifier = gpkg_contents.get("identifier", "")
    table_name = gpkg_contents.get("table_name", "")
    # FIXME: find a better way of roundtripping identifiers?
    identifier_prefix = _get_identifier_prefix(table_name)
    if identifier.startswith(identifier_prefix):
        identifier = identifier[len(identifier_prefix) :]
    return identifier


def _get_identifier_prefix(table_name):
    return f"{table_name}: "


def generate_gpkg_contents(v2_obj, table_name):
    """Generate a gpkg_contents meta item from a v2 dataset."""
    result = {
        "identifier": generate_unique_identifier(v2_obj, table_name),
        "description": v2_obj.get_meta_item("description"),
        "table_name": table_name,
        "data_type": "features" if v2_obj.has_geometry else "attributes",
    }
    if v2_obj.has_geometry:
        result["srs_id"] = _gpkg_srs_id(v2_obj)
    return result


def generate_unique_identifier(v2_obj, table_name):
    # FIXME: find a better way of roundtripping identifiers?
    identifier = v2_obj.get_meta_item("title") or ""
    identifier_prefix = _get_identifier_prefix(table_name)
    if not identifier.startswith(identifier_prefix):
        identifier = identifier_prefix + identifier
    return identifier


def generate_gpkg_geometry_columns(v2_obj, table_name):
    """Generate a gpkg_geometry_columns meta item from a dataset."""
    geom_columns = v2_obj.schema.geometry_columns
    if not geom_columns:
        return None

    geometry_type = geom_columns[0].extra_type_info.get("geometryType", "GEOMETRY")
    type_name, *zm = geometry_type.split(" ", 1)
    zm = zm[0] if zm else ""
    z = 1 if "Z" in zm else 0
    m = 1 if "M" in zm else 0

    return {
        "table_name": table_name,
        "column_name": geom_columns[0].name,
        "geometry_type_name": type_name,
        "srs_id": _gpkg_srs_id(v2_obj),
        "z": z,
        "m": m,
    }


def generate_gpkg_spatial_ref_sys(v2_obj):
    """Generate a gpkg_spatial_ref_sys meta item from a dataset."""
    geom_columns = v2_obj.schema.geometry_columns
    if not geom_columns:
        return []

    crs_pathname = geom_columns[0].extra_type_info.get("geometryCRS")
    if not crs_pathname:
        return []
    definition = v2_obj.get_crs_definition(crs_pathname)
    return wkt_to_gpkg_spatial_ref_sys(definition)


def _gpkg_srs_id(v2_obj):
    gsrs = generate_gpkg_spatial_ref_sys(v2_obj)
    return gsrs[0]["srs_id"] if gsrs else 0


def wkt_to_gpkg_spatial_ref_sys(wkt):
    """Given a WKT crs definition, generate a gpkg_spatial_ref_sys meta item."""
    auth_name, auth_code = crs_util.parse_authority(wkt)
    if auth_code and auth_code.isdigit() and int(auth_code) > 0:
        srs_id = int(auth_code)
    else:
        srs_id = crs_util.get_identifier_int(wkt)
    return [
        {
            "srs_name": crs_util.parse_name(wkt),
            "definition": wkt,
            "organization": auth_name or "NONE",
            "srs_id": srs_id,
            "organization_coordsys_id": srs_id,
            "description": None,
        }
    ]


def wkt_to_v2_name(wkt):
    identifier = crs_util.get_identifier_str(wkt)
    return f"crs/{identifier}.wkt"


def generate_sqlite_table_info(v2_obj):
    """Generate a sqlite_table_info meta item from a dataset."""
    return [
        _column_schema_to_gpkg(i, col, v2_obj.has_geometry)
        for i, col in enumerate(v2_obj.schema)
    ]


def v2_schema_to_sqlite_spec(v2_obj):
    """Generate a sqlite schema string from a dataset eg 'fid INTEGER, shape GEOMETRY'."""
    result = [
        v2_column_schema_to_gpkg_spec(col, v2_obj.has_geometry) for col in v2_obj.schema
    ]
    return ",".join(result)


def v2_column_schema_to_gpkg_spec(column_schema, has_geometry):
    gpkg_type = v2_type_to_gpkg_type(column_schema, has_geometry)
    col_name = quote(column_schema.name)
    result = f"{col_name} {gpkg_type}"

    is_pk = column_schema.pk_index is not None
    if is_pk:
        if gpkg_type == "INTEGER":
            result += " PRIMARY KEY AUTOINCREMENT NOT NULL"
        elif has_geometry:
            # GPKG feature-tables only allow integer PKs, so we demote this PK to a regular UNIQUE field.
            result += f" UNIQUE NOT NULL CHECK({col_name}<>'')"
        else:
            # Non-geometry tables are allowed non-integer primary keys
            result += " PRIMARY KEY NOT NULL"

    return result


def gpkg_to_v2_schema(
    sqlite_table_info, gpkg_geometry_columns, gpkg_spatial_ref_sys, id_salt
):
    """Generate a v2 Schema from the given gpkg meta items."""
    return Schema(
        [
            _gpkg_to_column_schema(
                col, gpkg_geometry_columns, gpkg_spatial_ref_sys, id_salt
            )
            for col in sorted(sqlite_table_info, key=_sort_by_cid)
        ]
    )


def _sort_by_cid(sqlite_col_info):
    return sqlite_col_info["cid"]


def _gpkg_to_column_schema(
    sqlite_col_info, gpkg_geometry_columns, gpkg_spatial_ref_sys, id_salt
):
    """
    Given the sqlite_table_info for a particular column, and some extra context about the
    geometry column, converts it to a ColumnSchema. The extra info will only be used if the
    given sqlite_col_info is the geometry column.
    Parameters:
    sqlite_col_info - a single column from sqlite_table_info.
    gpkg_geometry_columns - meta item about the geometry column, if it exists.
    gpkg_spatial_ref_sys - meta item about the spatial reference system, if it exists.
    id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
    the name and type of the column, and on this salt.
    """
    name = sqlite_col_info["name"]
    pk_index = 0 if sqlite_col_info["pk"] == 1 else None
    if gpkg_geometry_columns and name == gpkg_geometry_columns["column_name"]:
        data_type, extra_type_info = _gkpg_geometry_columns_to_v2_type(
            gpkg_geometry_columns,
            gpkg_spatial_ref_sys,
        )
    else:
        data_type, extra_type_info = gpkg_type_to_v2_type(sqlite_col_info["type"])

    col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
    return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)


def _column_schema_to_gpkg(cid, column_schema, has_geometry):
    is_pk = 1 if column_schema.pk_index is not None else 0
    not_null = is_pk
    gpkg_type = v2_type_to_gpkg_type(column_schema, has_geometry)
    if gpkg_type != "INTEGER" and has_geometry:
        is_pk = 0  # GPKG features only allow integer PKs, so we demote this PK to a regular field.
    return {
        "cid": cid,
        "name": column_schema.name,
        "pk": is_pk,
        "type": gpkg_type,
        "notnull": not_null,
        "dflt_value": None,
    }


# From http://www.geopackage.org/spec/
# The columns of tables in a GeoPackage SHALL only be declared using one of the following data types:
# BOOLEAN, TINYINT, SMALLINT, MEDIUMINT, INT / INTEGER, FLOAT, DOUBLE / REAL,
# TEXT{(max_len)}, BLOB{(max_len)}, DATE, DATETIME, <geometry_type_name>


V2_TYPE_TO_GPKG_TYPE = {
    "boolean": "BOOLEAN",
    "integer": {
        0: "INTEGER",
        8: "TINYINT",
        16: "SMALLINT",
        32: "MEDIUMINT",
        64: "INTEGER",
    },
    "float": {0: "REAL", 32: "FLOAT", 64: "REAL"},
    "text": "TEXT",
    "blob": "BLOB",
    "date": "DATE",
    "timestamp": "DATETIME",
    "time": "TEXT",  # Approximated
    "numeric": "TEXT",  # Approximated
    "interval": "TEXT",  # Approximated
    "geometry": "GEOMETRY",
}


GPKG_TYPE_TO_V2_TYPE = {
    "BOOLEAN": "boolean",
    "TINYINT": ("integer", 8),
    "SMALLINT": ("integer", 16),
    "MEDIUMINT": ("integer", 32),
    "INT": ("integer", 64),
    "INTEGER": ("integer", 64),
    "FLOAT": ("float", 32),
    "DOUBLE": ("float", 64),
    "REAL": ("float", 64),
    "TEXT": "text",
    "BLOB": "blob",
    "DATE": "date",
    "DATETIME": "timestamp",
    "GEOMETRY": "geometry",
}


# Types that can't be roundtripped perfectly in GPKG, and what they end up as.
APPROXIMATED_TYPES = {"interval": "text", "time": "text", "numeric": "text"}

# Extra type info that might be missing/extra due to an approximated type.
APPROXIMATED_TYPES_EXTRA_TYPE_INFO = ("length", "precision", "scale")


def gpkg_type_to_v2_type(gkpg_type):
    """Convert a gpkg type to v2 schema type."""
    m = re.match(r"^(TEXT|BLOB)\(([0-9]+)\)$", gkpg_type)
    if m:
        return m.group(1).lower(), {"length": int(m.group(2))}
    v2_type_info = GPKG_TYPE_TO_V2_TYPE.get(gkpg_type)
    if v2_type_info is None:
        raise ValueError(f"Unrecognised GPKG type: {gkpg_type}")
    elif isinstance(v2_type_info, tuple):
        v2_type, size = v2_type_info
        extra_type_info = {"size": size}
    else:
        v2_type = v2_type_info
        extra_type_info = {}
    return v2_type, extra_type_info


def _gkpg_geometry_columns_to_v2_type(ggc, gsrs):
    geometry_type = ggc["geometry_type_name"]
    z = "Z" if ggc["z"] else ""
    m = "M" if ggc["m"] else ""

    crs_identifier = None
    definition = gsrs and gsrs[0]["definition"]
    if definition and definition != "undefined":
        crs_identifier = crs_util.get_identifier_str(definition)

    extra_type_info = {
        "geometryType": f"{geometry_type} {z}{m}".strip(),
        "geometryCRS": crs_identifier,
    }
    return "geometry", extra_type_info


def v2_type_to_gpkg_type(column_schema, has_geometry):
    """Convert a v2 schema type to a gpkg type."""
    if (
        has_geometry
        and column_schema.pk_index is not None
        and column_schema.data_type == "integer"
    ):
        return "INTEGER"  # Must be INTEGER, not MEDIUMINT etc.

    v2_type = column_schema.data_type
    extra_type_info = column_schema.extra_type_info
    if column_schema.data_type == "geometry":
        return extra_type_info.get("geometryType", "GEOMETRY").split(" ", 1)[0]

    gpkg_type_info = V2_TYPE_TO_GPKG_TYPE.get(v2_type)
    if gpkg_type_info is None:
        raise ValueError(f"Unrecognised data type: {v2_type}")

    if isinstance(gpkg_type_info, dict):
        return gpkg_type_info.get(extra_type_info.get("size", 0))

    gpkg_type = gpkg_type_info
    length = extra_type_info.get("length", None)
    return f"{gpkg_type}({length})" if length else gpkg_type


def generate_gpkg_metadata(v2_obj, table_name, reference=False):
    metadata_xml = v2_obj.get_meta_item("metadata.xml")
    if metadata_xml is not None:
        return xml_to_gpkg_metadata(metadata_xml, table_name, reference)
    v2json = v2_obj.get_meta_item("metadata/dataset.json")
    if v2json is not None:
        return json_to_gpkg_metadata(v2json, table_name, reference)
    return None


def json_to_gpkg_metadata(v2_metadata_json, table_name, reference=False):
    """Generates either the gpkg_metadata or gpkg_metadata_reference tables from the given metadata."""
    result = []
    timestamp = datetime_to_iso8601_utc(datetime.now())
    md_file_id = 1

    for uri, uri_metadata in sorted(v2_metadata_json.items()):
        for mime_type, content in sorted(uri_metadata.items()):
            if reference:
                row = {
                    "reference_scope": "table",
                    "table_name": table_name,
                    "column_name": None,
                    "row_id_value": None,
                    "timestamp": timestamp,
                    "md_file_id": md_file_id,
                    "md_parent_id": None,
                }
            else:
                row = {
                    "id": md_file_id,
                    "md_scope": "dataset",
                    "md_standard_uri": uri,
                    "mime_type": mime_type,
                    "metadata": content,
                }

            result.append(row)
            md_file_id += 1

    return result


_KNOWN_METADATA_URIS = {
    "GDALMultiDomainMetadata": "http://gdal.org",
}


def _get_uri_from_xml(xml_metadata):
    from xml.dom.minidom import parseString

    try:
        doc = parseString(xml_metadata)
        element = doc.documentElement
        return (
            _KNOWN_METADATA_URIS.get(element.tagName)
            or element.getAttribute("xmlns")
            or element.namespaceURI
            or "(unknown)"
        )
    except Exception:
        return "(unknown)"


def xml_to_gpkg_metadata(metadata_xml, table_name, reference=False):
    if reference:
        timestamp = datetime_to_iso8601_utc(datetime.now())
        return [
            {
                "reference_scope": "table",
                "table_name": table_name,
                "column_name": None,
                "row_id_value": None,
                "timestamp": timestamp,
                "md_file_id": 1,
                "md_parent_id": None,
            }
        ]
    else:
        return [
            {
                "id": 1,
                "md_scope": "dataset",
                "md_standard_uri": _get_uri_from_xml(metadata_xml),
                "mime_type": "text/xml",
                "metadata": metadata_xml,
            }
        ]


def _join_gpkg_metadata(gpkg_metadata, gpkg_metadata_reference):
    id_to_gm = {gm["id"]: gm for gm in gpkg_metadata}
    id_to_gmr = {gmr["md_file_id"]: gmr for gmr in gpkg_metadata_reference}
    return [{**id_to_gm[i], **id_to_gmr[i]} for i in id_to_gm.keys() & id_to_gmr.keys()]


def _row_matches(row, pattern):
    return pattern.items() <= row.items()


def gpkg_metadata_to_json(gpkg_metadata, gpkg_metadata_reference):
    # Note that the rows fetched are only those that match the pattern we care about:
    # Table-scoped XML metadata (see query in gpkg_meta_items_from_db).
    if not gpkg_metadata or not gpkg_metadata_reference:
        return None

    joined_rows = _join_gpkg_metadata(gpkg_metadata, gpkg_metadata_reference)
    if not joined_rows:
        return None

    return {
        row["md_standard_uri"]: {"text/xml": row["metadata"]} for row in joined_rows
    }


def gpkg_metadata_to_xml(gpkg_metadata, gpkg_metadata_reference):
    # Note that the rows fetched are only those that match the pattern we care about:
    # Table-scoped XML metadata (see query in gpkg_meta_items_from_db).
    if not gpkg_metadata or not gpkg_metadata_reference:
        return None

    joined_rows = _join_gpkg_metadata(gpkg_metadata, gpkg_metadata_reference)
    if not joined_rows:
        return None

    xml_list = [row["metadata"] for row in joined_rows]
    if not xml_list:
        return None
    if len(xml_list) == 1:
        return xml_list[0]

    # We can't actually commit a whole list of XML, but we need to return something that makes sense.
    # Simply throwing an error here stops dirty-detection working, and stops commands that would fix the situation
    # from working, like `kart reset HEAD --discard-changes` or `kart create-workingcopy --discard-changes`.
    return xml_list


METADATA_QUERY = """
    SELECT {select}
    FROM gpkg_metadata_reference MR
        INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
    WHERE
        M.md_scope='dataset'
        AND M.mime_type='text/xml'
        AND MR.table_name=:table_name
        AND MR.column_name IS NULL
        AND MR.row_id_value IS NULL;
    """


def gpkg_meta_items_from_db(conn, table_name, keys=None):
    """
    Returns metadata from the gpkg_* tables about this GPKG.
    Keep this in sync with OgrImportSource.gpkg_meta_items for other datasource types.
    """

    QUERIES = {
        "gpkg_contents": (
            # we ignore dynamic fields (last-change, min_x, min_y, max_x, max_y)
            """
            SELECT table_name, data_type, identifier, description, srs_id
            FROM gpkg_contents WHERE table_name=:table_name;
            """,
            dict,
        ),
        "gpkg_geometry_columns": (
            """
            SELECT table_name, column_name, geometry_type_name, srs_id, z, m
            FROM gpkg_geometry_columns WHERE table_name=:table_name;
            """,
            dict,
        ),
        "sqlite_table_info": (f"PRAGMA table_info({quote(table_name)});", list),
        "gpkg_metadata": (
            METADATA_QUERY.format(select="M.*"),
            list,
        ),
        "gpkg_metadata_reference": (
            METADATA_QUERY.format(select="MR.*"),
            list,
        ),
        "gpkg_spatial_ref_sys": (
            """
            SELECT DISTINCT SRS.*
            FROM gpkg_spatial_ref_sys SRS
                LEFT OUTER JOIN gpkg_contents C ON (C.srs_id = SRS.srs_id)
                LEFT OUTER JOIN gpkg_geometry_columns G ON (G.srs_id = SRS.srs_id)
            WHERE
                (C.table_name=:table_name OR G.table_name=:table_name)
            """,
            list,
        ),
    }
    try:
        for key, (sql, rtype) in QUERIES.items():
            if keys is not None and key not in keys:
                continue
            # check table exists, the metadata ones may not
            if not key.startswith("sqlite_"):
                r = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=:name;",
                    {"name": key},
                )
                if not r.fetchone():
                    continue

            r = conn.execute(sql, {"table_name": table_name})
            value = [collections.OrderedDict(sorted(zip(row.keys(), row))) for row in r]
            if rtype is dict:
                value = value[0] if len(value) else None
            yield (key, value)
    except Exception:
        print(f"Error building meta/{key}")
        raise


def pk(conn, table):
    """ Find the primary key for a GeoPackage table """

    # Requirement 150:
    # A feature table or view SHALL have a column that uniquely identifies the
    # row. For a feature table, the column SHOULD be a primary key. If there
    # is no primary key column, the first column SHALL be of type INTEGER and
    # SHALL contain unique values for each row.

    q = conn.execute(f"PRAGMA table_info({quote(table)});")
    fields = []
    for field in q:
        if field["pk"]:
            return field["name"]
        fields.append(field)

    if fields[0]["type"] == "INTEGER":
        return fields[0]["name"]
    else:
        raise ValueError("No valid GeoPackage primary key field found")
