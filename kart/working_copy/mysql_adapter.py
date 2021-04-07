from kart import crs_util
from kart.schema import Schema, ColumnSchema

from sqlalchemy.dialects.mysql.base import MySQLIdentifierPreparer, MySQLDialect


_PREPARER = MySQLIdentifierPreparer(MySQLDialect())


V2_TYPE_TO_MYSQL_TYPE = {
    "boolean": "bit(1)",
    "blob": "longblob",
    "date": "date",
    "float": {0: "float", 32: "float", 64: "double precision"},
    "geometry": "geometry",
    "integer": {
        0: "int",
        8: "tinyint",
        16: "smallint",
        32: "int",
        64: "bigint",
    },
    "interval": "text",
    "numeric": "numeric",
    "text": "longtext",
    "time": "time",
    "timestamp": "timestamp",
}


MYSQL_TYPE_TO_V2_TYPE = {
    "bit": "boolean",
    "tinyint": ("integer", 8),
    "smallint": ("integer", 16),
    "int": ("integer", 32),
    "bigint": ("integer", 64),
    "float": ("float", 32),
    "double": ("float", 64),
    "double precision": ("float", 64),
    "binary": "blob",
    "blob": "blob",
    "char": "text",
    "date": "date",
    "datetime": "timestamp",
    "decimal": "numeric",
    "geometry": "geometry",
    "numeric": "numeric",
    "text": "text",
    "time": "time",
    "timestamp": "timestamp",
    "varchar": "text",
    "varbinary": "blob",
}

for prefix in ["tiny", "medium", "long"]:
    MYSQL_TYPE_TO_V2_TYPE[f"{prefix}blob"] = "blob"
    MYSQL_TYPE_TO_V2_TYPE[f"{prefix}text"] = "text"


# Types that can't be roundtripped perfectly in MySQL, and what they end up as.
APPROXIMATED_TYPES = {"interval": "text"}

# Extra type info that might be missing/extra due to an approximated type.
APPROXIMATED_TYPES_EXTRA_TYPE_INFO = ("length",)

MYSQL_GEOMETRY_TYPES = {
    "GEOMETRY",
    "POINT",
    "LINESTRING",
    "POLYGON",
    "MULTIPOINT",
    "MULTILINESTRING",
    "MULTIPOLYGON",
    "GEOMETRYCOLLECTION",
}


def quote(ident):
    return _PREPARER.quote(ident)


def v2_schema_to_mysql_spec(schema, v2_obj):
    """
    Generate the SQL CREATE TABLE spec from a V2 object eg:
    'fid INTEGER, geom POINT WITH CRSID 2136, desc VARCHAR(128), PRIMARY KEY(fid)'
    """
    result = [_v2_column_schema_to_mysql_spec(col, v2_obj) for col in schema]

    if schema.pk_columns:
        pk_col_names = ", ".join((quote(col.name) for col in schema.pk_columns))
        result.append(f"PRIMARY KEY({pk_col_names})")

    return ", ".join(result)


def _v2_column_schema_to_mysql_spec(column_schema, v2_obj):
    name = column_schema.name
    mysql_type = _v2_type_to_mysql_type(column_schema, v2_obj)

    return " ".join([quote(name), mysql_type])


_MAX_SPECIFIABLE_LENGTH = 0xFFFF


def _v2_type_to_mysql_type(column_schema, v2_obj):
    """Convert a v2 schema type to a MySQL type."""
    v2_type = column_schema.data_type
    if v2_type == "geometry":
        return _v2_geometry_type_to_mysql_type(column_schema, v2_obj)

    extra_type_info = column_schema.extra_type_info

    mysql_type_info = V2_TYPE_TO_MYSQL_TYPE.get(v2_type)
    if mysql_type_info is None:
        raise ValueError(f"Unrecognised data type: {v2_type}")

    if isinstance(mysql_type_info, dict):
        return mysql_type_info.get(extra_type_info.get("size", 0))

    mysql_type = mysql_type_info

    length = extra_type_info.get("length", None)
    if length and length > 0 and length <= _MAX_SPECIFIABLE_LENGTH:
        if mysql_type == "longtext":
            return f"varchar({length})"
        elif mysql_type == "longblob":
            return f"varbinary({length})"

    if mysql_type == "numeric":
        precision = extra_type_info.get("precision", None)
        scale = extra_type_info.get("scale", None)
        if precision is not None and scale is not None:
            return f"numeric({precision},{scale})"
        elif precision is not None:
            return f"numeric({precision})"
        else:
            return "numeric"

    return mysql_type


def _v2_geometry_type_to_mysql_type(column_schema, v2_obj):
    extra_type_info = column_schema.extra_type_info
    mysql_type = extra_type_info.get("geometryType", "geometry").split(" ")[0]

    crs_name = extra_type_info.get("geometryCRS")
    crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
    if crs_id is not None:
        mysql_type += f" SRID {crs_id}"

    return mysql_type


def sqlserver_to_v2_schema(mysql_table_info, mysql_crs_info, id_salt):
    """Generate a V2 schema from the given My SQL metadata."""
    return Schema(
        [
            _mysql_to_column_schema(col, mysql_crs_info, id_salt)
            for col in mysql_table_info
        ]
    )


def _mysql_to_column_schema(mysql_col_info, mysql_crs_info, id_salt):
    """
    Given the MySQL column info for a particular column, converts it to a ColumnSchema.

    Parameters:
    mysql_col_info - info about a single column from mysql_table_info.
    mysql_crs_info - info about all the CRS, in case this column is a geometry column.
    id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
              the name and type of the column, and on this salt.
    """
    name = mysql_col_info["COLUMN_NAME"]
    pk_index = mysql_col_info["pk_ordinal_position"]
    if pk_index is not None:
        pk_index -= 1
    if mysql_col_info["DATA_TYPE"].upper() in MYSQL_GEOMETRY_TYPES:
        data_type, extra_type_info = _mysql_type_to_v2_geometry_type(
            mysql_col_info, mysql_crs_info
        )
    else:
        data_type, extra_type_info = _mysql_type_to_v2_type(mysql_col_info)

    col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
    return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)


def _mysql_type_to_v2_type(mysql_col_info):
    v2_type_info = MYSQL_TYPE_TO_V2_TYPE.get(mysql_col_info["DATA_TYPE"])

    if isinstance(v2_type_info, tuple):
        v2_type = v2_type_info[0]
        extra_type_info = {"size": v2_type_info[1]}
    else:
        v2_type = v2_type_info
        extra_type_info = {}

    if v2_type in ("text", "blob"):
        length = mysql_col_info["CHARACTER_MAXIMUM_LENGTH"] or None
        if length is not None and length > 0 and length <= _MAX_SPECIFIABLE_LENGTH:
            extra_type_info["length"] = length

    if v2_type == "numeric":
        extra_type_info["precision"] = mysql_col_info["NUMERIC_PRECISION"] or None
        extra_type_info["scale"] = mysql_col_info["NUMERIC_SCALE"] or None

    return v2_type, extra_type_info


def _mysql_type_to_v2_geometry_type(mysql_col_info, mysql_crs_info):
    geometry_type = mysql_col_info["DATA_TYPE"].upper()
    geometry_crs = None

    crs_id = mysql_col_info["SRS_ID"]
    if crs_id:
        crs_info = next((r for r in mysql_crs_info if r["SRS_ID"] == crs_id), None)
        if crs_info:
            geometry_crs = crs_util.get_identifier_str(crs_info["DEFINITION"])

    return "geometry", {"geometryType": geometry_type, "geometryCRS": geometry_crs}
