from sno.schema import Schema, ColumnSchema

from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.dialects.mssql.base import MSDialect


_PREPARER = IdentifierPreparer(MSDialect())


def quote(ident):
    return _PREPARER.quote(ident)


V2_TYPE_TO_MS_TYPE = {
    "boolean": "bit",
    "blob": "varbinary",
    "date": "date",
    "float": {0: "real", 32: "real", 64: "float"},
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
    "text": "nvarchar",
    "time": "time",
    "timestamp": "datetimeoffset",
}

MS_TYPE_TO_V2_TYPE = {
    "bit": "boolean",
    "tinyint": ("integer", 8),
    "smallint": ("integer", 16),
    "int": ("integer", 32),
    "bigint": ("integer", 64),
    "real": ("float", 32),
    "float": ("float", 64),
    "binary": "blob",
    "char": "text",
    "date": "date",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "datetimeoffset": "timestamp",
    "decimal": "numeric",
    "geography": "geometry",
    "geometry": "geometry",
    "nchar": "text",
    "numeric": "numeric",
    "nvarchar": "text",
    "ntext": "text",
    "text": "text",
    "time": "time",
    "varchar": "text",
    "varbinary": "blob",
}

# Types that can't be roundtripped perfectly in SQL Server, and what they end up as.
APPROXIMATED_TYPES = {"interval": "text"}
# Note that although this means that all other V2 types above can be roundtripped, it
# doesn't mean that extra type info is always preserved. Specifically, extra
# geometry type info - the geometry type and CRS - is not roundtripped.


def v2_schema_to_sqlserver_spec(schema, v2_obj):
    """
    Generate the SQL CREATE TABLE spec from a V2 object eg:
    'fid INTEGER, geom GEOMETRY(POINT,2136), desc VARCHAR(128), PRIMARY KEY(fid)'
    """
    result = [f"{quote(col.name)} {v2_type_to_ms_type(col, v2_obj)}" for col in schema]

    if schema.pk_columns:
        pk_col_names = ", ".join((quote(col.name) for col in schema.pk_columns))
        result.append(f"PRIMARY KEY({pk_col_names})")

    return ", ".join(result)


def v2_type_to_ms_type(column_schema, v2_obj):
    """Convert a v2 schema type to a SQL server type."""

    v2_type = column_schema.data_type
    extra_type_info = column_schema.extra_type_info

    ms_type_info = V2_TYPE_TO_MS_TYPE.get(v2_type)
    if ms_type_info is None:
        raise ValueError(f"Unrecognised data type: {v2_type}")

    if isinstance(ms_type_info, dict):
        return ms_type_info.get(extra_type_info.get("size", 0))

    ms_type = ms_type_info

    if ms_type in ("varchar", "nvarchar", "varbinary"):
        length = extra_type_info.get("length", None)
        return f"{ms_type}({length})" if length is not None else f"{ms_type}(max)"

    if ms_type == "numeric":
        precision = extra_type_info.get("precision", None)
        scale = extra_type_info.get("scale", None)
        if precision is not None and scale is not None:
            return f"numeric({precision},{scale})"
        elif precision is not None:
            return f"numeric({precision})"
        else:
            return "numeric"

    return ms_type


def sqlserver_to_v2_schema(ms_table_info, id_salt):
    """Generate a V2 schema from the given SQL server metadata."""
    return Schema([_sqlserver_to_column_schema(col, id_salt) for col in ms_table_info])


def _sqlserver_to_column_schema(ms_col_info, id_salt):
    """
    Given the MS column info for a particular column, converts it to a ColumnSchema.

    Parameters:
    ms_col_info - info about a single column from ms_table_info.
    id_salt - the UUIDs of the generated ColumnSchema are deterministic and depend on
              the name and type of the column, and on this salt.
    """
    name = ms_col_info["column_name"]
    pk_index = ms_col_info["pk_ordinal_position"]
    if pk_index is not None:
        pk_index -= 1
    data_type, extra_type_info = _ms_type_to_v2_type(ms_col_info)

    col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
    return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)


def _ms_type_to_v2_type(ms_col_info):
    v2_type_info = MS_TYPE_TO_V2_TYPE.get(ms_col_info["data_type"])

    if isinstance(v2_type_info, tuple):
        v2_type = v2_type_info[0]
        extra_type_info = {"size": v2_type_info[1]}
    else:
        v2_type = v2_type_info
        extra_type_info = {}

    if v2_type == "geometry":
        return v2_type, extra_type_info

    if v2_type == "text":
        length = ms_col_info["character_maximum_length"] or None
        if length is not None:
            extra_type_info["length"] = length

    if v2_type == "numeric":
        extra_type_info["precision"] = ms_col_info["numeric_precision"] or None
        extra_type_info["scale"] = ms_col_info["numeric_scale"] or None

    return v2_type, extra_type_info
