from kart import crs_util
from kart.schema import Schema, ColumnSchema

from sqlalchemy.dialects.mssql.base import MSIdentifierPreparer, MSDialect


_PREPARER = MSIdentifierPreparer(MSDialect())


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
    "interval": "text",  # Approximated
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

# Extra type info that might be missing/extra due to an approximated type.
APPROXIMATED_TYPES_EXTRA_TYPE_INFO = ("length",)


# Used for constraining a column to be of a certain type, including subtypes of that type.
# The CHECK need to explicitly list all types and subtypes, eg for SURFACE:
# >>> CHECK(geom.STGeometryType() IN ('SURFACE','POLYGON','CURVEPOLYGON'))
_MS_GEOMETRY_SUBTYPES = {
    "Geometry": set(["Point", "Curve", "Surface", "GeometryCollection"]),
    "Curve": set(["LineString", "CircularString", "CompoundCurve"]),
    "Surface": set(["Polygon", "CurvePolygon"]),
    "GeometryCollection": set(["MultiPoint", "MultiCurve", "MultiSurface"]),
    "MultiCurve": set(["MultiLineString"]),
    "MultiSurface": set(["MultiPolygon"]),
}


# Adds all CURVE subtypes to GEOMETRY's subtypes since CURVE is a subtype of GEOMETRY, and so on.
def _build_transitive_subtypes(type_):
    subtypes = _MS_GEOMETRY_SUBTYPES.get(type_, set())
    sub_subtypes = set()
    for subtype in subtypes:
        sub_subtypes |= _build_transitive_subtypes(subtype)
    subtypes |= sub_subtypes
    # The type itself should also be one of its "subtypes".
    subtypes.add(type_)
    # Also key this data by upper case name, so we can find it in a case-insensitive manner
    # (since V2 geometry types are uppercase).
    _MS_GEOMETRY_SUBTYPES[type_.upper()] = subtypes
    return subtypes


_build_transitive_subtypes("Geometry")


def v2_schema_to_sqlserver_spec(schema, v2_obj):
    """
    Generate the SQL CREATE TABLE spec from a V2 object eg:
    'fid INTEGER, geom GEOMETRY(POINT,2136), desc VARCHAR(128), PRIMARY KEY(fid)'
    """
    result = [v2_column_schema_to_sqlserver_spec(col, v2_obj) for col in schema]

    if schema.pk_columns:
        pk_col_names = ", ".join((quote(col.name) for col in schema.pk_columns))
        result.append(f"PRIMARY KEY({pk_col_names})")

    return ", ".join(result)


def v2_column_schema_to_sqlserver_spec(column_schema, v2_obj):
    name = column_schema.name
    ms_type = v2_type_to_ms_type(column_schema)
    constraints = []

    if ms_type == "geometry":
        extra_type_info = column_schema.extra_type_info
        geometry_type = extra_type_info.get("geometryType")
        if geometry_type is not None:
            geometry_type = geometry_type.split(" ")[0].upper()
            if geometry_type != "GEOMETRY":
                constraints.append(_geometry_type_constraint(name, geometry_type))

        crs_name = extra_type_info.get("geometryCRS")
        crs_id = crs_util.get_identifier_int_from_dataset(v2_obj, crs_name)
        if crs_id is not None:
            constraints.append(_geometry_crs_constraint(name, crs_id))

    if constraints:
        constraint = f"CHECK({' AND '.join(constraints)})"
        return " ".join([quote(column_schema.name), ms_type, constraint])

    return " ".join([quote(name), ms_type])


def _geometry_type_constraint(col_name, geometry_type):
    ms_geometry_types = _MS_GEOMETRY_SUBTYPES.get(geometry_type.upper())
    ms_geometry_types_sql = ",".join(f"'{g}'" for g in ms_geometry_types)

    result = f"({quote(col_name)}).STGeometryType()"
    if len(ms_geometry_types) > 1:
        result += f" IN ({ms_geometry_types_sql})"
    else:
        result += f" = {ms_geometry_types_sql}"

    return result


def _geometry_crs_constraint(col_name, crs_id):
    return f"({quote(col_name)}).STSrid = {crs_id}"


def v2_type_to_ms_type(column_schema):
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


def sqlserver_to_v2_schema(ms_table_info, ms_crs_info, id_salt):
    """Generate a V2 schema from the given SQL server metadata."""
    return Schema(
        [
            _sqlserver_to_column_schema(col, ms_crs_info, id_salt)
            for col in ms_table_info
        ]
    )


def _sqlserver_to_column_schema(ms_col_info, ms_crs_info, id_salt):
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

    if ms_col_info["data_type"] in ("geometry", "geography"):
        data_type, extra_type_info = _ms_type_to_v2_geometry_type(
            ms_col_info, ms_crs_info
        )
    else:
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

    if v2_type in ("text", "blob"):
        length = ms_col_info["character_maximum_length"] or None
        if length is not None and length > 0:
            extra_type_info["length"] = length

    if v2_type == "numeric":
        extra_type_info["precision"] = ms_col_info["numeric_precision"] or None
        extra_type_info["scale"] = ms_col_info["numeric_scale"] or None

    return v2_type, extra_type_info


def _ms_type_to_v2_geometry_type(ms_col_info, ms_crs_info):
    extra_type_info = {"geometryType": "geometry"}

    crs_row = next(
        (r for r in ms_crs_info if r["column_name"] == ms_col_info["column_name"]), None
    )
    if crs_row:
        auth_name = crs_row['authority_name']
        auth_code = crs_row['authorized_spatial_reference_id']
        if not auth_name and not auth_code:
            auth_name, auth_code = "CUSTOM", crs_row['srid']
        geometry_crs = f"{auth_name}:{auth_code}"
        extra_type_info["geometryCRS"] = geometry_crs

    return "geometry", extra_type_info
