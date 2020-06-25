import re
from .dataset2 import Schema, ColumnSchema

_GPKG_TYPE_TO_V2_TYPE = {
    "SMALLINT": ("integer", {"size": 16}),
    "MEDIUMINT": ("integer", {"size": 32}),
    "INTEGER": ("integer", {"size": 64}),
    "REAL": ("float", {"size": 32}),
    "FLOAT": ("float", {"size": 32}),
    "DOUBLE": ("float", {"size": 64}),
}


def gpkg_to_v2_schema(sqlite_table_info, gpkg_geometry_columns, id_salt):
    return Schema(
        [
            _gkpg_to_columnschema(col, gpkg_geometry_columns, id_salt)
            for col in sorted(sqlite_table_info, key=_sort_by_cid)
        ]
    )


def _sort_by_cid(sqlite_col_info):
    return sqlite_col_info["cid"]


def _gkpg_to_columnschema(sqlite_col_info, gpkg_geometry_columns, id_salt):
    name = sqlite_col_info["name"]
    pk_index = 0 if sqlite_col_info["pk"] == 1 else None
    if gpkg_geometry_columns and name == gpkg_geometry_columns["column_name"]:
        data_type, extra_type_info = _gkpg_geometry_columns_to_v2_type(
            gpkg_geometry_columns
        )
    else:
        data_type, extra_type_info = gpkg_type_to_v2_type(sqlite_col_info["type"])

    col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
    return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)


def gpkg_type_to_v2_type(gkpg_type):
    m = re.match(r"^(TEXT|BLOB)\(([0-9]+)\)$", gkpg_type)
    if m:
        return m.group(1).lower(), {"length": int(m.group(2))}
    v2_type = _GPKG_TYPE_TO_V2_TYPE.get(gkpg_type)
    if v2_type is None:
        v2_type = (gkpg_type.lower(), {})
    return v2_type


def _gkpg_geometry_columns_to_v2_type(ggc):
    geometry_type = ggc["geometry_type_name"]
    z = "Z" if ggc["z"] else ""
    m = "M" if ggc["m"] else ""
    srs_id = ggc["srs_id"]
    extra_type_info = {
        "geometryType": f"{geometry_type} {z}{m}".strip(),
        "geometrySRS": f"EPSG:{srs_id}",
    }
    return "geometry", extra_type_info
