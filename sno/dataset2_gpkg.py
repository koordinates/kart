import re
from .dataset2 import Schema, ColumnSchema


def v2_to_gpkg_contents(dataset2):
    """Generate a gpkg_contents meta item from a dataset v2"""
    geom_columns = _get_geometry_columns(dataset2.schema)
    is_spatial = bool(geom_columns)
    result = {
        "identifier": dataset2.get_meta_item("title"),
        "description": dataset2.get_meta_item("description"),
        "table_name": dataset2.tree.name,
        "data_type": "features" if is_spatial else "attributes",
    }
    if is_spatial:
        result["srs_id"] = srs_str_to_int(
            geom_columns[0].extra_type_info["geometrySRS"]
        )
    return result


def v2_to_gpkg_geometry_columns(dataset2):
    """Generate a gpkg_geometry_columns meta item from a dataset v2"""
    geom_columns = _get_geometry_columns(dataset2.schema)
    if not geom_columns:
        return None

    geom_column = geom_columns[0]
    type_name, *zm = geom_column.extra_type_info["geometryType"].split(" ", 1)
    srs_id = srs_str_to_int(geom_column.extra_type_info["geometrySRS"])
    zm = zm[0] if zm else ""
    z = 1 if "Z" in zm else 0
    m = 1 if "M" in zm else 0
    return {
        "table_name": dataset2.tree.name,
        "column_name": geom_column.name,
        "geometry_type_name": type_name,
        "srs_id": srs_id,
        "z": z,
        "m": m,
    }


def v2_to_gpkg_spatial_ref_sys(dataset2):
    """Generate a gpkg_spatial_ref_sys meta item from a dataset v2"""
    geom_columns = _get_geometry_columns(dataset2.schema)
    if not geom_columns:
        return []

    srs_str = geom_columns[0].extra_type_info["geometrySRS"]
    srs_id = srs_str_to_int(srs_str)
    definition = dataset2.get_meta_item(f"srs/{srs_str}.wkt")
    # This should be more complicated too.
    # TODO: srs_name, description.
    return [
        {
            "srs_name": srs_str,  # This name is not quite right.
            "definition": definition,
            "organization": "EPSG",
            "srs_id": srs_id,
            "organization_coordsys_id": srs_id,
        }
    ]


def v2_to_sqlite_table_info(dataset2):
    return [_columnschema_to_gpkg(i, col) for i, col in enumerate(dataset2.schema)]


def _get_geometry_columns(schema):
    return [c for c in schema.columns if c.data_type == "geometry"]


def srs_str_to_int(srs_str):
    # This should be more complicated.
    if srs_str.startswith("EPSG:"):
        srs_str = srs_str[5:]
    if srs_str.isdigit():
        return int(srs_str)
    raise ValueError(f"Can't parse SRS ID: {srs_str}")


def srs_int_to_str(srs_int):
    # This should be more complicated
    return f"EPSG:{srs_int}"


def gpkg_to_v2_schema(sqlite_table_info, gpkg_geometry_columns, id_salt):
    """Generate a v2 Schema from the given gpkg meta items."""
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


def _columnschema_to_gpkg(cid, column_schema):
    is_pk = 1 if column_schema.pk_index is not None else 0
    return {
        "cid": cid,
        "name": column_schema.name,
        "pk": is_pk,
        "type": v2_type_to_gpkg_type(column_schema),
        "notnull": 0,
        "dflt_value": None,
    }


_GPKG_TYPE_TO_V2_TYPE = {
    "SMALLINT": ("integer", {"size": 16}),
    "MEDIUMINT": ("integer", {"size": 32}),
    "INTEGER": ("integer", {"size": 64}),
    "REAL": ("float", {"size": 32}),
    "FLOAT": ("float", {"size": 32}),
    "DOUBLE": ("float", {"size": 64}),
}


_V2_TYPE_TO_GPKG_TYPE = {
    "integer": {0: "INTEGER", 16: "SMALLINT", 32: "MEDIUMINT", 64: "INTEGER"},
    "float": {0: "FLOAT", 32: "FLOAT", 64: "DOUBLE"},
}


def gpkg_type_to_v2_type(gkpg_type):
    """Convert a gpkg type to v2 schema type."""
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
        "geometrySRS": srs_int_to_str(srs_id),
    }
    return "geometry", extra_type_info


def v2_type_to_gpkg_type(column_schema):
    """Convert a v2 schema type to a gpkg type."""
    v2_type = column_schema.data_type
    extra_type_info = column_schema.extra_type_info
    if column_schema.data_type == "geometry":
        return extra_type_info["geometryType"].split(" ", 1)[0]

    gpkg_types = _V2_TYPE_TO_GPKG_TYPE.get(v2_type)
    if gpkg_types:
        return gpkg_types.get(extra_type_info.get("size", 0))

    length = extra_type_info.get("length", None)
    if length:
        return f"{v2_type.upper()}({length})"

    return v2_type.upper()
