import re
from .dataset2 import Schema, ColumnSchema
from osgeo.osr import SpatialReference


GPKG_META_ITEMS = (
    "gpkg_contents",
    "gpkg_geometry_columns",
    "gpkg_spatial_ref_sys",
    "sqlite_table_info",
    "gkpg_metadata",
    "gpkg_metadata_reference",
)


def get_meta_item(dataset, path):
    if path not in GPKG_META_ITEMS:
        raise KeyError(f"Not a gpkg meta_item: {path}")

    if path == "gpkg_contents":
        return generate_gpkg_contents(dataset)
    elif path == "gpkg_geometry_columns":
        return generate_gpkg_geometry_columns(dataset)
    elif path == "gpkg_spatial_ref_sys":
        return generate_gpkg_spatial_ref_sys(dataset)
    elif path == "sqlite_table_info":
        return generate_sqlite_table_info(dataset)
    elif path == "gpkg_metadata" or path == "gpkg_metadata_reference":
        return None


def generate_gpkg_contents(dataset2):
    """Generate a gpkg_contents meta item from a dataset."""
    gpkg_spatial_ref_sys = dataset2.get_meta_item("gpkg_spatial_ref_sys")
    is_spatial = bool(gpkg_spatial_ref_sys)

    result = {
        "identifier": dataset2.get_meta_item("title"),
        "description": dataset2.get_meta_item("description"),
        "table_name": dataset2.tree.name,
        "data_type": "features" if is_spatial else "attributes",
    }
    if is_spatial:
        result["srs_id"] = gpkg_spatial_ref_sys[0]["srs_id"]
    return result


def generate_gpkg_geometry_columns(dataset2):
    """Generate a gpkg_geometry_columns meta item from a dataset."""
    geom_columns = _get_geometry_columns(dataset2.schema)
    if not geom_columns:
        return None

    geometry_type = geom_columns[0].extra_type_info["geometryType"]
    type_name, *zm = geometry_type.split(" ", 1)
    zm = zm[0] if zm else ""
    z = 1 if "Z" in zm else 0
    m = 1 if "M" in zm else 0
    gpkg_spatial_ref_sys = dataset2.get_meta_item("gpkg_spatial_ref_sys")
    return {
        "table_name": dataset2.tree.name,
        "column_name": geom_columns[0].name,
        "geometry_type_name": type_name,
        "srs_id": gpkg_spatial_ref_sys[0]["srs_id"],
        "z": z,
        "m": m,
    }


def generate_gpkg_spatial_ref_sys(dataset):
    """Generate a gpkg_spatial_ref_sys meta item from a dataset."""
    geom_columns = _get_geometry_columns(dataset.schema)
    if not geom_columns:
        return []

    srs_pathname = geom_columns[0].extra_type_info["geometrySRS"]
    definition = dataset.get_srs_definition(srs_pathname)
    return wkt_to_gpkg_spatial_ref_sys(definition)


DEFAULT_GPKG_SPATIAL_REF_SYS = [
    {
        "srs_name": "Unknown CRS",
        "definition": "",
        "organization": "EPSG",
        "srs_id": 0,
        "organization_coordsys_id": 0,
        "description": None,
    }
]


def wkt_to_gpkg_spatial_ref_sys(wkt):
    """Given a WKT srs definition, generate a gpkg_spatial_ref_sys meta item."""
    return _gpkg_spatial_ref_sys(SpatialReference(wkt), wkt)


def osgeo_to_gpkg_spatial_ref_sys(spatial_ref):
    """Given an osgeo SpatialReference, generate a gpkg_spatial_ref_sys meta item."""

    return _gpkg_spatial_ref_sys(spatial_ref, spatial_ref.ExportToWkt())


def _gpkg_spatial_ref_sys(spatial_ref, wkt):
    spatial_ref.AutoIdentifyEPSG()
    return [
        {
            "srs_name": spatial_ref.GetName(),
            "definition": wkt,
            "organization": spatial_ref.GetAuthorityName(None),
            "srs_id": spatial_ref.GetAuthorityCode(None),
            "organization_coordsys_id": spatial_ref.GetAuthorityCode(None),
            "description": None,
        }
    ]


DEFAULT_SRS_STR = "EPSG:0"


def wkt_to_srs_str(wkt):
    """Given a WKT srs definition, generate a sensible name for it."""
    return osgeo_to_srs_str(SpatialReference(wkt))


def osgeo_to_srs_str(spatial_ref):
    """Given a osgeo SpatialReference, generate a sensible name for it."""
    auth_name = spatial_ref.GetAuthorityName(None)
    auth_code = spatial_ref.GetAuthorityCode(None)
    return f"{auth_name}:{auth_code}"


def generate_sqlite_table_info(dataset):
    """Generate a sqlite_table_info meta item from a dataset."""
    return [_column_schema_to_gpkg(i, col) for i, col in enumerate(dataset.schema)]


def _get_geometry_columns(schema):
    return [c for c in schema.columns if c.data_type == "geometry"]


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
            gpkg_geometry_columns, gpkg_spatial_ref_sys,
        )
    else:
        data_type, extra_type_info = gpkg_type_to_v2_type(sqlite_col_info["type"])

    col_id = ColumnSchema.deterministic_id(name, data_type, id_salt)
    return ColumnSchema(col_id, name, data_type, pk_index, **extra_type_info)


def _column_schema_to_gpkg(cid, column_schema):
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


def _gkpg_geometry_columns_to_v2_type(ggc, gsrs):
    geometry_type = ggc["geometry_type_name"]
    z = "Z" if ggc["z"] else ""
    m = "M" if ggc["m"] else ""

    srs_str = DEFAULT_SRS_STR
    if gsrs and gsrs[0]["definition"]:
        srs_str = wkt_to_srs_str(gsrs[0]["definition"])

    extra_type_info = {
        "geometryType": f"{geometry_type} {z}{m}".strip(),
        "geometrySRS": srs_str,
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
