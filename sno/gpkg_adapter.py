import re
from .exceptions import NotYetImplemented
from .schema import Schema, ColumnSchema
from osgeo.osr import SpatialReference


GPKG_META_ITEMS = (
    "gpkg_contents",
    "gpkg_geometry_columns",
    "gpkg_spatial_ref_sys",
    "sqlite_table_info",
    "gpkg_metadata",
    "gpkg_metadata_reference",
)


V2_META_ITEMS = (
    "title",
    "description",
    "schema",
    "metadata/dataset.json",
)


def is_gpkg_meta_item(path):
    return path in GPKG_META_ITEMS


def generate_gpkg_meta_item(v2_dataset, path):
    if not is_gpkg_meta_item(path):
        raise KeyError(f"Not a gpkg meta_item: {path}")

    if path == "gpkg_contents":
        return generate_gpkg_contents(v2_dataset)
    elif path == "gpkg_geometry_columns":
        return generate_gpkg_geometry_columns(v2_dataset)
    elif path == "gpkg_spatial_ref_sys":
        return generate_gpkg_spatial_ref_sys(v2_dataset)
    elif path == "sqlite_table_info":
        return generate_sqlite_table_info(v2_dataset)
    elif path == "gpkg_metadata" or path == "gpkg_metadata_reference":
        # TODO - store and generate this metadata.
        return None


def is_v2_meta_item(path):
    return path in V2_META_ITEMS or path.startswith("srs/")


def generate_v2_meta_item(v1_dataset, path, id_salt=None):
    if not is_v2_meta_item(path):
        raise KeyError(f"Not a v2 meta_item: {path}")

    if path == "title":
        return extract_title(v1_dataset)

    elif path == "description":
        description = v1_dataset.get_meta_item("gpkg_contents").get("description")
        return description if description else None

    elif path == "schema":
        return gpkg_to_v2_schema(
            v1_dataset.get_meta_item("sqlite_table_info"),
            v1_dataset.get_meta_item("gpkg_geometry_columns"),
            v1_dataset.get_meta_item("gpkg_spatial_ref_sys"),
            id_salt or v1_dataset.name,
        ).to_column_dicts()
    elif path == "metadata/dataset.json":
        # TODO - store and generate this metadata.
        return None

    elif path.startswith("srs/"):
        gpkg_spatial_ref_sys = v1_dataset.get_meta_item("gpkg_spatial_ref_sys") or ()
        for gsrs in gpkg_spatial_ref_sys:
            definition = gsrs["definition"]
            if not definition or definition == "undefined":
                continue
            if wkt_to_v2_name(definition) == path:
                return definition
        raise KeyError(f"No SRS found for {path}")


def iter_v2_meta_items(v1_dataset, id_salt=None):
    for path in V2_META_ITEMS:
        result = generate_v2_meta_item(v1_dataset, path, id_salt=id_salt)
        if result:
            yield path, result

    gpkg_spatial_ref_sys = v1_dataset.get_meta_item("gpkg_spatial_ref_sys") or ()
    for gsrs in gpkg_spatial_ref_sys:
        definition = gsrs["definition"]
        if not definition or definition == "undefined":
            continue
        yield wkt_to_v2_name(definition), definition


def extract_title(v1_dataset):
    """Extract the dataset title from a v1 dataset."""
    identifier = v1_dataset.get_meta_item("gpkg_contents").get("identifier")
    # FIXME: find a better way of roundtripping identifiers?
    identifier_prefix = f"{v1_dataset.name}: "
    if identifier.startswith(identifier_prefix):
        identifier = identifier[len(identifier_prefix) :]
    return identifier if identifier else None


def generate_gpkg_contents(v2_dataset):
    """Generate a gpkg_contents meta item from a v2 dataset."""
    is_spatial = bool(_get_geometry_columns(v2_dataset.schema))

    result = {
        "identifier": generate_unique_identifier(v2_dataset),
        "description": v2_dataset.get_meta_item("description") or "",
        "table_name": v2_dataset.name,
        "data_type": "features" if is_spatial else "attributes",
    }
    if is_spatial:
        result["srs_id"] = _gpkg_srs_id(v2_dataset)
    return result


def generate_unique_identifier(v2_dataset):
    # FIXME: find a better way of roundtripping identifiers?
    identifier = v2_dataset.get_meta_item("title") or ""
    identifier_prefix = f"{v2_dataset.name}: "
    if not identifier.startswith(identifier_prefix):
        identifier = identifier_prefix + identifier
    return identifier


def generate_gpkg_geometry_columns(v2_dataset):
    """Generate a gpkg_geometry_columns meta item from a dataset."""
    geom_columns = _get_geometry_columns(v2_dataset.schema)
    if not geom_columns:
        return None

    geometry_type = geom_columns[0].extra_type_info["geometryType"]
    type_name, *zm = geometry_type.split(" ", 1)
    zm = zm[0] if zm else ""
    z = 1 if "Z" in zm else 0
    m = 1 if "M" in zm else 0

    return {
        "table_name": v2_dataset.name,
        "column_name": geom_columns[0].name,
        "geometry_type_name": type_name,
        "srs_id": _gpkg_srs_id(v2_dataset),
        "z": z,
        "m": m,
    }


def generate_gpkg_spatial_ref_sys(v2_dataset):
    """Generate a gpkg_spatial_ref_sys meta item from a dataset."""
    geom_columns = _get_geometry_columns(v2_dataset.schema)
    if not geom_columns:
        return []

    srs_pathname = geom_columns[0].extra_type_info["geometrySRS"]
    if not srs_pathname:
        return []
    definition = v2_dataset.get_srs_definition(srs_pathname)
    return wkt_to_gpkg_spatial_ref_sys(definition)


def _gpkg_srs_id(dataset):
    gsrs = dataset.get_meta_item("gpkg_spatial_ref_sys")
    return gsrs[0]["srs_id"] if gsrs else 0


def wkt_to_gpkg_spatial_ref_sys(wkt):
    """Given a WKT srs definition, generate a gpkg_spatial_ref_sys meta item."""
    return _gpkg_spatial_ref_sys(SpatialReference(wkt), wkt)


def osgeo_to_gpkg_spatial_ref_sys(spatial_ref):
    """Given an osgeo SpatialReference, generate a gpkg_spatial_ref_sys meta item."""
    return _gpkg_spatial_ref_sys(spatial_ref, spatial_ref.ExportToWkt())


def _gpkg_spatial_ref_sys(spatial_ref, wkt):
    # TODO: Better support for custom WKT. https://github.com/koordinates/sno/issues/148
    spatial_ref.AutoIdentifyEPSG()
    organization = spatial_ref.GetAuthorityName(None) or "NONE"
    srs_id = spatial_ref.GetAuthorityCode(None) or 0
    return [
        {
            "srs_name": spatial_ref.GetName(),
            "definition": wkt,
            "organization": organization,
            "srs_id": srs_id,
            "organization_coordsys_id": srs_id,
            "description": None,
        }
    ]


def wkt_to_v2_name(wkt):
    return f"srs/{wkt_to_srs_str(wkt)}.wkt"


def wkt_to_srs_str(wkt):
    """Given a WKT srs definition, generate a sensible identifier for it."""
    return osgeo_to_srs_str(SpatialReference(wkt))


def osgeo_to_srs_str(spatial_ref):
    """Given a osgeo SpatialReference, generate a identifier name for it."""
    auth_name = spatial_ref.GetAuthorityName(None)
    auth_code = spatial_ref.GetAuthorityCode(None)
    return f"{auth_name}:{auth_code}"


def generate_sqlite_table_info(v2_dataset):
    """Generate a sqlite_table_info meta item from a dataset."""
    is_spatial = bool(_get_geometry_columns(v2_dataset.schema))
    return [
        _column_schema_to_gpkg(i, col, is_spatial)
        for i, col in enumerate(v2_dataset.schema)
    ]


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


def _column_schema_to_gpkg(cid, column_schema, is_spatial):
    is_pk = 1 if column_schema.pk_index is not None else 0
    return {
        "cid": cid,
        "name": column_schema.name,
        "pk": is_pk,
        "type": v2_type_to_gpkg_type(column_schema, is_spatial),
        "notnull": 0,
        "dflt_value": None,
    }


# From http://www.geopackage.org/spec/
# The columns of tables in a GeoPackage SHALL only be declared using one of the following data types:
# BOOLEAN, TINYINT, SMALLINT, MEDIUMINT, INT / INTEGER, FLOAT, DOUBLE / REAL,
# TEXT{(max_len)}, BLOB{(max_len)}, DATE, DATETIME, <geometry_type_name>


_GPKG_TYPE_TO_V2_TYPE = {
    "BOOLEAN": "boolean",
    "TINYINT": ("integer", {"size": 8}),
    "SMALLINT": ("integer", {"size": 16}),
    "MEDIUMINT": ("integer", {"size": 32}),
    "INT": ("integer", {"size": 64}),
    "INTEGER": ("integer", {"size": 64}),
    "FLOAT": ("float", {"size": 32}),
    "DOUBLE": ("float", {"size": 64}),
    "REAL": ("float", {"size": 64}),
    "TEXT": "text",
    "BLOB": "blob",
    "DATE": "date",
    "DATETIME": "datetime",
    # GEOMETRY types handled differently
}


_V2_TYPE_TO_GPKG_TYPE = {
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
    "datetime": "DATETIME",
    # geometry types handled differently
}


def gpkg_type_to_v2_type(gkpg_type):
    """Convert a gpkg type to v2 schema type."""
    m = re.match(r"^(TEXT|BLOB)\(([0-9]+)\)$", gkpg_type)
    if m:
        return m.group(1).lower(), {"length": int(m.group(2))}
    v2_type_info = _GPKG_TYPE_TO_V2_TYPE.get(gkpg_type)
    if v2_type_info is None:
        raise ValueError(f"Unrecognised GPKG type: {gkpg_type}")
    elif isinstance(v2_type_info, tuple):
        v2_type, extra_type_info = v2_type_info
    else:
        v2_type, extra_type_info = v2_type_info, {}
    return v2_type, extra_type_info


def _gkpg_geometry_columns_to_v2_type(ggc, gsrs):
    geometry_type = ggc["geometry_type_name"]
    z = "Z" if ggc["z"] else ""
    m = "M" if ggc["m"] else ""

    srs_str = None
    if gsrs and gsrs[0]["definition"]:
        srs_str = wkt_to_srs_str(gsrs[0]["definition"])

    extra_type_info = {
        "geometryType": f"{geometry_type} {z}{m}".strip(),
        "geometrySRS": srs_str,
    }
    return "geometry", extra_type_info


def v2_type_to_gpkg_type(column_schema, is_spatial):
    """Convert a v2 schema type to a gpkg type."""
    if is_spatial and column_schema.pk_index is not None:
        if column_schema.data_type == "integer":
            return "INTEGER"  # Must be INTEGER, not MEDIUMINT etc.
        else:
            raise NotYetImplemented(
                "GPKG features only support integer primary keys"
                f" - converting from {column_schema.data_type} not yet supported"
            )

    v2_type = column_schema.data_type
    extra_type_info = column_schema.extra_type_info
    if column_schema.data_type == "geometry":
        return extra_type_info["geometryType"].split(" ", 1)[0]

    gpkg_type_info = _V2_TYPE_TO_GPKG_TYPE.get(v2_type)
    if gpkg_type_info is None:
        raise ValueError(f"Unrecognised data type: {v2_type}")

    if isinstance(gpkg_type_info, dict):
        return gpkg_type_info.get(extra_type_info.get("size", 0))

    gpkg_type = gpkg_type_info
    length = extra_type_info.get("length", None)
    return f"{gpkg_type}({length})" if length else gpkg_type
