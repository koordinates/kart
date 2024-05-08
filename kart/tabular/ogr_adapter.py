from osgeo import ogr

from kart.exceptions import NotYetImplemented

# General purpose code for converting between OGR types and Kart types.

KART_TYPE_TO_OGR_TYPE = {
    "boolean": (ogr.OFTInteger, ogr.OFSTBoolean),
    "blob": ogr.OFTBinary,
    "date": ogr.OFTDate,
    "float": ogr.OFTReal,
    ("float", 32): (ogr.OFTReal, ogr.OFSTFloat32),
    ("float", 64): ogr.OFTReal,
    "integer": ogr.OFTInteger64,
    ("integer", 8): (ogr.OFTInteger, ogr.OFSTInt16),
    ("integer", 16): (ogr.OFTInteger, ogr.OFSTInt16),
    ("integer", 32): ogr.OFTInteger,
    ("integer", 64): ogr.OFTInteger64,
    "interval": ogr.OFTString,
    "numeric": ogr.OFTReal,
    "text": ogr.OFTString,
    "time": ogr.OFTTime,
    "timestamp": ogr.OFTDateTime,
}

# NOTE: We don't support *List fields (eg IntegerList).

OGR_TYPE_TO_KART_TYPE = {
    (ogr.OFTInteger, ogr.OFSTBoolean): "boolean",
    ogr.OFTBinary: "blob",
    ogr.OFTDate: "date",
    (ogr.OFTReal, ogr.OFSTFloat32): ("float", 32),
    ogr.OFTReal: ("float", 64),
    (ogr.OFTInteger, ogr.OFSTInt16): ("integer", 16),
    ogr.OFTInteger: ("integer", 32),
    ogr.OFTInteger64: ("integer", 64),
    ogr.OFTString: "text",
    ogr.OFTTime: "time",
    ogr.OFTDateTime: "timestamp",
}


def _build_geometry_dicts():
    ogr_type_names = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]
    kart_to_ogr = {}
    ogr_to_kart = {}

    def link_together(kart_type, ogr_type):
        kart_to_ogr[kart_type] = ogr_type
        ogr_to_kart[ogr_type] = kart_type

    for type_name in ogr_type_names:
        kart_type = type_name.upper()
        base_ogr_type = getattr(ogr, f"wkb{type_name}")
        link_together(kart_type, base_ogr_type)
        link_together(f"{kart_type} Z", base_ogr_type + 1000)
        link_together(f"{kart_type} M", base_ogr_type + 2000)
        link_together(f"{kart_type} ZM", base_ogr_type + 3000)

    link_together("GEOMETRY", ogr.wkbUnknown)
    return kart_to_ogr, ogr_to_kart


(
    KART_GEOM_TYPE_TO_OGR_GEOM_TYPE,
    OGR_GEOM_TYPE_TO_KART_GEOM_TYPE,
) = _build_geometry_dicts()


def _lookup_subtyped_dict(subtyped_dict, in_type, in_subtype):
    """
    Looks up one of the two dicts above, finds the entry corresponding to (in_type, in_subtype)
    when present, and falls back to just in_type when it is not.
    Returns a tuple (out_type, out_subtype) - out_subtype may be None.
    """

    value = subtyped_dict.get((in_type, in_subtype))
    if value is not None:
        return value if isinstance(value, tuple) else (value, None)
    value = subtyped_dict.get(in_type)
    if value is not None:
        return value if isinstance(value, tuple) else (value, None)
    raise KeyError(f"No entry for {in_type}")


def kart_schema_col_to_ogr_field_definition(col):
    """
    Given a kart.schema.ColumnSchema, returns the ogr.FieldDefn that best corresponds to it.
    """
    ogr_type, ogr_subtype = _lookup_subtyped_dict(
        KART_TYPE_TO_OGR_TYPE, col.data_type, col.get("size")
    )

    result = ogr.FieldDefn(col.name, ogr_type)
    if ogr_subtype is not None:
        result.SetSubType(ogr_subtype)

    if col.data_type in ("text", "blob"):
        length = col.get("length")
        if length:
            result.SetWidth(length)

    if col.data_type == "numeric":
        precision = col.get("precision")
        scale = col.get("scale")
        # Rather confusingly, OGR's concepts of 'width' and 'precision'
        # correspond to 'precision' and 'scale' in most other systems, respectively:
        if precision:
            result.SetWidth(precision)
        if scale:
            result.SetPrecision(scale)

    return result


def ogr_field_definition_to_kart_type(fd):
    """
    Given an ogr.FieldDefn, returns the tuple (kart_type: str, extra_type_info: dict) that best
    corresponds to it. Never returns numeric since this is more complicated - callers can handle
    this case themselves instead of calling this function.
    """
    ogr_type = fd.GetType()
    ogr_subtype = fd.GetSubType() or None
    try:
        kart_type, kart_size = _lookup_subtyped_dict(
            OGR_TYPE_TO_KART_TYPE, ogr_type, ogr_subtype
        )
    except KeyError:
        raise NotYetImplemented(
            f"Unsupported column type for import: OGR type={fd.GetTypeName()}, OGR subtype={ogr_subtype}"
        )

    if kart_size:
        return kart_type, {"size": kart_size}

    if kart_type in ("text", "blob"):
        ogr_width = fd.GetWidth()
        if ogr_width:
            return kart_type, {"length": ogr_width}

    return kart_type, {}


def kart_geometry_type_to_ogr_geometry_type(kart_geom_type):
    """Given a Kart str like "MULTILINESTRING ZM" returns the equivalent OGR magic number."""
    return KART_GEOM_TYPE_TO_OGR_GEOM_TYPE[kart_geom_type]


def ogr_geometry_type_to_kart_geometry_type(ogr_geom_type):
    """Given an OGR magic number like ogr.wkbMultiPolygonZM, returns the equivalent Kart str."""
    return OGR_GEOM_TYPE_TO_KART_GEOM_TYPE[ogr_geom_type]
