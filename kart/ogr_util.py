from decimal import Decimal

from .geometry import ogr_to_gpkg_geom


def adapt_ogr_date(value):
    if value is None:
        return value
    # OGR uses this strange format: '2012/07/09'
    # We convert back to a normal ISO8601 format.
    return str(value).replace("/", "-")


def adapt_ogr_timestamp(value):
    if value is None:
        return value
    # OGR uses this strange format: '2012/07/09 09:01:52+00'
    # We convert back to a normal ISO8601 format.
    return str(value).replace("/", "-").replace(" ", "T").replace("+00", "Z")


def adapt_ogr_numeric(value):
    if value is None:
        return value
    return str(Decimal(value))


def adapt_ogr_geometry(value):
    if value is None:
        return value
    return ogr_to_gpkg_geom(value)


def ensure_bool(value):
    if isinstance(value, int) and value in (0, 1):
        return bool(value)

    if value is not None and not isinstance(value, bool):
        raise ValueError(f"Expected boolean but found {value!r}")
    return value


def ensure_bytes(value):
    if value is not None and not isinstance(value, bytes):
        raise ValueError(f"Expected bytes but found {value!r}")
    return value


def ensure_int(value):
    return int(value) if value is not None else None


def ensure_float(value):
    return float(value) if value is not None else None


def ensure_str(value):
    if value is not None and not isinstance(value, str):
        raise ValueError(f"Expected str but found {value!r}")
    return value


def adapt_to_str(value):
    # These types aren't supported by OGR
    return str(value) if value is not None else None


OGR_TYPE_ADAPTERS = {
    "boolean": ensure_bool,
    "blob": ensure_bytes,
    "date": adapt_ogr_date,
    "float": ensure_float,
    "geometry": adapt_ogr_geometry,
    "integer": ensure_int,
    "interval": adapt_to_str,
    "numeric": adapt_ogr_numeric,
    "text": ensure_str,
    "time": adapt_to_str,
    "timestamp": adapt_ogr_timestamp,
}


def get_type_value_adapter(v2_type):
    """
    Returns a function which will convert values to the given V2 type
    from the equivalent OGR type.

    For most types this should be a no-op, but we try to be defensive and ensure that
    (for instance) floats stay floats and ints stay ints.
    """
    return OGR_TYPE_ADAPTERS[v2_type]
