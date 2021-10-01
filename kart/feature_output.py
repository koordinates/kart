import json


from .exceptions import InvalidOperation
from .geometry import Geometry, GeometryType, ogr_to_hex_wkb
from .utils import ungenerator


def feature_as_text(row, prefix=""):
    result = []
    for key in row.keys():
        if key.startswith("__"):
            continue
        result.append(feature_field_as_text(row, key, prefix))
    return "\n".join(result)


def feature_field_as_text(row, key, prefix):
    val = row[key]

    if isinstance(val, Geometry):
        geom_typ = val.geometry_type_name
        if val.is_empty():
            val = f"{geom_typ} EMPTY"
        else:
            val = f"{geom_typ}(...)"
    elif isinstance(val, bytes):
        val = "BLOB(...)"

    val = "␀" if val is None else val
    return f"{prefix}{key:>40} = {val}"


@ungenerator(dict)
def feature_as_json(row, pk_value, geometry_transform=None):
    """
    Turns a row into a dict for serialization as JSON.
    The geometry is serialized as hexWKB.
    """
    for k, v in row.items():
        if isinstance(v, Geometry):
            if geometry_transform is None:
                v = v.to_hex_wkb()
            else:
                # reproject
                ogr_geom = v.to_ogr()
                try:
                    ogr_geom.Transform(geometry_transform)
                except RuntimeError as e:
                    raise InvalidOperation(
                        f"Can't reproject geometry with ID '{pk_value}' into target CRS"
                    ) from e
                v = ogr_to_hex_wkb(ogr_geom)
        elif isinstance(v, bytes):
            v = bytes.hex(v)
        yield k, v


def feature_as_geojson(row, pk_value, change=None, geometry_transform=None):
    """
    Turns a row into a dict representing a GeoJSON feature.
    """
    change_id = f"{change}::{pk_value}" if change else str(pk_value)
    f = {
        "type": "Feature",
        "geometry": None,
        "properties": {},
        "id": change_id,
    }

    for k in row.keys():
        v = row[k]
        if isinstance(v, Geometry):
            g = v.to_ogr()
            if geometry_transform is not None:
                # reproject
                try:
                    g.Transform(geometry_transform)
                except RuntimeError as e:
                    raise InvalidOperation(
                        f"Can't reproject geometry at '{change_id}' into target CRS"
                    ) from e
            json_str = g.ExportToJson()
            f["geometry"] = json.loads(json_str) if json_str else None
        elif isinstance(v, bytes):
            f["properties"][k] = bytes.hex(v)
        else:
            f["properties"][k] = v

    return f
