from kart.tabular.ogr_adapter import (
    kart_schema_col_to_ogr_field_definition,
    ogr_field_definition_to_kart_type,
    kart_geometry_type_to_ogr_geometry_type,
    ogr_geometry_type_to_kart_geometry_type,
)
from kart.schema import ColumnSchema


def test_ogr_type_roundtrip():
    # These types should all roundtrip cleanly.
    for kart_type in [
        "boolean",
        "blob",
        "date",
        "float",
        "integer",
        "text",
        "time",
        "timestamp",
    ]:
        sizes = [None]
        if kart_type == "integer":
            sizes = [16, 32, 64]
        elif kart_type == "float":
            sizes = [32, 64]
        for size in sizes:
            extra_type_info = {"size": size} if size else {}
            kart_col = ColumnSchema(data_type=kart_type, **extra_type_info)
            ogr_fd = kart_schema_col_to_ogr_field_definition(kart_col)
            (
                rt_kart_type,
                rt_extra_type_info,
            ) = ogr_field_definition_to_kart_type(ogr_fd)
            assert rt_kart_type == kart_type
            assert rt_extra_type_info == extra_type_info


def test_ogr_geometry_type_roundtrip():
    for base_type in [
        "POINT",
        "LINESTRING",
        "POLYGON",
        "MULTIPOINT",
        "MULTILINESTRING",
        "MULTIPOLYGON",
        "GEOMETRYCOLLECTION",
        "GEOMETRY",
    ]:
        suffixes = ["", " Z", " M", " ZM"] if base_type != "GEOMETRY" else [""]
        for suffix in suffixes:
            kart_type = base_type + suffix
            ogr_type = kart_geometry_type_to_ogr_geometry_type(kart_type)
            roundtripped_type = ogr_geometry_type_to_kart_geometry_type(ogr_type)
            assert roundtripped_type == kart_type
