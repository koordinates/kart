from memory_repo import MemoryTree, MemoryRepo

from kart.tabular.v3 import TableV3
from kart.schema import Legend, ColumnSchema, Schema


DATASET_PATH = "path/to/dataset"


def test_legend_roundtrip():
    orig = Legend(["a", "b", "c"], ["d", "e", "f"])

    roundtripped = Legend.loads(orig.dumps())

    assert roundtripped is not orig
    assert roundtripped == orig

    empty_dataset = TableV3.new_dataset_for_writing(DATASET_PATH, None, MemoryRepo())
    path, data = empty_dataset.encode_legend(orig)
    tree = MemoryTree({path: data})

    tableV3 = TableV3(tree / DATASET_PATH, DATASET_PATH, MemoryRepo())
    roundtripped = tableV3.get_legend(orig.hexhash())

    assert roundtripped is not orig
    assert roundtripped == orig


def test_raw_dict_to_value_tuples():
    legend = Legend(["a", "b", "c"], ["d", "e", "f"])
    raw_feature_dict = {
        "e": "eggs",
        "a": 123,
        "f": None,
        "d": 5.0,
        "c": True,
        "b": b"bytes",
    }
    pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
    assert pk_values == (123, b"bytes", True)
    assert non_pk_values == (5.0, "eggs", None)
    roundtripped = legend.value_tuples_to_raw_dict(pk_values, non_pk_values)
    assert roundtripped is not raw_feature_dict
    assert roundtripped == raw_feature_dict


def abcdef_schema():
    return Schema(
        [
            {
                "id": "a",
                "name": "a",
                "dataType": "integer",
                "primaryKeyIndex": 0,
                "size": 64,
            },
            {
                "id": "b",
                "name": "b",
                "dataType": "geometry",
            },
            {
                "id": "c",
                "name": "c",
                "dataType": "boolean",
            },
            {
                "id": "d",
                "name": "d",
                "dataType": "float",
            },
            {
                "id": "e",
                "name": "e",
                "dataType": "text",
            },
            {
                "id": "f",
                "name": "f",
                "dataType": "text",
            },
        ]
    )


def test_raw_feature_roundtrip():
    legend = Legend(["a"], ["b", "c", "d", "e", "f"])
    schema = abcdef_schema()
    empty_dataset = TableV3.new_dataset_for_writing(DATASET_PATH, schema, MemoryRepo())
    legend_path, legend_data = empty_dataset.encode_legend(legend)

    raw_feature_dict = {
        "e": "eggs",
        "a": 123,
        "f": None,
        "d": 5.0,
        "c": True,
        "b": b"bytes",
    }
    feature_path, feature_data = empty_dataset.encode_raw_feature_dict(
        raw_feature_dict, legend, schema=schema
    )
    tree = MemoryTree({legend_path: legend_data, feature_path: feature_data})

    tableV3 = TableV3(tree / DATASET_PATH, DATASET_PATH, MemoryRepo())
    roundtripped = tableV3.get_raw_feature_dict(path=feature_path)
    assert roundtripped is not raw_feature_dict
    assert roundtripped == raw_feature_dict

    empty_feature_dict = {
        "a": 123,
        "b": None,
        "c": None,
        "d": None,
        "e": None,
        "f": None,
    }
    _, empty_feature_data = empty_dataset.encode_raw_feature_dict(
        empty_feature_dict,
        legend,
        schema=schema,
    )
    tree = MemoryTree({legend_path: legend_data, feature_path: empty_feature_data})

    tableV3 = TableV3(tree / DATASET_PATH, DATASET_PATH, MemoryRepo())
    roundtripped = tableV3.get_raw_feature_dict(path=feature_path)
    # Overwriting the old feature with an empty feature at the same path only
    # clears the non-pk values, since the pk values are part of the path.
    assert roundtripped == {
        "a": 123,
        "b": None,
        "c": None,
        "d": None,
        "e": None,
        "f": None,
    }


GEOM_TYPE_INFO = {"geometryType": "MULTIPOLYGON ZM", "geometryCRS": "EPSG:2193"}


def test_schema_roundtrip(gen_uuid):
    orig = Schema(
        [
            ColumnSchema(
                id=gen_uuid(), name="geom", data_type="geometry", **GEOM_TYPE_INFO
            ),
            ColumnSchema(
                id=gen_uuid(), name="id", data_type="integer", pk_index=1, size=64
            ),
            ColumnSchema(
                id=gen_uuid(), name="artist", data_type="text", pk_index=0, length=200
            ),
            ColumnSchema(id=gen_uuid(), name="recording", data_type="blob"),
        ]
    )

    roundtripped = Schema.loads(orig.dumps())

    assert roundtripped is not orig
    assert roundtripped == orig

    empty_dataset = TableV3.new_dataset_for_writing(DATASET_PATH, None, MemoryRepo())
    path, data = empty_dataset.encode_schema(orig)
    tree = MemoryTree({path: data})

    tableV3 = TableV3(tree / DATASET_PATH, DATASET_PATH, MemoryRepo())
    roundtripped = tableV3.schema

    assert roundtripped is not orig
    assert roundtripped == orig


def test_feature_roundtrip(gen_uuid):
    schema = Schema(
        [
            ColumnSchema(
                id=gen_uuid(), name="geom", data_type="geometry", **GEOM_TYPE_INFO
            ),
            ColumnSchema(
                id=gen_uuid(), name="id", data_type="integer", pk_index=1, size=64
            ),
            ColumnSchema(
                id=gen_uuid(), name="artist", data_type="text", pk_index=0, length=200
            ),
            ColumnSchema(id=gen_uuid(), name="recording", data_type="blob"),
        ]
    )
    empty_dataset = TableV3.new_dataset_for_writing(DATASET_PATH, schema, MemoryRepo())
    schema_path, schema_data = empty_dataset.encode_schema(schema)
    legend_path, legend_data = empty_dataset.encode_legend(schema.legend)

    # encode_feature also accepts a feature tuple, but mostly we use dicts everywhere.
    feature_tuple = ("010100000087BF756489EF5C4C", 7, "GIS Choir", b"MP3")
    # When encoding dicts, we use the keys - so the correct initialisation order is not necessary.
    feature_dict = {
        "artist": "GIS Choir",
        "recording": b"MP3",
        "id": 7,
        "geom": "010100000087BF756489EF5C4C",
    }

    feature_path, feature_data = empty_dataset.encode_feature(feature_tuple, schema)
    feature_path2, feature_data2 = empty_dataset.encode_feature(feature_dict, schema)
    # Either encode method should give the same result.
    assert (feature_path, feature_data) == (feature_path2, feature_data2)

    tree = MemoryTree(
        {schema_path: schema_data, legend_path: legend_data, feature_path: feature_data}
    )

    tableV3 = TableV3(tree / DATASET_PATH, DATASET_PATH, MemoryRepo())
    roundtripped_feature = tableV3.get_feature(path=feature_path)
    assert roundtripped_feature is not feature_dict
    assert roundtripped_feature == feature_dict
    # We guarantee that the dict iterates in row-order.
    assert tuple(roundtripped_feature.values()) == feature_tuple


def test_schema_change_roundtrip(gen_uuid):
    old_schema = Schema(
        [
            ColumnSchema(id=gen_uuid(), name="ID", data_type="integer", pk_index=0),
            ColumnSchema(id=gen_uuid(), name="given_name", data_type="text"),
            ColumnSchema(id=gen_uuid(), name="surname", data_type="text"),
            ColumnSchema(id=gen_uuid(), name="date_of_birth", data_type="date"),
        ]
    )
    new_schema = Schema(
        [
            ColumnSchema(
                id=old_schema[0].id,
                name="personnel_id",
                data_type="integer",
                pk_index=0,
            ),
            ColumnSchema(id=gen_uuid(), name="tax_file_number", data_type="text"),
            ColumnSchema(id=old_schema[2].id, name="last_name", data_type="text"),
            ColumnSchema(id=old_schema[1].id, name="first_name", data_type="text"),
            ColumnSchema(id=gen_uuid(), name="middle_names", data_type="text"),
        ]
    )
    # Updating the schema without updating features is only possible
    # if the old and new schemas have the same primary key columns:
    assert old_schema.is_pk_compatible(new_schema)

    feature_tuple = (7, "Joe", "Bloggs", "1970-01-01")
    feature_dict = {
        "given_name": "Joe",
        "surname": "Bloggs",
        "date_of_birth": "1970-01-01",
        "ID": 7,
    }

    empty_dataset = TableV3.new_dataset_for_writing(
        DATASET_PATH, old_schema, MemoryRepo()
    )
    feature_path, feature_data = empty_dataset.encode_feature(feature_tuple, old_schema)
    feature_path2, feature_data2 = empty_dataset.encode_feature(
        feature_dict, old_schema
    )
    # Either encode method should give the same result.
    assert (feature_path, feature_data) == (feature_path2, feature_data2)

    # The dataset should store only the current schema, but all legends.
    schema_path, schema_data = empty_dataset.encode_schema(new_schema)
    new_legend_path, new_legend_data = empty_dataset.encode_legend(new_schema.legend)
    old_legend_path, old_legend_data = empty_dataset.encode_legend(old_schema.legend)
    tree = MemoryTree(
        {
            schema_path: schema_data,
            new_legend_path: new_legend_data,
            old_legend_path: old_legend_data,
            feature_path: feature_data,
        }
    )

    tableV3 = TableV3(tree / DATASET_PATH, DATASET_PATH, MemoryRepo())
    # Old columns that are not present in the new schema are gone.
    # New columns that are not present in the old schema have 'None's.
    roundtripped = tableV3.get_feature(path=feature_path)
    assert roundtripped == {
        "personnel_id": 7,
        "tax_file_number": None,
        "last_name": "Bloggs",
        "first_name": "Joe",
        "middle_names": None,
    }
    # We guarantee that the dict iterates in row-order.
    assert tuple(roundtripped.values()) == (7, None, "Bloggs", "Joe", None)
