import os
import pytest

from sno.dataset2 import Dataset2, Legend, ColumnSchema, Schema


class DictTree:
    """
    A fake directory tree structure.
    Contains a dict of all file data, {path: data}, and the current path.
    Supports only two operators:
    self / "some/path" returns a fake subdirectory / descendant file.
    self.data returns data of the current file, if there is a file at the current path.
    More complex directory navigation is not supported.
    """

    def __init__(self, all_data, cur_path=""):
        self.all_data = all_data
        self.cur_path = cur_path

        if all_data.get(cur_path) is not None:
            self.data = all_data[cur_path]

    def __truediv__(self, path):
        return DictTree(self.all_data, os.path.join(self.cur_path, path))


def test_legend_roundtrip():
    orig = Legend(["a", "b", "c"], ["d", "e", "f"])

    roundtripped = Legend.loads(orig.dumps())

    assert roundtripped is not orig
    assert roundtripped == orig

    path, data = Dataset2.encode_legend(orig)
    tree = DictTree({path: data})

    dataset2 = Dataset2(tree)
    roundtripped = dataset2.get_legend(orig.hexhash())

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


def test_raw_feature_roundtrip():
    legend = Legend(["a", "b", "c"], ["d", "e", "f"])
    legend_path, legend_data = Dataset2.encode_legend(legend)

    raw_feature_dict = {
        "e": "eggs",
        "a": 123,
        "f": None,
        "d": 5.0,
        "c": True,
        "b": b"bytes",
    }
    feature_path, feature_data = Dataset2.encode_raw_feature_dict(
        raw_feature_dict, legend
    )
    tree = DictTree({legend_path: legend_data, feature_path: feature_data})

    dataset2 = Dataset2(tree)
    roundtripped = dataset2.get_raw_feature_dict(path=feature_path)
    assert roundtripped is not raw_feature_dict
    assert roundtripped == raw_feature_dict

    empty_feature_dict = {
        "a": None,
        "b": None,
        "c": None,
        "d": None,
        "e": None,
        "f": None,
    }
    _, empty_feature_data = Dataset2.encode_raw_feature_dict(empty_feature_dict, legend)
    tree = DictTree({legend_path: legend_data, feature_path: empty_feature_data})

    dataset2 = Dataset2(tree)
    roundtripped = dataset2.get_raw_feature_dict(path=feature_path)
    # Overwriting the old feature with an empty feature at the same path only
    # clears the non-pk values, since the pk values are part of the path.
    assert roundtripped == {
        "a": 123,
        "b": b"bytes",
        "c": True,
        "d": None,
        "e": None,
        "f": None,
    }


@pytest.fixture
def pki(gen_uuid):
    def _pki(pk_index):
        # Returns an arbitrary ColumnSchema, but with the given pk_index property.
        id = gen_uuid()
        return ColumnSchema(id, id[:8], "integer", pk_index)

    return _pki


def test_valid_schemas(pki):
    Schema([pki(None), pki(None), pki(None)])
    Schema([pki(2), pki(1), pki(0)])
    Schema([pki(None), pki(1), pki(None), pki(0)])


def test_invalid_schemas(pki):
    with pytest.raises(ValueError):
        Schema([pki(0), pki(None), pki(2)])
    with pytest.raises(ValueError):
        Schema([pki(0), pki(1), pki(1)])
    with pytest.raises(ValueError):
        Schema([pki(-1), pki(None), pki(None)])


GEOM_TYPE_INFO = {"geometryType": "MULTIPOLYGON ZM", "geometrySRS": "EPSG:2193"}


def test_schema_roundtrip(gen_uuid):
    orig = Schema(
        [
            ColumnSchema(gen_uuid(), "geom", "geometry", None, **GEOM_TYPE_INFO),
            ColumnSchema(gen_uuid(), "id", "integer", 1, size=64),
            ColumnSchema(gen_uuid(), "artist", "text", 0, length=200),
            ColumnSchema(gen_uuid(), "recording", "blob", None),
        ]
    )

    roundtripped = Schema.loads(orig.dumps())

    assert roundtripped is not orig
    assert roundtripped == orig

    path, data = Dataset2.encode_current_schema(orig)
    tree = DictTree({path: data})

    dataset2 = Dataset2(tree)
    roundtripped = dataset2.get_current_schema()

    assert roundtripped is not orig
    assert roundtripped == orig


def test_feature_roundtrip(gen_uuid):
    schema = Schema(
        [
            ColumnSchema(gen_uuid(), "geom", "geometry", None, **GEOM_TYPE_INFO),
            ColumnSchema(gen_uuid(), "id", "integer", 1, size=64),
            ColumnSchema(gen_uuid(), "artist", "text", 0, length=200),
            ColumnSchema(gen_uuid(), "recording", "blob", None),
        ]
    )
    schema_path, schema_data = Dataset2.encode_current_schema(schema)
    legend_path, legend_data = Dataset2.encode_legend(schema.legend)

    # Feature tuples must be in schema order:
    feature_tuple = ("010100000087BF756489EF5C4C", 7, "GIS Choir", b"MP3")
    # But for feature dicts, the initialisation order is not important.
    feature_dict = {
        "artist": "GIS Choir",
        "recording": b"MP3",
        "id": 7,
        "geom": "010100000087BF756489EF5C4C",
    }

    feature_path, feature_data = Dataset2.encode_feature(feature_tuple, schema)
    feature_path2, feature_data2 = Dataset2.encode_feature(feature_dict, schema)
    # Either encode method should give the same result.
    assert (feature_path, feature_data) == (feature_path2, feature_data2)

    tree = DictTree(
        {schema_path: schema_data, legend_path: legend_data, feature_path: feature_data}
    )

    dataset2 = Dataset2(tree)
    roundtripped_tuple = dataset2.get_feature(path=feature_path, keys=False)
    assert roundtripped_tuple is not feature_tuple
    assert roundtripped_tuple == feature_tuple

    roundtripped_dict = dataset2.get_feature(path=feature_path, keys=True)
    assert roundtripped_dict is not feature_dict
    assert roundtripped_dict == feature_dict


def test_schema_change_roundtrip(gen_uuid):
    old_schema = Schema(
        [
            ColumnSchema(gen_uuid(), "ID", "integer", 0),
            ColumnSchema(gen_uuid(), "given_name", "text", None),
            ColumnSchema(gen_uuid(), "surname", "text", None),
            ColumnSchema(gen_uuid(), "date_of_birth", "date", None),
        ]
    )
    new_schema = Schema(
        [
            ColumnSchema(old_schema[0].id, "personnel_id", "integer", 0),
            ColumnSchema(gen_uuid(), "tax_file_number", "text", None),
            ColumnSchema(old_schema[2].id, "last_name", "text", None),
            ColumnSchema(old_schema[1].id, "first_name", "text", None),
            ColumnSchema(gen_uuid(), "middle_names", "text", None),
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

    feature_path, feature_data = Dataset2.encode_feature(feature_tuple, old_schema)
    feature_path2, feature_data2 = Dataset2.encode_feature(feature_dict, old_schema)
    # Either encode method should give the same result.
    assert (feature_path, feature_data) == (feature_path2, feature_data2)

    # The dataset should store only the current schema, but all legends.
    schema_path, schema_data = Dataset2.encode_current_schema(new_schema)
    new_legend_path, new_legend_data = Dataset2.encode_legend(new_schema.legend)
    old_legend_path, old_legend_data = Dataset2.encode_legend(old_schema.legend)
    tree = DictTree(
        {
            schema_path: schema_data,
            new_legend_path: new_legend_data,
            old_legend_path: old_legend_data,
            feature_path: feature_data,
        }
    )

    dataset2 = Dataset2(tree)
    # Old columns that are not present in the new schema are gone.
    # New columns that are not present in the old schema have 'None's.
    roundtripped = dataset2.get_feature(path=feature_path, keys=False)
    assert roundtripped == (7, None, "Bloggs", "Joe", None)
    roundtripped = dataset2.get_feature(path=feature_path, keys=True)
    assert roundtripped == {
        "personnel_id": 7,
        "tax_file_number": None,
        "last_name": "Bloggs",
        "first_name": "Joe",
        "middle_names": None,
    }
