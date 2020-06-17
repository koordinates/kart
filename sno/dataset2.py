import base64
from collections import namedtuple
import functools
import hashlib
import json
import os
import uuid

import msgpack

from .structure import DatasetStructure, IntegrityError


def _pack(data):
    """data (any type) -> bytes"""
    return msgpack.packb(data, use_bin_type=True)


def _unpack(bytestring):
    """bytes -> data (any type)"""
    return msgpack.unpackb(bytestring, raw=False)


# _json and _unjson are functionally identical to _pack and _unpack,
# but their storage format is less compact and more human-readable.
def _json(data):
    """data (any type) -> bytes"""
    return json.dumps(data).encode("utf8")


def _unjson(bytestring):
    """bytes -> data (any type)"""
    return json.loads(bytestring, encoding="utf8")


def _b64encode_str(bytestring):
    """bytes -> urlsafe str"""
    return base64.urlsafe_b64encode(bytestring).decode("ascii")


def _b64decode_str(b64_str):
    """urlsafe str -> bytes"""
    return base64.urlsafe_b64decode(b64_str)


def _hexhash(data):
    """data (any type) -> hex str. Irreversible."""
    # We only return 160 bits of the hash, same as git hashes - more is overkill.
    return hashlib.sha256(data).hexdigest()[:40]


def walk_tree(tree, path="", max_depth=4):
    """
    Recursively yields all possible (file_path, file_data) tuples
    in the given directory tree, up to a given max_depth.
    """
    for entry in tree:
        entry_path = f"{path}/{entry.name}" if path else entry.name
        if hasattr(entry, "data"):
            yield entry_path, entry.data
        elif max_depth > 0:
            yield from walk_tree(entry, entry_path, max_depth - 1)


class Legend:
    """
    A legend is like a table-header that describes how a row is read. Legends are immutable.
    A row is an array of values - eg ["Null Island", POINT(0, 0), "Imaginary points of interest"]
    To read this, we need to look at the legend, which is an array of UUIDs, like:
    ["1144c97-73a6-78d38ab68ed", "2f1551c-a094-c1a3678e317", "2feac23-08af-73e61dbced9"],
    The two arrays can be zipped together to make a dict of (column-id, value) pairs, ie:
    {
        "1144c97-73a6-78d38ab68ed": "Null Island",
        "2f1551c-a094-c1a3678e317": POINT(0, 0),
        "2feac23-08af-73e61dbced9": "Imaginary points of interest"
    }
    Then the schema can be consulted to find the current user-visible names and ordering of those columns.

    In practise, rows are split into two parts when written - the primary key columns are embedded in the file path,
    and the remaining columns are written to the file contents. For this reason, we receive rows as two arrays -
    primary-key values and non-primary-key values - and so a legend contains two arrays of UUIDs for making sense of
    both parts.
    """

    def __init__(self, pk_columns, non_pk_columns):
        """
        Create a new legend.
            pk_columns - a list of column IDs for primary key columns.
            non_pk_columns - a list of column IDs for non primary key columns.
        """
        self._pk_columns = tuple(pk_columns)
        self._non_pk_columns = tuple(non_pk_columns)

    @property
    def pk_columns(self):
        return self._pk_columns

    @property
    def non_pk_columns(self):
        return self._non_pk_columns

    @classmethod
    def loads(cls, data):
        """Load a legend from a bytestring"""
        pk_columns, non_pk_columns = _unpack(data)
        return cls(pk_columns, non_pk_columns)

    def dumps(self):
        """Writes this legend to a bytestring."""
        return _pack((self.pk_columns, self.non_pk_columns))

    def value_tuples_to_raw_dict(self, pk_values, non_pk_values):
        """
        Given all the values, zips them into a dict with their column IDs.
        This dict is called the "raw" dict since it uses internal column IDs, not the user-visible column names.
        Legends only deal with raw dicts - for user-visible properties, see Schema.
        """
        assert len(pk_values) == len(self.pk_columns)
        assert len(non_pk_values) == len(self.non_pk_columns)
        raw_feature_dict = {}
        for column, value in zip(self.pk_columns, pk_values):
            raw_feature_dict[column] = value
        for column, value in zip(self.non_pk_columns, non_pk_values):
            raw_feature_dict[column] = value
        return raw_feature_dict

    def raw_dict_to_value_tuples(self, raw_feature_dict):
        """Inverse of value_tuples_to_raw_dict."""
        pk_values = tuple(raw_feature_dict[column] for column in self.pk_columns)
        non_pk_values = tuple(
            raw_feature_dict[column] for column in self.non_pk_columns
        )
        return pk_values, non_pk_values

    def __eq__(self, other):
        if not isinstance(other, Legend):
            return False
        return (
            self.pk_columns == other.pk_columns
            and self.non_pk_columns == other.non_pk_columns
        )

    def __hash__(self):
        return hash((self.pk_columns, self.non_pk_columns))

    def hexhash(self):
        """Like __hash__ but with platform-independent, 160-bit hex strings."""
        return _hexhash(self.dumps())


def _pk_index_ordering(column):
    """Returns primary key columns first, in pk_index order, then other columns."""
    if column.pk_index is not None:
        return column.pk_index
    else:
        # Non primary-key columns after primary key columns.
        return float('inf')


ALL_DATA_TYPES = {
    "BOOLEAN",
    "BLOB",
    "DATE",
    "DATETIME",
    "FLOAT",
    "GEOMETRY",
    "INTEGER",
    "INTERVAL",
    "NUMERIC",
    "TEXT",
    "TIME",
    "TIMESTAMP",
}


class ColumnSchema(
    # namedtuple for Immutability
    namedtuple(
        "ColumnSchema", ("id", "name", "data_type", "pk_index", "extra_type_info")
    )
):
    """
    The schema for a single column. A column has a unique ID that is constant for the columns lifetime
    - even if the column is moved or renamed - and it has a name, a datatype, info about if this
    column is one of the primary keys, and potentially extra info about the specific datatype.
    """

    @staticmethod
    def new_id():
        return str(uuid.uuid4())

    def __new__(cls, id, name, data_type, pk_index, **extra_type_info):
        assert data_type in ALL_DATA_TYPES
        return super().__new__(cls, id, name, data_type, pk_index, extra_type_info)

    @classmethod
    def from_json_dict(cls, json_dict):
        return cls(
            json_dict.pop("id"),
            json_dict.pop("name"),
            json_dict.pop("dataType"),
            json_dict.pop("primaryKeyIndex"),
            **json_dict,
        )

    def to_json_dict(self):
        return {
            "id": self.id,
            "name": self.name,
            "dataType": self.data_type,
            "primaryKeyIndex": self.pk_index,
            **self.extra_type_info,
        }

    def __eq__(self, other):
        if not isinstance(other, ColumnSchema):
            return False
        return (
            self.id == other.id
            and self.name == other.name
            and self.data_type == other.data_type
            and self.pk_index == other.pk_index
            and self.extra_type_info == other.extra_type_info
        )


class Schema:
    """A schema is just an immutable ordered list of ColumnSchemas."""

    def __init__(self, columns):
        """Create a new schema from a list of ColumnSchemas."""
        self._columns = tuple(columns)
        # Creating a legend validates the primaryKeyIndex field.
        self._legend = self._to_legend()

    @property
    def columns(self):
        return self._columns

    @property
    def legend(self):
        return self._legend

    def __getitem__(self, i):
        """Return the _i_th ColumnSchema."""
        return self._columns[i]

    @classmethod
    def loads(cls, data):
        """Load a schema from a bytestring"""
        json_array = _unjson(data)
        columns = [ColumnSchema.from_json_dict(c) for c in json_array]
        return cls(columns)

    def dumps(self):
        """Writes this schema to a bytestring."""
        json_array = [c.to_json_dict() for c in self.columns]
        return _json(json_array)

    def __str__(self):
        cols = ",\n".join(str(c) for c in self.columns)
        return f"Schema([{cols}])"

    def __repr__(self):
        cols = ",\n".join(repr(c) for c in self.columns)
        return f"Schema([{cols}])"

    def feature_from_raw_dict(self, raw_dict, keys=True):
        """
        Takes a "raw" feature dict - values keyed by column ID.
        Returns a dict of values keyed by column name (if keys=True)
        or a tuple of value in schema order (if keys=False).
        """
        if keys:
            return {c.name: raw_dict.get(c.id, None) for c in self.columns}
        else:
            return tuple([raw_dict.get(c.id, None) for c in self.columns])

    def feature_to_raw_dict(self, feature):
        """
        Takes a feature - either a dict of values keyed by column name,
        or a list / tuple of values in schema order.
        Returns a "raw" feature dict - values keyed by column ID.
        """
        raw_dict = {}
        if isinstance(feature, dict) or hasattr(feature, "keys"):
            # Feature values are keyed by column name - find them by key.
            # This also works for DB rows that are subscriptable.
            for column in self.columns:
                raw_dict[column.id] = feature[column.name]
        else:
            # Feature values are not keyed, but should be in order, one per column.
            assert len(feature) == len(self.columns)
            for column, value in zip(self.columns, feature):
                raw_dict[column.id] = value
        return raw_dict

    def _to_legend(self):
        pk_column_ids = []
        non_pk_column_ids = []
        for i, column in enumerate(sorted(self.columns, key=_pk_index_ordering)):
            if column.pk_index is not None:
                if i != column.pk_index:
                    raise ValueError(
                        f"Expected contiguous primaryKeyIndex {i} but only found {column.pk_index}"
                    )
                pk_column_ids.append(column.id)
            else:
                non_pk_column_ids.append(column.id)
        return Legend(pk_column_ids, non_pk_column_ids)

    def __eq__(self, other):
        if not isinstance(other, Schema):
            return False
        return self.columns == other.columns

    def is_pk_compatible(self, other):
        """
        Does a schema change from self -> other mean that every feature needs a new path?
        Only if the primary key columns have changed.
        """
        return self.legend.pk_columns == other.legend.pk_columns


# TODO: this class is unfinished, and so doesn't extend DatasetStructure.
class Dataset2(DatasetStructure):
    """
    - Uses messagePack to serialise features.
    - Stores each feature in a blob with path dependent on primary key values.
    - Add at any location: `sno import GPKG:my.gpkg:mytable path/to/mylayer`

    any/structure/mylayer/
      .sno-table/
        meta/
          version               = 2.0
          schema                = [current schema JSON]
          legend/
            [legend-a-hash]   = [column-id0, column-id1, ...]
            [legend-b-hash]   = [column-id0, column-id1, ...]
            ...

        [hex(pk-hash):2]/
          [hex(pk-hash):2]/
            [base64(pk-value)]  = [msgpack([legend-x-hash, value0, value1, ...])]

    Dataset2 is initialised pointing at a particular directory tree, and uses that
    to read features and schemas. However, it never writes to the tree, since this
    is not straight-forward in git/sno and involves batching writes into a commit.
    Therefore, there are no methods which write, only methods which return things
    which *should be written*. The caller must write these to a commit.
    """

    VERSION_PATH = ".sno-table/meta/version"
    VERSION_SPECIFIER = "2."
    VERSION_IMPORT = "2.0"

    ENCODE_VERSION = (VERSION_PATH, VERSION_IMPORT.encode('utf8'))

    FEATURE_PATH = ".sno-table/feature/"
    META_PATH = ".sno-table/meta/"
    LEGEND_PATH = ".sno-table/meta/legend/"
    SCHEMA_PATH = ".sno-table/meta/schema"

    @property
    def version(self):
        return self.get_data_at(self.VERSION_PATH, as_str=True)

    def get_data_at(self, path, as_str=False):
        """Return the data at the given path from within this dataset."""
        leaf = self.tree / str(path)
        if not hasattr(leaf, "data"):
            raise IntegrityError(f"No data found at path {path}, type={type(leaf)}")
        return leaf.data.decode('utf8') if as_str else leaf.data

    @functools.lru_cache()
    def get_meta_item(self, path):
        # TODO: this is hardly used - could maybe be removed.
        content_is_str = not path.startswith(self.LEGEND_PATH)
        return self.get_data_at(self.META_PATH + path, as_str=content_is_str)

    @functools.lru_cache()
    def get_legend(self, legend_hash=None, *, path=None):
        """Load the legend with the given hash / at the given path from this dataset."""
        if path is None:
            path = self.LEGEND_PATH + legend_hash
        return Legend.loads(self.get_data_at(path))

    @classmethod
    def encode_legend(cls, legend):
        """
        Given a legend, returns the path and the data which *should be written*
        to write this legend. This is almost the inverse of get_legend, except
        Dataset2 doesn't write the data.
        """
        return cls.LEGEND_PATH + legend.hexhash(), legend.dumps()

    @property
    @functools.lru_cache(maxsize=1)
    def schema(self):
        """Load the current schema from this dataset."""
        return Schema.loads(self.get_data_at(self.SCHEMA_PATH))

    @classmethod
    def encode_schema(cls, schema):
        """
        Given a schema, returns the path and the data which *should be written*
        to write this schema. This is almost the inverse of calling .schema,
        except Dataset2 doesn't write the data. (Note that the schema's legend
        should also be stored if any features are written with this schema.)
        """
        return cls.SCHEMA_PATH, schema.dumps()

    def get_raw_feature_dict(self, pk_values=None, *, path=None, data=None):
        """
        Gets the feature with the given primary key(s) / at the given path.
        The result is a "raw" feature dict, values are keyed by column ID,
        and contains exactly those values that are actually stored in the tree,
        which might not be the same values that are now in the schema.
        To get a feature consistent with the current schema, call get_feature.
        """

        # Either pk_values or path should be supplied, but not both.
        if pk_values is None and path is not None:
            pk_values = self.decode_path_to_pk_values(path)
        elif path is None and pk_values is not None:
            pk_values = self.sanitise_pk_values(pk_values)
            path = self.encode_pk_values_to_path(pk_values)
        else:
            raise ValueError("Exactly one of (pk_values, path) must be supplied")

        # Optionally, data can be supplied if the caller already knows it.
        # This is just the data stored at path.
        if data is None:
            data = self.get_data_at(path)

        legend_hash, non_pk_values = _unpack(data)
        legend = self.get_legend(legend_hash)
        return legend.value_tuples_to_raw_dict(pk_values, non_pk_values)

    def get_feature(self, pk_values=None, *, path=None, data=None, keys=True):
        """
        Gets the feature with the given primary key(s) / at the given path.
        The result is either a dict of values keyed by column name (if keys=True)
        or a tuple of values in schema order (if keys=False).
        """
        raw_dict = self.get_raw_feature_dict(pk_values=pk_values, path=path, data=data)
        return self.schema.feature_from_raw_dict(raw_dict, keys=keys)

    def features(self, keys=True):
        """
        Returns a generator that calls get_feature once per feature.
        Each entry in the generator is the path of the feature and then the feature itself.
        """

        # TODO: optimise.
        # TODO: don't return the path of each feature by default - most callers don't care.
        # (but this is the interface shared by dataset1 at the moment.)
        if self.FEATURE_PATH not in self.tree:
            return
        for feature_path, feature_data in walk_tree(self.tree / self.FEATURE_PATH):
            yield feature_path, self.get_feature(
                path=feature_path, data=feature_data, keys=keys
            ),

    @classmethod
    def decode_path_to_pk_values(cls, path):
        """Given a feature path, returns the pk values encoded in it."""
        encoded = os.path.basename(path)
        return _unpack(_b64decode_str(encoded))

    @classmethod
    def encode_raw_feature_dict(cls, raw_feature_dict, legend):
        """
        Given a "raw" feature dict (keyed by column IDs) and a schema, returns the path
        and the data which *should be written* to write this feature. This is almost the
        inverse of get_raw_feature_dict, except Dataset2 doesn't write the data.
        """
        pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
        path = cls.encode_pk_values_to_path(pk_values)
        data = _pack([legend.hexhash(), non_pk_values])
        return path, data

    @classmethod
    def encode_feature(cls, feature, schema):
        """
        Given a feature (either a dict keyed by column name, or a list / tuple in schema order),
        returns the path and the data which *should be written* to write this feature. This is
        almost the inverse of get_feature, except Dataset2 doesn't write the data.
        """
        raw_dict = schema.feature_to_raw_dict(feature)
        return cls.encode_raw_feature_dict(raw_dict, schema.legend)

    @classmethod
    def encode_pk_values_to_path(cls, pk_values):
        """
        Given some pk values, returns the path the feature should be written to.
        pk_values should be a single pk value, or a list of pk values.
        """
        pk_values = cls.sanitise_pk_values(pk_values)
        packed_pk = _pack(pk_values)
        pk_hash = _hexhash(packed_pk)
        filename = _b64encode_str(packed_pk)
        return f"{cls.FEATURE_PATH}{pk_hash[:2]}/{pk_hash[2:4]}/{filename}"

    @classmethod
    def sanitise_pk_values(cls, pk_values):
        # This means get_feature(123) is treated same as get_feature([123]) -
        # pk_values needs to be sanitized to a list or tuple of values, one for
        # each pk column - even if there is only one pk column.
        if not isinstance(pk_values, (list, tuple)):
            return (pk_values,)
        return pk_values

    def repo_path(self, rel_path):
        return f"{self.path}/{rel_path}"

    def import_iter_meta_blobs(self, repo, source):
        schema = source.get_v2_schema()
        meta_blobs = [
            self.ENCODE_VERSION,
            self.encode_schema(schema),
            self.encode_legend(schema.legend),
        ]

        for meta_path, meta_content in meta_blobs:
            yield self.repo_path(meta_path), meta_content

    def import_iter_feature_blobs(self, resultset, source):
        schema = source.get_v2_schema()
        for feature in resultset:
            feature_path, feature_content = self.encode_feature(feature, schema)
            yield self.repo_path(feature_path), feature_content
