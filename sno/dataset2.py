import base64
import functools
import hashlib
import os

import msgpack

from .structure import IntegrityError


def _pack(data):
    """data (any type) -> bytes"""
    return msgpack.packb(data)


def _unpack(bytestring):
    """bytes -> data (any type)"""
    return msgpack.unpackb(bytestring, raw=False)


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


def _pk_index(c):
    # TODO - maybe write a class to describe a schema column, so this could be
    # `c.pk_index` instead of _pk_index(c)
    return c["primaryKeyIndex"]


def _is_pk_column(c):
    return _pk_index(c) is not None


class Legend:
    """
    A legend is like a table-header that describes how a row is read.
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
        self.pk_columns = tuple(pk_columns)
        self.non_pk_columns = tuple(non_pk_columns)

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


# TODO: this class is unfinished, and so doesn't extend DatasetStructure.
class Dataset2:
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

    def __init__(self, tree):
        # Generally a pygit2.Tree, but conceptually just a directory tree
        # with the appropriate structure.
        self.tree = tree
        # TODO - add support for reading from pygit2.Index or similar during merge.

    def get_data_at(self, path):
        """Return the data at the given path from within this dataset."""
        leaf = self.tree / str(path)
        if not hasattr(leaf, "data"):
            raise IntegrityError(f"No data found at path {path}, type={type(leaf)}")
        return leaf.data

    @functools.lru_cache(maxsize=16)
    def get_legend(self, legend_hash):
        """Load the legend with the given hash from this dataset."""
        return Legend.loads(self.get_data_at(f"meta/legend/{legend_hash}"))

    @classmethod
    def encode_legend(cls, legend):
        """
        Given a legend, returns the path and the data which *should be written*
        to write this legend. This is almost the inverse of get_legend, except
        Dataset2 doesn't write the data.
        """
        return f"meta/legend/{legend.hexhash()}", legend.dumps()

    def read_raw_feature_dict(self, path):
        """
        Given a dataset path, reads the feature stored there, and returns the feature row as a dict of
        {column-id: value}. This is the raw feature dict - it returns only what is actually stored in the row,
        and is keyed by internal column IDs. Getting a dict where the keys have their user-visible
        names and are in the user-visible order requires looking up the current Schema.
        """
        data = self.get_data_at(path)
        legend_hash, non_pk_values = _unpack(data)
        legend = self.get_legend(legend_hash)
        pk_values = self.decode_path_to_pk_values(path)
        return legend.value_tuples_to_raw_dict(pk_values, non_pk_values)

    @classmethod
    def decode_path_to_pk_values(cls, path):
        """Given a feature path, returns the pk values encoded in it."""
        encoded = os.path.basename(path)
        return _unpack(_b64decode_str(encoded))

    @classmethod
    def encode_raw_feature_dict(cls, raw_feature_dict, legend):
        """
        Given a feature row and a legednd, returns the path and the data which
        *should be written* to write this feature. This is almost the inverse
        of read_raw_feature_dict, except Dataset2 doesn't write the data.
        """
        pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
        path = cls.encode_pk_values_to_path(pk_values)
        data = _pack([legend.hexhash(), non_pk_values])
        return path, data

    @classmethod
    def encode_pk_values_to_path(self, pk_values):
        """Given some pk values, returns the path the feature should be written to."""
        packed_pk = _pack(pk_values)
        pk_hash = _hexhash(packed_pk)
        filename = _b64encode_str(packed_pk)
        return "/".join([".sno-table", pk_hash[:2], pk_hash[2:4], filename])
