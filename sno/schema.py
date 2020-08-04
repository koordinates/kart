from collections import namedtuple
import uuid

from .serialise_util import (
    msg_pack,
    msg_unpack,
    json_pack,
    json_unpack,
    sha256,
    hexhash,
)


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
        pk_columns, non_pk_columns = msg_unpack(data)
        return cls(pk_columns, non_pk_columns)

    def dumps(self):
        """Writes this legend to a bytestring."""
        return msg_pack((self.pk_columns, self.non_pk_columns))

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
        return hexhash(self.dumps())


def pk_index_ordering(column):
    """Returns primary key columns first, in pk_index order, then other columns."""
    if column.pk_index is not None:
        return column.pk_index
    else:
        # Non primary-key columns after primary key columns.
        return float('inf')


ALL_DATA_TYPES = {
    "boolean",
    "blob",
    "date",
    "datetime",
    "float",
    "geometry",
    "integer",
    "interval",
    "numeric",
    "text",
    "time",
    "timestamp",
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

    @staticmethod
    def deterministic_id(*data):
        """*data (any types) -> str(UUID). Deterministic, irreversible."""
        bytes16 = sha256(*data).digest()[:16]
        return str(uuid.UUID(bytes=bytes16))

    def __new__(cls, id, name, data_type, pk_index, **extra_type_info):
        assert data_type in ALL_DATA_TYPES, data_type
        return super().__new__(cls, id, name, data_type, pk_index, extra_type_info)

    @classmethod
    def from_dict(cls, d):
        d = d.copy()
        return cls(
            d.pop("id"),
            d.pop("name"),
            d.pop("dataType"),
            d.pop("primaryKeyIndex"),
            **d,
        )

    def to_dict(self):
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

    def __hash__(self):
        return hash(
            (
                self.id,
                self.name,
                self.data_type,
                self.pk_index,
                frozenset(self.extra_type_info.items()),
            )
        )


class Schema:
    """A schema is just an immutable ordered list of ColumnSchemas."""

    def __init__(self, columns):
        """Create a new schema from a list of ColumnSchemas."""
        self._columns = tuple(columns)
        # Creating a legend validates the primaryKeyIndex field.
        self._legend = self._to_legend()
        self._pk_columns = tuple(
            c for c in sorted(columns, key=pk_index_ordering) if c.pk_index is not None
        )
        self._hash = hash(self._columns)

    @property
    def columns(self):
        return self._columns

    @property
    def legend(self):
        return self._legend

    @property
    def pk_columns(self):
        return self._pk_columns

    def __getitem__(self, i):
        """Return the _i_th ColumnSchema, or, the ColumnSchema with the given ID."""
        if isinstance(i, str):
            try:
                return next(c for c in self.columns if c.id == i)
            except StopIteration:
                raise KeyError(f"No such column: {i}")

        return self._columns[i]

    def __contains__(self, id):
        return any(c.id == id for c in self.columns)

    @classmethod
    def from_column_dicts(cls, column_dicts):
        columns = [ColumnSchema.from_dict(d) for d in column_dicts]
        return cls(columns)

    @classmethod
    def loads(cls, data):
        """Load a schema from a bytestring"""
        return cls.from_column_dicts(json_unpack(data))

    def to_column_dicts(self):
        return [c.to_dict() for c in self.columns]

    def dumps(self):
        """Writes this schema to a bytestring."""
        return json_pack(self.to_column_dicts())

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

    def encode_feature_blob(self, feature):
        """
        Given a feature, encodes it using this schema.
        Doesn't encode a path, so primary key values are not encoded.
        """
        raw_dict = self.feature_to_raw_dict(feature)
        pk_values, non_pk_values = self.legend.raw_dict_to_value_tuples(raw_dict)
        data = msg_pack([self.legend.hexhash(), non_pk_values])
        return data

    def _to_legend(self):
        pk_column_ids = []
        non_pk_column_ids = []
        for i, column in enumerate(sorted(self.columns, key=pk_index_ordering)):
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

    def __hash__(self):
        return self._hash

    def is_pk_compatible(self, other):
        """
        Does a schema change from self -> other mean that every feature needs a new path?
        Only if the primary key columns have changed.
        """
        return self.legend.pk_columns == other.legend.pk_columns

    def sanitise_pks(self, pk_values):
        """
        Fixes two common problems with pk_values, particularly if pk_values were provided by the user
        as text and so had to be parsed from text:
        1. pk_values should be a list / tuple, with one value per primary key column.
           (There is hardly ever >1 primary key column, but as this is what our model supports, we need a list.)
        2. integer columns need int values, so if the values were supplied as text, we need to cast to int here.
        """
        if isinstance(pk_values, tuple):
            pk_values = list(pk_values)
        elif not isinstance(pk_values, list):
            pk_values = [pk_values]
        for i, (value, column) in enumerate(zip(pk_values, self.pk_columns)):
            if column.data_type == "integer" and isinstance(value, str):
                pk_values[i] = int(pk_values[i])
        return tuple(pk_values)

    def align_to_self(self, new_schema):
        """
        Returns a new schema the same as the one given, except that some column IDs might be changed to match those in
        self. Column IDs are copied from self onto the resulting schema if the columns in the resulting schema
        are the "same" as columns in the previous schema. Uses a heuristic - columns are the same if they have the
        same name + type (handles reordering), or, if they have the same position and type (handles renames).
        """
        # TODO - could prompt the user to help with more complex schema changes.
        old_cols = self.to_column_dicts()
        new_cols = new_schema.to_column_dicts()
        Schema.align_schema_cols(old_cols, new_cols)
        return Schema.from_column_dicts(new_cols)

    @classmethod
    def align_schema_cols(cls, old_cols, new_cols):
        """Same as align_to_self, but works on lists of column dicts, instead of Schema objects."""
        for old_col in old_cols:
            old_col["done"] = False
        for new_col in new_cols:
            new_col["done"] = False

        # Align columns by name + type - handles reordering.
        old_cols_by_name = {c["name"]: c for c in old_cols}
        for new_col in new_cols:
            cls._try_align(old_cols_by_name.get(new_col["name"]), new_col)

        # Align columns by position + type - handles renames.
        for old_col, new_col in zip(old_cols, new_cols):
            cls._try_align(old_col, new_col)

        for old_col in old_cols:
            del old_col["done"]
        for new_col in new_cols:
            del new_col["done"]
        return new_cols

    @classmethod
    def _try_align(cls, old_col, new_col):
        if old_col is None or new_col is None:
            return False
        if old_col["done"] or new_col["done"]:
            return False
        if old_col["dataType"] != new_col["dataType"]:
            return False
        if old_col["primaryKeyIndex"] != new_col["primaryKeyIndex"]:
            return False
        new_col["id"] = old_col["id"]

        # We can't roundtrip size properly for GPKG primary keys since they have to be of type INTEGER.
        if (
            old_col["primaryKeyIndex"] is not None
            and "size" in old_col
            and "size" in new_col
            and old_col["size"] != new_col["size"]
        ):
            new_col["size"] = old_col["size"]

        old_col["done"] = True
        new_col["done"] = True
        return True

    def diff_types(self, new_schema):
        """Returns a dict showing which columns have been affected by which types of changes."""
        old_ids_list = [c.id for c in self]
        old_ids = set(old_ids_list)
        new_ids_list = [c.id for c in new_schema]
        new_ids = set(new_ids_list)

        inserts = new_ids - old_ids
        deletes = old_ids - new_ids
        position_updates = set()
        name_updates = set()
        type_updates = set()
        pk_updates = set()

        for new_index, new_col in enumerate(new_schema):
            col_id = new_col.id
            if col_id not in old_ids:
                continue
            old_col = self[col_id]

            old_index = old_ids_list.index(col_id)
            if old_index != new_index:
                position_updates.add(col_id)

            if old_col.name != new_col.name:
                name_updates.add(col_id)
            if (
                old_col.data_type != new_col.data_type
                or old_col.extra_type_info != new_col.extra_type_info
            ):
                type_updates.add(col_id)
            if old_col.pk_index != new_col.pk_index:
                pk_updates.add(col_id)

        return {
            "inserts": inserts,
            "deletes": deletes,
            "position_updates": position_updates,
            "name_updates": name_updates,
            "type_updates": type_updates,
            "pk_updates": pk_updates,
        }

    def diff_type_counts(self, new_schema):
        return {k: len(v) for k, v in self.diff_types(new_schema).items()}
