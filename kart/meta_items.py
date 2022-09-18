from enum import Enum, auto
import functools
import re

from kart import crs_util
from kart.schema import Schema
from kart.serialise_util import ensure_text, ensure_bytes, json_pack, json_unpack


class TagsJsonFileType:
    # schema.json should be checked on read and write, by dropping any optional fields that are None.
    def decode_from_bytes(self, data):
        if data is None:
            return None
        return self.assert_list_of_strings(json_unpack(data))

    def encode_to_bytes(self, meta_item):
        if meta_item is None:
            return None
        return json_pack(self.assert_list_of_strings(meta_item))

    def assert_list_of_strings(self, meta_item):
        try:
            assert isinstance(meta_item, list)
            for tag in meta_item:
                assert isinstance(tag, str)
        except AssertionError as e:
            raise AssertionError("tags.json should be a list of strings")
        return meta_item


TagsJsonFileType.INSTANCE = TagsJsonFileType()


class SchemaJsonFileType:
    # schema.json should be normalised on read and write, by dropping any optional fields that are None.
    def decode_from_bytes(self, data):
        if data is None:
            return None
        return Schema.normalise_column_dicts(json_unpack(data))

    def encode_to_bytes(self, meta_item):
        if meta_item is None:
            return None
        if not isinstance(meta_item, Schema):
            meta_item = Schema.from_column_dicts(meta_item)
        return meta_item.dumps()


SchemaJsonFileType.INSTANCE = SchemaJsonFileType()


class MetaItemFileType(Enum):
    """Different types of meta-item a dataset may contain."""

    BYTES = auto()
    JSON = auto()
    TEXT = auto()
    WKT = auto()
    XML = auto()
    UNKNOWN = auto()

    @classmethod
    def get_from_suffix(cls, meta_item_path):
        parts = meta_item_path.rsplit(".", maxsplit=1)
        if len(parts) == 2:
            try:
                return cls[parts[1].upper()]
            except KeyError:
                pass
        return None

    def decode_from_bytes(self, data):
        if data is None:
            return None
        if self == self.BYTES:
            return data
        elif self in (self.TEXT, self.XML):
            return ensure_text(data)
        elif self == self.JSON:
            return json_unpack(data)
        elif self == self.WKT:
            return crs_util.normalise_wkt(ensure_text(data))
        else:
            try:
                return ensure_text(data)
            except UnicodeDecodeError:
                return binascii.hexlify(data).decode()

    def encode_to_bytes(self, meta_item):
        if meta_item is None:
            return meta_item
        if self == self.JSON:
            return json_pack(meta_item)
        elif self == self.WKT:
            return ensure_bytes(crs_util.normalise_wkt(meta_item))
        return ensure_bytes(meta_item)

    @classmethod
    def get_from_definition_or_suffix(cls, definition, meta_item_path):
        if definition is not None:
            return definition.file_type
        else:
            return (
                MetaItemFileType.get_from_suffix(meta_item_path)
                or MetaItemFileType.UNKNOWN
            )


@functools.total_ordering
class MetaItemVisibility(Enum):
    """
    Different meta-items have different levels of user-visibility.
    This is not a security model, as the user can view or edit any meta-item they want if they try hard enough.
    """

    # Some extra data we don't recognise is in the meta-item area. User is shown it and can edit it:
    EXTRA = 5
    # User is shown this meta-item (eg in `kart diff`) and can edit this meta-item:
    EDITABLE = 4
    # User is shown but cannot (easily) edit this meta-item
    VISIBLE = 3
    # User is not "shown" this meta-item (but may be able to see it if they request it).
    # This data belongs with the dataset and should be preserved if the dataset is rewritten somewhere new:
    HIDDEN = 2
    # User is not "shown" this meta-item and this data need not be preserved if the dataset is rewritten somewhere new
    # (ie, it is specific to how the dataset is encoded in this instance, it is not part of the data of the dataset.)
    INTERNAL_ONLY = 1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented


class MetaItemDefinition:
    """Used for storing meta-information about meta-items."""

    def __init__(
        self, path_or_pattern, file_type=None, visibility=MetaItemVisibility.EDITABLE
    ):
        assert path_or_pattern is not None
        assert visibility is not None

        if isinstance(path_or_pattern, str):
            self.path = path_or_pattern
            self.pattern = None
            if file_type is None:
                file_type = MetaItemFileType.get_from_suffix(file_type)
        else:
            self.path = None
            self.pattern = path_or_pattern
            if file_type is None:
                file_type = MetaItemFileType.get_from_suffix(path_or_pattern.pattern)

        if file_type is None:
            raise ValueError(f"Unknown file_type for meta-item: {path_or_pattern}")
        self.file_type = file_type
        self.visibility = visibility

    def __repr__(self):
        return f"MetaItemDefinition({self.path or self.pattern.pattern})"

    def matches(self, meta_item_path):
        if self.path:
            return self.path == meta_item_path
        elif self.pattern:
            return bool(self.pattern.fullmatch(meta_item_path))

    def match_group(self, meta_item_path, match_group):
        assert self.pattern
        match = self.pattern.fullmatch(meta_item_path)
        return match.group(match_group) if match else None

    # Some common meta-items, used by many types of dataset (but not necessarily every dataset):


# The dataset's name / title:
TITLE = MetaItemDefinition("title", MetaItemFileType.TEXT)

# A longer description about the dataset's contents:
DESCRIPTION = MetaItemDefinition("description", MetaItemFileType.TEXT)

# A list of tags - each tag is free form text.
TAGS_JSON = MetaItemDefinition("tags.json", TagsJsonFileType.INSTANCE)

# JSON representation of the dataset's schema. See kart/tabular/schema.py, datasets_v3.rst
SCHEMA_JSON = MetaItemDefinition("schema.json", SchemaJsonFileType.INSTANCE)

# No more than one unnamed CRS definition in a single file named "crs.wkt":
CRS_WKT = MetaItemDefinition("crs.wkt", MetaItemFileType.WKT)

# ... or for multiple CRS datasets, use the following:

# Any number of named CRS definitions in well-known-text in a folder called "crs":
CRS_DEFINITIONS = MetaItemDefinition(re.compile(r"crs/(.*)\.wkt"), MetaItemFileType.WKT)
