from enum import Enum, auto
import binascii
import functools
import logging
import re
import sys

import click

from kart import crs_util
from kart.core import find_blobs_with_paths_in_tree
from kart.exceptions import InvalidOperation, UNSUPPORTED_VERSION
from kart.serialise_util import ensure_text, ensure_bytes, json_pack, json_unpack


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


class BaseDatasetMetaClass(type):
    """Metaclass that automatically splits a dataset class's META_ITEMS into PATH_META_ITEMS and PATTERN_META_ITEMS."""

    def __new__(*args):
        dataset_cls = type.__new__(*args)

        path_meta_items = {}
        pattern_meta_items = []
        for definition in dataset_cls.META_ITEMS:
            if definition.path:
                path_meta_items[definition.path] = definition
            else:
                pattern_meta_items.append(definition)

        dataset_cls.PATH_META_ITEMS = path_meta_items
        dataset_cls.PATTERN_META_ITEMS = pattern_meta_items
        return dataset_cls


class BaseDataset(metaclass=BaseDatasetMetaClass):
    """
    Common interface for all datasets.

    A Dataset instance is immutable since it is a view of a particular git tree.
    To get a new version of a dataset, commit the desired changes,
    then instantiate a new Dataset instance that references the new git tree.

    A dataset has a user-defined path eg `path/to/dataset`, and inside that it
    has a hidden folder with a special name - cls.DATASET_DIRNAME.
    The path to this folder is the inner_path: `path/to/dataset/DATASET_DIRNAME`.
    Similarly, a dataset's tree is the tree at `path/to/dataset`,
    and its inner_tree is the tree at `path/to/dataset/DATASET_DIRNAME`.

    All relative paths are defined as being relative to the inner_path / inner_tree.
    """

    # Subclasses should override these fields to properly implement the dataset interface.

    DATASET_TYPE = None  # Example: "hologram"
    VERSION = None  # Example: 1
    DATASET_DIRNAME = None  # Example: ".hologram-dataset.v1".
    # (This should match the pattern DATASET_DIRNAME_PATTERN in kart.structure.)

    # Paths are generally relative to self.inner_tree, but datasets may choose to put extra data in the outer
    # tree also where it will eventually be user-visible (once attachments are fully supported).

    # Where meta-items are stored - blobs containing metadata about the structure or schema of the dataset.
    META_PATH = "meta/"

    # Some common meta-items, used by many types of dataset (but not necessarily every dataset):

    # The dataset's name / title:
    TITLE = MetaItemDefinition("title", MetaItemFileType.TEXT)
    # A longer description about the dataset's contents:
    DESCRIPTION = MetaItemDefinition("description", MetaItemFileType.TEXT)
    # JSON representation of the dataset's schema. See kart/tabular/schema.py, datasets_v3.rst
    SCHEMA_JSON = MetaItemDefinition("schema.json", MetaItemFileType.JSON)
    # Any XML metadata about the dataset.
    METADATA_XML = MetaItemDefinition("metadata.xml", MetaItemFileType.XML)
    # CRS definitions in well-known-text:
    CRS_DEFINITIONS = MetaItemDefinition(
        re.compile(r"crs/(.*)\.wkt"), MetaItemFileType.WKT
    )

    # No meta-items are defined by default - subclasses have to define these themselves.
    META_ITEMS = ()

    @classmethod
    def is_dataset_tree(cls, tree):
        """Returns True if the given tree seems to contain a dataset of this type."""
        if tree is None:
            return False
        return (
            cls.DATASET_DIRNAME in tree
            and (tree / cls.DATASET_DIRNAME).type_str == "tree"
        )

    def __init__(self, tree, path, repo, dirname=None):
        """
        Initialise a dataset which has the given path and the contents from the given tree.

        tree - pygit2.Tree or similar, if supplied it must contains a subtree of name dirname.
               If set to None, this dataset will be completely empty, but this could still be useful as a placeholder or
               as a starting point from which to write a new dataset.
        path - a string eg "path/to/dataset". Should be the path to the given tree, if a tree is provided.
        repo - the repo in which this dataset is found, or is to be created. Since a dataset is a view of a particular
               tree, the repo's functionality is not generally needed, but this is used for obtaining repo.empty_tree.
        dirname - the name of the subtree in which the dataset data is kept eg ".hologram-dataset.v1".
                  If this is None, it defaults to the DATASET_DIRNAME from the class.
                  If this is also None, then inner_tree is set to the same as tree - this is not the normal structure of
                  a dataset, but is supported for legacy reasons.
        """
        assert path is not None
        assert repo is not None
        if dirname is None:
            dirname = self.DATASET_DIRNAME
        path = path.strip("/")

        self.L = logging.getLogger(self.__class__.__qualname__)

        self.tree = tree
        self.path = path
        self.dirname = dirname
        self.repo = repo

        self.inner_path = f"{path}/{dirname}" if dirname else path
        if self.tree is not None:
            self.inner_tree = self.tree / dirname if dirname else self.tree
        else:
            self.inner_tree = None

        self._empty_tree = repo.empty_tree

        self.ensure_only_supported_capabilities()

    def ensure_only_supported_capabilities(self):
        # TODO - loosen this restriction. A dataset with capabilities that we don't support should (at worst) be treated
        # the same as any other unsupported dataset.
        capabilities = self.get_meta_item("capabilities.json", missing_ok=True)
        if capabilities is not None:
            from .cli import get_version
            from .output_util import dump_json_output

            click.echo(
                f"The dataset at {self.path} requires the following capabilities which Kart {get_version()} does not support:",
                err=True,
            )
            dump_json_output(capabilities, sys.stderr)
            raise InvalidOperation(
                "Download the latest Kart to work with this dataset",
                exit_code=UNSUPPORTED_VERSION,
            )

    @functools.lru_cache()
    def get_subtree(self, subtree_path):
        if self.inner_tree is not None:
            try:
                return self.inner_tree / subtree_path
            except KeyError:
                pass
        # Returning an empty tree makes it easier for callers to not have to handle None as a special case.
        return self._empty_tree

    def get_blob_at(self, rel_path, missing_ok=False, from_tree=None):
        """
        Return the blob at the given relative path from within this dataset.
        If missing_ok is true, we return None instead of raising a KeyError for missing data.
        The caller can choose a subtree to look for the data inside, the default is to look in self.inner_tree
        """
        leaf = None
        caught_error = None
        from_tree = from_tree or self.inner_tree
        try:
            leaf = from_tree / str(rel_path)
            if leaf is not None and leaf.type_str == "blob":
                return leaf
        except (AttributeError, TypeError, KeyError) as e:
            caught_error = e

        # If we got here, that means leaf wasn't a blob, or one of the above
        # exceptions happened...
        if missing_ok:
            return None
        else:
            raise self._new_key_error_from_caught_error(rel_path, leaf, caught_error)

    def _new_key_error_from_caught_error(self, rel_path, leaf, caught_error):
        if caught_error and caught_error.args:
            detail = f": {caught_error.args[0]}"
        result = KeyError(
            f"No data found at rel-path {rel_path}, type={type(leaf)}{detail}"
        )
        if hasattr(caught_error, "code"):
            result.code = caught_error.code
        if hasattr(caught_error, "subcode"):
            result.subcode = caught_error.subcode
        return result

    def get_data_at(
        self, rel_path, as_memoryview=False, missing_ok=False, from_tree=None
    ):
        """
        Return the data at the given relative path from within this dataset.

        Data is usually returned as a bytestring.
        If as_memoryview=True is given, data is returned as a memoryview instead -
        (this avoids a copy, so can make loops more efficient for many rows)

        If missing_ok is true, we return None instead of raising a KeyError for missing data.

        The caller can choose a subtree to look for the data inside, the default is to look in self.inner_tree
        """
        blob = self.get_blob_at(rel_path, missing_ok=missing_ok, from_tree=from_tree)
        if blob is not None:
            return memoryview(blob) if as_memoryview else blob.data
        return None

    @property
    def meta_tree(self):
        return self.get_subtree(self.META_PATH)

    def get_meta_item_definition(self, meta_item_path):
        # Quicker dict lookup:
        definition = self.PATH_META_ITEMS.get(meta_item_path)
        if definition is not None:
            return definition
        # Slower pattern search:
        for definition in self.PATTERN_META_ITEMS:
            if definition.matches(meta_item_path):
                return definition
        return None

    @functools.lru_cache()
    def get_meta_item(self, meta_item_path, missing_ok=True):
        """
        Returns the meta-item at the given path (relative to self.meta_tree).
        Meta-items are "decoded" by decode_meta_item before being returned - for instance, this deserialises
        JSON objects using json.loads(). All user-visible meta-items should be JSON dumpable once deserialised, whether
        or not they are stored as JSON. Meta-items that are for internal use only can use binary fornats.
        """
        data = self.get_data_at(
            meta_item_path, missing_ok=missing_ok, from_tree=self.meta_tree
        )
        if data is None:
            return data
        definition = self.get_meta_item_definition(meta_item_path)
        file_type = MetaItemFileType.get_from_definition_or_suffix(
            definition, meta_item_path
        )
        return file_type.decode_from_bytes(data)

    @functools.lru_cache()
    def meta_items(self, min_visibility=MetaItemVisibility.VISIBLE):
        """
        Returns a dict of all the meta-items, keyed by meta-item-path.
        Meta-items returned are sorted by the order in which they appear in self.META_ITEMS,
        and extra (unexpected) meta-items are returned last of all.
        """

        # meta_items() is written in such a way that you shouldn't need to override it.
        # It always delegates to get_meta_item, so overriding that should be enough.

        result = {}
        for definition in self.META_ITEMS:
            if definition.visibility < min_visibility:
                continue
            result.update(self.get_meta_items_matching(definition))

        result.update(self.get_meta_items_matching(None))
        return result

    def get_meta_items_matching(self, definition):
        result = {
            path: self.get_meta_item(path)
            for path in self.get_meta_item_paths_matching(definition)
        }
        # Filter out any None values.
        return {k: v for k, v in result.items() if v is not None}

    def get_meta_item_paths_matching(self, definition):
        if definition and definition.path:
            return [definition.path]
        return self._meta_item_paths_grouped_by_definition().get(definition, [])

    @functools.lru_cache(maxsize=1)
    def _meta_item_paths_grouped_by_definition(self):
        result = {}
        for meta_item_path, blob in find_blobs_with_paths_in_tree(self.meta_tree):
            definition = self.get_meta_item_definition(meta_item_path)
            result.setdefault(definition, []).append(meta_item_path)
        return result
