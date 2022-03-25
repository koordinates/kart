from enum import Enum, auto
import functools
import logging
import sys

import click

from kart import crs_util
from kart.exceptions import InvalidOperation, UNSUPPORTED_VERSION
from kart.serialise_util import ensure_text, ensure_bytes, json_pack, json_unpack


class MetaItemType(Enum):
    BYTES = auto()
    JSON = auto()
    TEXT = auto()
    WKT = auto()
    XML = auto()

    @classmethod
    def get(cls, name):
        try:
            return cls[name]
        except KeyError:
            return None


class BaseDataset:
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

    # Paths - these are generally relative to self.inner_tree, but datasets may choose to put extra data in the outer
    # tree also where it will eventually be user-visible (once attachments are fully supported).

    # Where meta-items are stored - blobs containing metadata about the structure or schema of the dataset.
    META_PATH = "meta/"

    # There are no other paths that are common to all types of dataset.

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

    def get_meta_item_type(self, meta_item_path):
        result = self.get_meta_item_type_from_suffix(meta_item_path)
        if result is None:
            result = self.get_meta_item_type_without_suffix(meta_item_path)
        return result

    def get_meta_item_type_from_suffix(self, meta_item_path):
        parts = meta_item_path.rsplit(".", maxsplit=1)
        if len(parts) == 2:
            return MetaItemType.get(parts[1].upper())
        return None

    def get_meta_item_type_without_suffix(self, meta_item_path):
        return MetaItemType.TEXT

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
        return self.decode_meta_item(data, meta_item_path)

    def decode_meta_item(self, data, meta_item_path):
        meta_item_type = self.get_meta_item_type(meta_item_path)
        if meta_item_type is MetaItemType.BYTES:
            return data
        elif meta_item_type in (MetaItemType.TEXT, MetaItemType.XML):
            return ensure_text(data)
        elif meta_item_type is MetaItemType.JSON:
            return json_unpack(data)
        elif meta_item_type is MetaItemType.WKT:
            return crs_util.normalise_wkt(ensure_text(data))
        else:
            raise RuntimeError(f"Unexpected meta_item_type: {meta_item_type}")

    def encode_meta_item(self, meta_item, meta_item_path):
        if meta_item is None:
            return None
        meta_item_type = self.get_meta_item_type(meta_item_path)
        if meta_item_type is MetaItemType.JSON:
            return json_pack(meta_item)
        elif meta_item_type == MetaItemType.WKT:
            return ensure_bytes(crs_util.normalise_wkt(meta_item))
        else:
            return ensure_bytes(meta_item)
