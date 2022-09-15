import binascii
import functools
import logging
import re
import sys

import click

from kart.core import find_blobs_with_paths_in_tree
from kart.dataset_mixins import DatasetDiffMixin
from kart.exceptions import InvalidOperation, UNSUPPORTED_VERSION, PATCH_DOES_NOT_APPLY
from kart.meta_items import MetaItemFileType, MetaItemVisibility
from kart.serialise_util import ensure_bytes, json_pack


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

        if dataset_cls.DATASET_DIRNAME:
            dataset_cls.DATASET_DIRNAME_PATH = dataset_cls.DATASET_DIRNAME + "/"
        return dataset_cls


class BaseDataset(DatasetDiffMixin, metaclass=BaseDatasetMetaClass):
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

    # The name / type of main items that make up this dataset - ie, not the meta items.
    ITEM_TYPE = None

    WORKING_COPY_PART_TYPE = None  # Example: working_copy.PartType.TABULAR

    # Paths are generally relative to self.inner_tree, but table datasets put certain meta items in the outer-tree.
    # (This is an anti-pattern - when designing a new dataset type, don't put meta items in the outer-tree.)

    # Where meta-items are stored - blobs containing metadata about the structure or schema of the dataset.
    META_PATH = "meta/"

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

    def apply_meta_diff(
        self, meta_diff, object_builder, *, resolve_missing_values_from_ds=None
    ):
        """
        Apply a meta diff to this dataset in the most straight-forward way possible -
        ie, every delta causes a change to a single item in the meta/ tree, no items
        are special cased. Subclasses may override if certain meta items need special casing.

        meta_diff - the diff to apply.
        object_builder - wraps an existing tree, and allows for building a new tree on top.
        resolve_missing_values_from_ds - if set, updates in the diff need not be fully specified,
            they can be represented as inserts. The missing "old values" will be resolved by
            loading them from this dataset. The update will be considered a conflict if the current
            value of an item to be updated is different from the value as loaded from this dataset.
        """
        if not meta_diff:
            return

        no_conflicts = True

        resolve_missing_values_from_tree = None
        if resolve_missing_values_from_ds:
            resolve_missing_values_from_tree = resolve_missing_values_from_ds.meta_tree

        # Apply diff to hidden meta items folder: <dataset-path>/.<dataset-marker>/meta/<item-name>
        with object_builder.chdir(f"{self.inner_path}/{self.META_PATH}"):
            no_conflicts &= self._apply_meta_deltas_to_tree(
                meta_diff.values(),
                object_builder,
                self.meta_tree if self.inner_tree is not None else None,
                resolve_missing_values_from_tree=resolve_missing_values_from_tree,
            )

        if not no_conflicts:
            raise InvalidOperation(
                "Patch does not apply",
                exit_code=PATCH_DOES_NOT_APPLY,
            )

    def _apply_meta_deltas_to_tree(
        self,
        deltas,
        object_builder,
        existing_tree,
        *,
        resolve_missing_values_from_tree=None,
    ):
        # Applying diffs works even if there is no tree yet created for the dataset,
        # as is the case when the dataset is first being created right now.
        if existing_tree is None:
            # This lets us test if something is in existing_tree without crashing.
            existing_tree = ()

        no_conflicts = True
        for delta in deltas:
            no_conflicts &= self._apply_meta_delta_to_tree(
                delta,
                object_builder,
                existing_tree,
                resolve_missing_values_from_tree=resolve_missing_values_from_tree,
            )

        return no_conflicts

    def _apply_meta_delta_to_tree(
        self,
        delta,
        object_builder,
        existing_tree,
        *,
        resolve_missing_values_from_tree=None,
    ):
        """
        Applies the given delta to the given tree"""
        # Applying diffs works even if there is no tree yet created for the dataset,
        # as is the case when the dataset is first being created right now.
        if existing_tree is None:
            # This lets us test if something is in existing_tree without crashing.
            existing_tree = ()

        name = delta.key
        old_value = delta.old_value
        new_value = delta.new_value

        # Conflict detection
        if delta.type == "delete" and name not in existing_tree:
            click.echo(
                f"{self.path}: Trying to delete nonexistent meta item: {name}",
                err=True,
            )
            return False
        if delta.type == "insert" and name in existing_tree:
            current_data = (existing_tree / name).data
            if current_data:
                if resolve_missing_values_from_tree:
                    old_data = (resolve_missing_values_from_tree / name).data
                    if old_data != current_data:
                        click.echo(
                            f"{self.path}: Meta item was modified since patch: {name}",
                            err=True,
                        )
                        return False

                else:
                    click.echo(
                        f"{self.path}: Trying to create meta item that already exists: {name}",
                        err=True,
                    )
                    return False

        if delta.type == "update" and name not in existing_tree:
            click.echo(
                f"{self.path}: Trying to update nonexistent meta item: {name}",
                err=True,
            )
            return False
        if delta.type == "update" and self.get_meta_item(name) != old_value:
            click.echo(
                f"{self.path}: Trying to update out-of-date meta item: {name}",
                err=True,
            )
            return False

        # Actual implementation once we've figured out there's no conflict:
        if new_value is not None:
            definition = self.get_meta_item_definition(name)
            file_type = MetaItemFileType.get_from_definition_or_suffix(definition, name)
            object_builder.insert(name, file_type.encode_to_bytes(delta.new_value))
        else:
            object_builder.remove(name)

        return True

    def ensure_full_path(self, path):
        """Given a path relative to this dataset, returns its full path from the repo root."""
        if path.startswith(self.inner_path):
            # Already a full-path. Return as-is.
            return path
        elif self.DATASET_DIRNAME and path.startswith(self.DATASET_DIRNAME_PATH):
            # A path relative to the outer tree (prefer to use paths relative to the inner tree).
            return f"{self.path}/{path}"
        else:
            # A path relative to the inner tree.
            return f"{self.inner_path}/{path}"

    def ensure_rel_path(self, path):
        """Given a full path to something in this dataset, returns its path relative to the dataset inner-path."""
        if path.startswith(self.inner_path):
            # A full-path. Strip off the inner-path prefix.
            return path[len(self.inner_path) + 1 :]
        elif self.DATASET_DIRNAME and path.startswith(self.DATASET_DIRNAME_PATH):
            # A path relative to the outer tree. Strip off the dataset-dirname prefix.
            return path[len(self.DATASET_DIRNAME_PATH) :]
        else:
            # Already a rel-path. Return as is.
            return path

    def decode_path(self, path):
        """
        Given a path to something inside this dataset eg "meta/title" or "meta/crs/EPSG:4326.wkt"
        Returns a 2-tuple eg ("meta", "title"), or ("meta", "crs/EPSG:4326.wkt")
        Subclasses can override to do more complex decoding when the name of an item
        is not the same as the path to it.
        """
        rel_path = self.ensure_rel_path(path)
        parts = rel_path.split("/", maxsplit=1)
        if len(parts) == 2:
            return tuple(parts)
        else:
            # It generally shouldn't happen that we have files in the top-level.
            return ("", rel_path)
