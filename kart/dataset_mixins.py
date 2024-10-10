from collections.abc import Iterable
from typing import Callable, Optional, TYPE_CHECKING

import pygit2

from kart.diff_structs import DatasetDiff, DeltaDiff, Delta
from kart.diff_format import DiffFormat
from kart.key_filters import DatasetKeyFilter, MetaKeyFilter, UserStringKeyFilter


class DatasetDiffMixin:
    """
    This mixin should be added to a dataset to add diffing functionality.

    self.diff_meta() should work "out of the box" as long as self.meta_items() is implemented.
    self.diff_subtree() can be called with appropriate args to generate diffs of dataset contents, eg, features.
    """

    # Returns the meta-items diff for this dataset.
    def diff(
        self: "BaseDataset",
        other: Optional["BaseDataset"],
        ds_filter: DatasetKeyFilter = DatasetKeyFilter.MATCH_ALL,
        reverse: bool = False,
        diff_format: DiffFormat = DiffFormat.FULL,
    ):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = DatasetDiff()
        meta_filter = ds_filter.get("meta", ds_filter.child_type())
        ds_diff["meta"] = self.diff_meta(other, meta_filter, reverse=reverse)
        return ds_diff

    def diff_meta(
        self: "BaseDataset",
        other: Optional["BaseDataset"],
        meta_filter: UserStringKeyFilter = MetaKeyFilter.MATCH_ALL,
        reverse: bool = False,
    ):
        """
        Returns a diff from self -> other, but only for meta items.
        If reverse is true, generates a diff from other -> self.
        """
        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        meta_old = (
            {k: v for k, v in old.meta_items().items() if k in meta_filter}
            if old
            else {}
        )
        meta_new = (
            {k: v for k, v in new.meta_items().items() if k in meta_filter}
            if new
            else {}
        )
        return DeltaDiff.diff_dicts(meta_old, meta_new)

    def diff_to_working_copy(
        self: "BaseDataset",
        workdir_diff_cache: "WorkdirDiffCache",
        ds_filter: DatasetKeyFilter = DatasetKeyFilter.MATCH_ALL,
        *,
        convert_to_dataset_format: bool = None,
    ):
        """
        Generates a diff from self to the working-copy.
        It may be the case that only the dataset-revision used to write the working
        copy can be used to do this (if we are tracking changes from that revision).
        See diff_util.get_dataset_diff() to generate diffs more generally.
        """
        # Subclasses to override.
        pass

    def get_raw_diff_for_subtree(
        self: "BaseDataset",
        other: Optional["BaseDataset"],
        subtree_name: str,
        reverse: bool = False,
    ):
        """
        Get a pygit2.Diff of the diff between some subtree of this dataset, and the same subtree of another dataset
        (typically actually the "same" dataset, but at a different revision).
        """
        flags = pygit2.GIT_DIFF_SKIP_BINARY_CHECK
        self_subtree = self.get_subtree(subtree_name)
        other_subtree = other.get_subtree(subtree_name) if other else self._empty_tree

        diff = self_subtree.diff_to_tree(other_subtree, flags=flags, swap=reverse)
        self.L.debug(
            "diff %s (%s -> %s / %s): %s changes",
            subtree_name,
            self_subtree.id,
            other_subtree.id if other_subtree else None,
            "R" if reverse else "F",
            len(diff),
        )
        return diff

    def diff_subtree(
        self: "BaseDataset",
        other: Optional["BaseDataset"],
        subtree_name: str,
        key_filter: UserStringKeyFilter = UserStringKeyFilter.MATCH_ALL,
        *,
        key_decoder_method: str,
        value_decoder_method: str,
        key_encoder_method: Optional[str] = None,
        reverse: bool = False,
    ):
        """
        A pattern for datasets to use for diffing some specific subtree. Works as follows:
        1. Take some specific subtree of self and of a other
           (ie self.inner_tree / "feature", other.inner_tree / "feature")
        2. Use get_raw_diff_from_subtree to get a pygit2.Diff of the changes between those two trees.
        3. Go through all the resulting (insert, update, delete) deltas
        4. Fix up the paths to be relative to the dataset again (ie, prepend "feature/" onto them all
        5. Run some transform on each path to decide what to call each item (eg decode primary key)
        6. Run some transform on each path to load the content of each item (eg, read and decode feature)

        Args:
        other - a dataset similar to self (ie the same dataset, but at a different commit).
            This can be None, in which case there are no items in other and they don't need to be transformed.
        subtree_name - the name of the subtree of the dataset to scan for diffs.
        key_filter - deltas are only yielded if they involve at least one key that matches the key filter.
        key_decoder_method, value_decoder_method - these must be names of methods that are present in both
            self and other - self's methods are used to decode self's items, and other's methods for other's items.
        key_encoder_method - optional. A method that is present in both self and other that allows us to go
            straight to the keys the user is interested in (if they have requested specific keys in the key_filter).
        reverse - normally yields deltas from self -> other, but if reverse is True, yields deltas from other -> self.
        """

        subtree_name = subtree_name.rstrip("/")

        if not key_filter.match_all and key_encoder_method is not None:
            # Handle the case where we are only interested in a few features.
            deltas = self.get_raw_deltas_for_keys(
                other, key_encoder_method, key_filter, reverse=reverse
            )
        else:
            raw_diff = self.get_raw_diff_for_subtree(
                other, subtree_name, reverse=reverse
            )
            # NOTE - we could potentially call diff.find_similar() to detect renames here
            deltas = self.wrap_deltas_from_raw_diff(
                raw_diff, lambda path: f"{subtree_name}/{path}"
            )

        def _no_dataset_error(method_name):
            raise RuntimeError(
                f"Can't call {method_name} to decode diff deltas: dataset is None"
            )

        def get_dataset_attr(dataset, method_name):
            if dataset is not None:
                return getattr(dataset, method_name)
            # This shouldn't happen:
            return lambda x: _no_dataset_error(method_name)

        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        yield from self.transform_raw_deltas(
            deltas,
            key_filter,
            old_key_transform=get_dataset_attr(old, key_decoder_method),
            old_value_transform=get_dataset_attr(old, value_decoder_method),
            new_key_transform=get_dataset_attr(new, key_decoder_method),
            new_value_transform=get_dataset_attr(new, value_decoder_method),
        )

    # We treat UNTRACKED like an ADD since we don't have a staging area -
    # if the user has untracked files, we have to assume they want to add them.
    # (This is not actually needed right now since we are not using this for working copy diffs).
    _INSERT_TYPES = (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_UNTRACKED)
    _UPDATE_TYPES = (pygit2.GIT_DELTA_MODIFIED,)
    _DELETE_TYPES = (pygit2.GIT_DELTA_DELETED,)

    _INSERT_UPDATE_DELETE = _INSERT_TYPES + _UPDATE_TYPES + _DELETE_TYPES
    _INSERT_UPDATE = _INSERT_TYPES + _UPDATE_TYPES
    _UPDATE_DELETE = _UPDATE_TYPES + _DELETE_TYPES

    def get_raw_deltas_for_keys(
        self: "BaseDataset",
        other: Optional["BaseDataset"],
        key_encoder_method: str,
        key_filter: UserStringKeyFilter,
        reverse: bool = False,
    ):
        """
        Since we know which keys we are looking for, we can encode those keys and look up those blobs directly.
        We output this as a series of deltas, just as we do when we run a normal diff, so that we can
        take output from either code path and use it to generate a kart diff using transform_raw_deltas.
        """

        def _expand_keys(keys):
            # If the user asks for mydataset:feature:123 they might mean str("123") or int(123) - which
            # would be encoded differently. We look up both paths to see what we can find.
            for key in keys:
                yield key
                if isinstance(key, str) and key.isdigit():
                    yield int(key)

        encode_fn = getattr(self, key_encoder_method)
        paths = set()
        for key in _expand_keys(key_filter):
            try:
                paths.add(encode_fn(key, relative=True))
            except TypeError:
                # The path encoder for this dataset can't encode that key, so it won't be there.
                continue

        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        def _get_blob(dataset, path):
            if dataset is None or dataset.inner_tree is None:
                return None
            try:
                return dataset.inner_tree / path
            except KeyError:
                return None

        for path in paths:
            old_blob = _get_blob(old, path)
            new_blob = _get_blob(new, path)
            if old_blob is None and new_blob is None:
                continue
            if (
                old_blob is not None
                and new_blob is not None
                and old_blob.oid == new_blob.oid
            ):
                continue
            yield RawDiffDelta.of(
                path if old_blob else None, path if new_blob else None
            )

    def wrap_deltas_from_raw_diff(
        self: "BaseDataset", raw_diff: pygit2.Diff, path_transform: Callable[[str], str]
    ):
        for delta in raw_diff.deltas:
            old_path = path_transform(delta.old_file.path) if delta.old_file else None
            new_path = path_transform(delta.new_file.path) if delta.new_file else None
            yield RawDiffDelta(delta.status, delta.status_char(), old_path, new_path)

    def transform_raw_deltas(
        self: "BaseDataset",
        deltas: Iterable["RawDiffDelta"],
        key_filter: UserStringKeyFilter = UserStringKeyFilter.MATCH_ALL,
        *,
        old_key_transform: Callable[[str], str] = lambda x: x,
        old_value_transform: Callable[[str], str] = lambda x: x,
        new_key_transform: Callable[[str], str] = lambda x: x,
        new_value_transform: Callable[[str], str] = lambda x: x,
    ):
        """
        Given a list of RawDiffDeltas - inserts, updates, and deletes that happened at particular paths -
        yields a list of Kart deltas, which look something like ((old_key, old_value), (new_key, new_value)).
        A key could be a path, a meta-item name, or a primary key value.

        key-filter - deltas are discarded if they don't involve any keys that matches the key filter.
        old/new_key_transform - converts the path into a key.
        old/new_value_transform - converts the canonical-path into a value,
            presumably first by loading the file contents at that path.

        If any transform is not set, that transform defaults to returning the value it was input.
        """
        for d in deltas:
            self.L.debug("diff(): %s %s %s", d.status_char, d.old_path, d.new_path)

            if d.status not in self._INSERT_UPDATE_DELETE:
                # RENAMED, COPIED, IGNORED, TYPECHANGE, UNMODIFIED, UNREADABLE
                # We don't enounter these status codes in the diffs we generate.
                raise NotImplementedError(f"Delta status: {d.status_char}")

            if d.status in self._UPDATE_DELETE:
                old_key = old_key_transform(d.old_path)
            else:
                old_key = None

            if d.status in self._INSERT_UPDATE:
                new_key = new_key_transform(d.new_path)
            else:
                new_key = None

            if old_key not in key_filter and new_key not in key_filter:
                continue

            if d.status in self._INSERT_TYPES:
                self.L.debug("diff(): insert %s (%s)", d.new_path, new_key)
            elif d.status in self._UPDATE_TYPES:
                self.L.debug(
                    "diff(): update %s %s -> %s %s",
                    d.old_path,
                    old_key,
                    d.new_path,
                    new_key,
                )
            elif d.status in self._DELETE_TYPES:
                self.L.debug("diff(): delete %s %s", d.old_path, old_key)

            if d.status in self._UPDATE_DELETE:
                old_half_delta = old_key, old_value_transform(d.old_path)
            else:
                old_half_delta = None

            if d.status in self._INSERT_UPDATE:
                new_half_delta = new_key, new_value_transform(d.new_path)
            else:
                new_half_delta = None

            yield Delta(old_half_delta, new_half_delta)


class RawDiffDelta:
    """
    Just like pygit2.DiffDelta, this simply stores the fact that a particular git blob has changed.
    Exactly how it is changed is not stored - just the status and the blob paths.
    Contrast with diff_structs.Delta, which is higher level - it stores information about
    a particular feature or meta-item that has changed, and exposes the values it has changed from and to.

    This is needed to fill the same purpose as pygit2.DiffDelta because pygit2.DiffDelta's can't be
    created except by running a pygit2 diff - which we don't always want to do when generating diff deltas:
    see get_raw_deltas_for_keys.
    """

    __slots__ = ["status", "status_char", "old_path", "new_path"]

    def __init__(self, status, status_char, old_path, new_path):
        self.status = status
        self.status_char = status_char
        self.old_path = old_path
        self.new_path = new_path

    @classmethod
    def of(cls, old_path, new_path, reverse=False):
        if reverse:
            old_path, new_path = new_path, old_path

        if old_path is None:
            return RawDiffDelta(pygit2.GIT_DELTA_ADDED, "A", old_path, new_path)
        elif new_path is None:
            return RawDiffDelta(pygit2.GIT_DELTA_DELETED, "D", old_path, new_path)
        else:
            return RawDiffDelta(pygit2.GIT_DELTA_MODIFIED, "M", old_path, new_path)


if TYPE_CHECKING:
    # This is here to avoid circular imports
    from kart.base_dataset import BaseDataset, WorkdirDiffCache
