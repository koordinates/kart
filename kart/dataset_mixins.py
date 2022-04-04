import functools

import pygit2

from kart.diff_structs import DatasetDiff, DeltaDiff, Delta
from kart.key_filters import DatasetKeyFilter, MetaKeyFilter, UserStringKeyFilter


class DatasetDiffMixin:
    """Adds diffing of meta-items to a dataset, by delegating to dataset.meta_items()"""

    def diff(self, other, ds_filter=DatasetKeyFilter.MATCH_ALL, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = DatasetDiff()
        meta_filter = ds_filter.get("meta", ds_filter.child_type())
        ds_diff["meta"] = self.diff_meta(other, meta_filter, reverse=reverse)
        return ds_diff

    def diff_meta(self, other, meta_filter=MetaKeyFilter.MATCH_ALL, reverse=False):
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

    def diff_to_wc(self, repo, ds_filter=DatasetKeyFilter.MATCH_ALL):
        """
        Generates a diff from self to the working-copy.
        It may be the case that only the dataset-revision used to write the working
        copy can be used to do this (if we are tracking changes from that revision).
        See diff_util.get_dataset_diff() to generate diffs more generally.
        """
        # Subclasses to override.
        pass

    def get_raw_diff_for_subtree(self, other, subtree_name, reverse=False):
        """
        Get a pygit2.Diff of the diff between some subtree of this dataset, and the same subtree of another dataset
        (generally the "same" dataset at a different revision).
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

    _INSERT_UPDATE_DELETE = (
        pygit2.GIT_DELTA_ADDED,
        pygit2.GIT_DELTA_MODIFIED,
        pygit2.GIT_DELTA_DELETED,
    )
    _INSERT_UPDATE = (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_MODIFIED)
    _UPDATE_DELETE = (pygit2.GIT_DELTA_MODIFIED, pygit2.GIT_DELTA_DELETED)

    def diff_subtree(
        self,
        other,
        subtree_name,
        key_filter=UserStringKeyFilter.MATCH_ALL,
        *,
        key_decoder_method,
        value_decoder_method,
        reverse=False,
    ):
        """
        Yields deltas from self -> other, but only for items that match the feature_filter.
        If reverse is true, yields deltas from other -> self.
        Uses get_raw_diff_from_subtree to find the initial diff, but interprets this diff
        according to key_decoder_method and value_decoder_method.
        """
        # TODO - if the key-filter is very restrictive (ie it has only a few items in) then
        # it would be more efficient if we first search for those items and diff only those.

        raw_diff = self.get_raw_diff_for_subtree(other, subtree_name, reverse=reverse)
        # NOTE - we could potentially call diff.find_similar() to detect renames here,
        #

        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        def _null_decoder(key):
            raise RuntimeError("Can't decode key when dataset is None")

        def get_decoder(dataset, method_name):
            return (
                getattr(dataset, method_name) if dataset is not None else _null_decoder
            )

        old_key_decoder = get_decoder(old, key_decoder_method)
        new_key_decoder = get_decoder(new, key_decoder_method)
        old_value_decoder = get_decoder(old, value_decoder_method)
        new_value_decoder = get_decoder(new, value_decoder_method)

        subtree_path = subtree_name.rstrip("/") + "/"

        for d in raw_diff.deltas:
            self.L.debug(
                "diff(): %s %s %s", d.status_char(), d.old_file.path, d.new_file.path
            )

            if d.status not in self._INSERT_UPDATE_DELETE:
                # RENAMED, COPIED, IGNORED, TYPECHANGE, UNMODIFIED, UNREADABLE, UNTRACKED
                # We don't enounter these status codes in the diffs we generate since we
                # only generate commit<>commit diffs without rename detection.
                raise NotImplementedError(f"Delta status: {d.status_char()}")

            if d.status in self._UPDATE_DELETE:
                old_path = subtree_path + d.old_file.path
                old_key = old_key_decoder(old_path)
            else:
                old_key = None

            if d.status in self._INSERT_UPDATE:
                new_path = subtree_path + d.new_file.path
                new_key = new_key_decoder(d.new_file.path)
            else:
                new_key = None

            if old_key not in key_filter and new_key not in key_filter:
                continue

            if d.status == pygit2.GIT_DELTA_ADDED:
                self.L.debug("diff(): insert %s (%s)", new_path, new_key)
            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                self.L.debug(
                    "diff(): update %s %s -> %s %s",
                    old_path,
                    old_key,
                    new_path,
                    new_key,
                )
            elif d.status == pygit2.GIT_DELTA_DELETED:
                self.L.debug("diff(): delete %s %s", old_path, old_key)

            if d.status in self._UPDATE_DELETE:
                old_feature_blob = old.get_blob_at(old_path)
                old_value_promise = functools.partial(
                    old_value_decoder, old_feature_blob
                )
                old_half_delta = old_key, old_value_promise
            else:
                old_half_delta = None

            if d.status in self._INSERT_UPDATE:
                new_feature_blob = new.get_blob_at(new_path)
                new_value_promise = functools.partial(
                    new_value_decoder, new_feature_blob
                )
                new_half_delta = new_key, new_value_promise
            else:
                new_half_delta = None

            yield Delta(old_half_delta, new_half_delta)
