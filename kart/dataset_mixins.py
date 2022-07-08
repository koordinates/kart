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

    def diff_to_working_copy(
        self,
        workdir_diff_cache,
        ds_filter=DatasetKeyFilter.MATCH_ALL,
        *,
        convert_to_dataset_format=False,
    ):
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
        reverse - normally yields deltas from self -> other, but if reverse is True, yields deltas from other -> self.
        """
        # TODO - if the key-filter is very restrictive (ie it has only a few items in) then
        # it would be more efficient if we first search for those items and diff only those.

        subtree_name = subtree_name.rstrip("/")
        raw_diff = self.get_raw_diff_for_subtree(other, subtree_name, reverse=reverse)
        # NOTE - we could potentially call diff.find_similar() to detect renames here,

        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        def _no_dataset_error(method_name):
            raise RuntimeError(
                f"Can't call {method_name} to decode diff deltas: dataset is None"
            )

        def get_decoder(dataset, method_name):
            if dataset is not None:
                return getattr(dataset, method_name)
            # This shouldn't happen:
            return lambda x: _no_dataset_error(method_name)

        path_decoder = lambda path: f"{subtree_name}/{path}"

        yield from self.transform_raw_deltas(
            raw_diff.deltas,
            key_filter,
            old_path_transform=path_decoder,
            old_key_transform=get_decoder(old, key_decoder_method),
            old_value_transform=get_decoder(old, value_decoder_method),
            new_path_transform=path_decoder,
            new_key_transform=get_decoder(new, key_decoder_method),
            new_value_transform=get_decoder(new, value_decoder_method),
        )

    def generate_wc_diff_from_workdir_index(
        self,
        workdir_diff_cache,
        key_filter=UserStringKeyFilter.MATCH_ALL,
        *,
        wc_path_filter_pattern=None,
        wc_to_ds_path_transform=lambda x: x,
        ds_key_decoder=lambda x: x,
        wc_key_decoder=lambda x: x,
        ds_value_decoder=lambda x: x,
        wc_value_decoder=lambda x: x,
        try_promote_inserts_to_updates=False,
    ):
        """
        A pattern for datasets to use for diffing their contents against the working copy.
        1. Uses the workdir-index in workdir_diff_cache to get a list of changes made to the working copy,
           as a pygit2.Diff object.
        2. Go through all the resulting (insert, update, delete) deltas
        3. For each path in the working copy, find the associated path in the dataset (dataset paths are always
           slightly different to working copy paths - for instance working copy paths never have "/.some-dataset/"
           in their path, but dataset paths do.
        4. Run some transform on each path to decide what to call each item (eg decode primary key)
        5. Run some transform on each path to load the content of each item (eg, read and decode feature)

        Args:
        workdir_diff_cache - a WorkdirDiffCache object, through which the raw diff is generated.
        key_filter - deltas are only yielded if they involve at least one key that matches the key filter.
        wc_path_filter_pattern - if unset, then all deltas that involve the dataset's path will be considered. If set, then
            only those deltas involving paths that match the given pattern will be considered.
        wc_to_ds_path_transform - a function for taking a working-copy path, and returning the path to the equivalent item
            in the dataset. (The result should generally be a relative path, since that is easier for the dataset to use).
        ds_key_decoder, wc_key_decoder - functions which take a path and return the name of the item.
        ds_value_decoder, wc_value_decoder - functions which take a path and load the contents of the item.

        If any transform is not set, that transform defaults to returning the value it was input.

        try_promote_inserts_to_updates - if True, we optimistically try and load the old value for
        what seems to be an insert, and if that works, we promote it to an update. This is useful in
        the workdir if the user manages to create a new file that internally overwrites an existing item.
        """

        # Note that this operation returns changes that the user made to the working copy.
        # As such, all paths in it will be working-copy paths.
        if not workdir_diff_cache:
            return
        deltas = workdir_diff_cache.workdir_deltas_for_dataset(self)
        if not deltas:
            return

        if wc_path_filter_pattern:
            p = wc_path_filter_pattern
            deltas = (
                d
                for d in deltas
                if (d.old_file and p.search(d.old_file.path))
                or (d.new_file and p.search(d.new_file.path))
            )

        yield from self.transform_raw_deltas(
            deltas,
            key_filter,
            old_path_transform=wc_to_ds_path_transform,  # For old paths, we need dataset paths - need to transform.
            old_key_transform=ds_key_decoder,
            old_value_transform=ds_value_decoder,
            new_path_transform=lambda x: x,  # For new paths, we need WC paths, so no need to transform.
            new_key_transform=wc_key_decoder,
            new_value_transform=wc_value_decoder,
            try_promote_inserts_to_updates=try_promote_inserts_to_updates,
        )

    # We treat UNTRACKED like an ADD since we don't have a staging area -
    # if the user has untracked files, we have to assume they want to add them.
    # So far this is only relevant to point cloud datasets.
    _INSERT_TYPES = (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_UNTRACKED)
    _UPDATE_TYPES = (pygit2.GIT_DELTA_MODIFIED,)
    _DELETE_TYPES = (pygit2.GIT_DELTA_DELETED,)

    _INSERT_UPDATE_DELETE = _INSERT_TYPES + _UPDATE_TYPES + _DELETE_TYPES
    _INSERT_UPDATE = _INSERT_TYPES + _UPDATE_TYPES
    _UPDATE_DELETE = _UPDATE_TYPES + _DELETE_TYPES

    def transform_raw_deltas(
        self,
        deltas,
        key_filter=UserStringKeyFilter.MATCH_ALL,
        *,
        old_path_transform=lambda x: x,
        old_key_transform=lambda x: x,
        old_value_transform=lambda x: x,
        new_path_transform=lambda x: x,
        new_key_transform=lambda x: x,
        new_value_transform=lambda x: x,
        try_promote_inserts_to_updates=False,
    ):
        """
        Given a list of deltas - inserts, updates, and deletes -
        yields a list of Kart deltas, which look something like ((old_key, old_value), (new_key, new_value)).
        A key could be a path, a meta-item name, or a primary key value.

        key-filter - deltas are discarded if they don't involve any keys that matches the key filter.
        old/new_path_transform - converts the raw-path into a canonical path.
            Useful if the raw-path is not relative to the preferred folder, you can tidy it up first.
        old/new_key_transform - converts the canonical-path into a key.
        old/new_value_transform - converts the canonical-path into a value,
            presumably first by loading the file contents at that path.

        If any transform is not set, that transform defaults to returning the value it was input.

        try_promote_inserts_to_updates - if True, we optimistically try and load the old value for
        what seems to be an insert, and if that works, we promote it to an update. This is useful in
        the workdir if the user manages to create a new file that internally overwrites an existing item.
        """
        for d in deltas:
            self.L.debug(
                "diff(): %s %s %s", d.status_char(), d.old_file.path, d.new_file.path
            )

            if d.status not in self._INSERT_UPDATE_DELETE:
                # RENAMED, COPIED, IGNORED, TYPECHANGE, UNMODIFIED, UNREADABLE
                # We don't enounter these status codes in the diffs we generate.
                raise NotImplementedError(f"Delta status: {d.status_char()}")

            if d.status in self._UPDATE_DELETE:
                old_path = old_path_transform(d.old_file.path)
                old_key = old_key_transform(old_path)
            else:
                old_key = None

            if d.status in self._INSERT_UPDATE:
                new_path = new_path_transform(d.new_file.path)
                new_key = new_key_transform(d.new_file.path)
            else:
                new_key = None

            if old_key not in key_filter and new_key not in key_filter:
                continue

            if d.status in self._INSERT_TYPES:
                self.L.debug("diff(): insert %s (%s)", new_path, new_key)
            elif d.status in self._UPDATE_TYPES:
                self.L.debug(
                    "diff(): update %s %s -> %s %s",
                    old_path,
                    old_key,
                    new_path,
                    new_key,
                )
            elif d.status in self._DELETE_TYPES:
                self.L.debug("diff(): delete %s %s", old_path, old_key)

            if d.status in self._UPDATE_DELETE:
                old_half_delta = old_key, old_value_transform(old_path)
            else:
                old_half_delta = None

            if d.status in self._INSERT_UPDATE:
                new_half_delta = new_key, new_value_transform(new_path)
            else:
                new_half_delta = None

            if try_promote_inserts_to_updates and old_half_delta is None:
                old_path = old_path_transform(d.new_file.path)
                old_key = old_key_transform(old_path)
                old_value = old_value_transform(old_path)
                if old_value:
                    old_half_delta = old_key, old_value

            yield Delta(old_half_delta, new_half_delta)
