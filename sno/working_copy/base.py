import contextlib
import itertools
import logging

import pygit2

from sno.diff_structs import RepoDiff, DatasetDiff, DeltaDiff, Delta
from sno.exceptions import InvalidOperation, NotYetImplemented
from sno.filter_util import UNFILTERED
from sno.repository_version import get_repo_version
from sno.schema import Schema
from sno.structure import RepositoryStructure


SNO_WORKINGCOPY_PATH = "sno.workingcopy.path"


L = logging.getLogger("sno.working_copy.base")


class WorkingCopyDirty(Exception):
    """Exception to abort immediately if working copy is dirty."""

    pass


class Mismatch(ValueError):
    """Error for if the tree id stored in state table doesn't match the one at HEAD."""

    def __init__(self, working_copy_tree_id, expected_tree_id):
        self.working_copy_tree_id = working_copy_tree_id
        self.expected_tree_id = expected_tree_id

    def __str__(self):
        return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.expected_tree_id}"


class WorkingCopy:
    VALID_VERSIONS = (1, 2)

    TRACKING_NAME = "track"
    STATE_NAME = "state"

    @property
    def TRACKING_TABLE(self):
        return self._sno_table(self.TRACKING_NAME)

    @property
    def STATE_TABLE(self):
        return self._sno_table(self.STATE_NAME)

    @classmethod
    def get(cls, repo, create_if_missing=False):
        from .gpkg import WorkingCopy_GPKG_1, WorkingCopy_GPKG_2
        from .postgis import WorkingCopy_Postgis

        if create_if_missing:
            cls.ensure_config_exists(repo)

        repo_cfg = repo.config
        path_key = SNO_WORKINGCOPY_PATH
        if path_key not in repo_cfg:
            return None

        path = repo_cfg[path_key]
        if path.startswith("postgresql://"):
            return WorkingCopy_Postgis(repo, path)

        full_path = repo.workdir_path / path
        if not full_path.is_file() and not create_if_missing:
            return None

        version = get_repo_version(repo)
        if version not in cls.VALID_VERSIONS:
            raise NotImplementedError(f"Working copy version: {version}")
        if version < 2:
            return WorkingCopy_GPKG_1(repo, path)
        else:
            return WorkingCopy_GPKG_2(repo, path)

    @classmethod
    def ensure_config_exists(cls, repo):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        is_bare = bare_key in repo_cfg and repo_cfg.get_bool(bare_key)
        if is_bare:
            return

        path_key = SNO_WORKINGCOPY_PATH
        path = repo_cfg[path_key] if path_key in repo_cfg else None
        if path is None:
            cls.write_config(repo, None, False)

    @classmethod
    def write_config(cls, repo, path=None, bare=False):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        path_key = SNO_WORKINGCOPY_PATH

        if bare:
            repo_cfg[bare_key] = True
            repo.del_config(path_key)
        else:
            path = path or cls.default_path(repo)
            repo_cfg[bare_key] = False
            repo_cfg[path_key] = str(path)

    @classmethod
    def default_path(cls, repo):
        """Returns `example.gpkg` for a sno repo in a directory named `example`."""
        stem = repo.workdir_path.stem
        return f"{stem}.gpkg"

    def get_db_tree(self, table_name="*"):
        """Returns the hex tree ID from the state table."""
        raise NotImplementedError()

    def assert_db_tree_match(self, tree, *, table_name="*"):
        wc_tree_id = self.get_db_tree(table_name)
        expected_tree_id = tree.id.hex if isinstance(tree, pygit2.Tree) else tree

        if wc_tree_id != expected_tree_id:
            raise Mismatch(wc_tree_id, expected_tree_id)
        return wc_tree_id

    def _chunk(self, iterable, size):
        """Generator. Yield successive chunks from iterable of length <size>."""
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                return
            yield chunk

    def check_not_dirty(self, help_message=None):
        """Checks the working copy has no changes in it. Otherwise, raises InvalidOperation"""
        if not help_message:
            help_message = "Commit these changes (`sno commit`) or discard these changes (`sno reset`) first."
        if self.is_dirty():
            raise InvalidOperation(
                f"You have uncommitted changes in your working copy.\n{help_message}"
            )

    def is_dirty(self):
        """
        Returns True if there are uncommitted changes in the working copy,
        or False otherwise.
        """
        try:
            self.diff_to_tree(raise_if_dirty=True)
            return False
        except WorkingCopyDirty:
            return True

    def diff_to_tree(self, repo_filter=UNFILTERED, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for every dataset in the given repository structure.
        """
        repo_filter = repo_filter or UNFILTERED

        repo_diff = RepoDiff()
        for dataset in RepositoryStructure.lookup(self.repo, self.get_db_tree()):
            if dataset.path not in repo_filter:
                continue
            ds_diff = self.diff_db_to_tree(
                dataset,
                ds_filter=repo_filter[dataset.path],
                raise_if_dirty=raise_if_dirty,
            )
            repo_diff[dataset.path] = ds_diff
        repo_diff.prune()
        return repo_diff

    def diff_db_to_tree(self, dataset, ds_filter=None, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for a single dataset only.
        """
        ds_filter = ds_filter or UNFILTERED
        with self.session():
            ds_diff = DatasetDiff()
            ds_diff["meta"] = self.diff_db_to_tree_meta(dataset, raise_if_dirty)
            find_renames = self.can_find_renames(ds_diff["meta"])
            ds_diff["feature"] = self.diff_db_to_tree_feature(
                dataset, ds_filter.get("feature", ()), find_renames, raise_if_dirty
            )
        return ds_diff

    def diff_db_to_tree_meta(self, dataset, raise_if_dirty=False):
        """
        Returns a DeltaDiff showing all the changes of metadata between the dataset and this working copy.
        """
        meta_old = dict(dataset.meta_items())
        meta_new = dict(self.meta_items(dataset))
        if "schema.json" in meta_old and "schema.json" in meta_new:
            Schema.align_schema_cols(meta_old["schema.json"], meta_new["schema.json"])
        result = DeltaDiff.diff_dicts(meta_old, meta_new)
        if raise_if_dirty and result:
            raise WorkingCopyDirty()
        return result

    def diff_db_to_tree_feature(
        self, dataset, feature_filter, find_renames, raise_if_dirty=False
    ):
        pk_field = dataset.schema.pk_columns[0].name

        with self.session() as db:
            dbcur = db.cursor()
            self._execute_diff_query(dbcur, dataset, feature_filter)

            feature_diff = DeltaDiff()
            insert_count = delete_count = 0

            geom_col = dataset.geom_column_name

            for row in dbcur:
                track_pk = row[0]  # This is always a str
                db_obj = {k: row[k] for k in row.keys() if k != ".__track_pk"}

                if db_obj[pk_field] is None:
                    db_obj = None

                if db_obj is not None and geom_col is not None:
                    db_obj[geom_col] = self._db_geom_to_gpkg_geom(db_obj[geom_col])

                try:
                    repo_obj = dataset.get_feature(track_pk)
                except KeyError:
                    repo_obj = None

                if repo_obj == db_obj:
                    # DB was changed and then changed back - eg INSERT then DELETE.
                    # TODO - maybe delete track_pk from tracking table?
                    continue

                if raise_if_dirty:
                    raise WorkingCopyDirty()

                if db_obj and not repo_obj:  # INSERT
                    insert_count += 1
                    feature_diff.add_delta(Delta.insert((db_obj[pk_field], db_obj)))

                elif repo_obj and not db_obj:  # DELETE
                    delete_count += 1
                    feature_diff.add_delta(Delta.delete((repo_obj[pk_field], repo_obj)))

                else:  # UPDATE
                    pk = db_obj[pk_field]
                    feature_diff.add_delta(Delta.update((pk, repo_obj), (pk, db_obj)))

        if find_renames and (insert_count + delete_count) <= 400:
            self.find_renames(feature_diff, dataset)

        return feature_diff

    def _execute_diff_query(self, dbcur, dataset, feature_filter):
        """
        Does a join on the tracking table and the table for the given dataset, such that the dbcursor's result
        is all the rows that have been inserted / updated / deleted.
        """
        raise NotImplementedError()

    def _execute_dirty_rows_query(self, dbcur, dataset):
        """
        Queries the tracking table for the rows belonging to the given dataset, such that the dbcursor's result
        is the primary keys of all the rows that have been inserted / updated / deleted.
        """
        raise NotImplementedError()

    def reset_tracking_table(self, reset_filter=UNFILTERED):
        """Delete the rows from the tracking table that match the given filter."""
        raise NotImplementedError()

    def _reset_tracking_table_for_dataset(self, dbcur, dataset):
        """Delete the rows from the tracking table that match the given dataset."""
        raise NotImplementedError()

    def _db_geom_to_gpkg_geom(self, g):
        """Convert a geometry as returned by the database to a sno geometry.Geometry object."""
        raise NotImplementedError()

    def can_find_renames(self, meta_diff):
        """Can we find a renamed (aka moved) feature? There's no point looking for renames if the schema has changed."""
        if "schema.json" not in meta_diff:
            return True

        schema_delta = meta_diff["schema.json"]
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_schema = Schema.from_column_dicts(schema_delta.old_value)
        new_schema = Schema.from_column_dicts(schema_delta.new_value)
        dt = old_schema.diff_type_counts(new_schema)
        # We could still recognise a renamed feature in the case of type updates (eg int32 -> int64),
        # but basically any other type of schema modification means there's no point looking for renames.
        dt.pop("type_updates")
        return sum(dt.values()) == 0

    def find_renames(self, feature_diff, dataset):
        """
        Matches inserts + deletes into renames on a best effort basis.
        changes at most one matching insert and delete into an update per blob-hash.
        Modifies feature_diff in place.
        """

        def hash_feature(feature):
            return pygit2.hash(dataset.encode_feature_blob(feature)).hex

        inserts = {}
        deletes = {}

        for delta in feature_diff.values():
            if delta.type == "insert":
                inserts[hash_feature(delta.new_value)] = delta
            elif delta.type == "delete":
                deletes[hash_feature(delta.old_value)] = delta

        for h in deletes:
            if h in inserts:
                delete_delta = deletes[h]
                insert_delta = inserts[h]

                del feature_diff[delete_delta.key]
                del feature_diff[insert_delta.key]
                update_delta = delete_delta + insert_delta
                feature_diff.add_delta(update_delta)

    def update_state_table_tree(self, tree):
        """Write the given tree to the state table."""
        tree_id = tree.id.hex if isinstance(tree, pygit2.Tree) else tree
        L.info(f"Tree sha: {tree_id}")
        with self.session() as db:
            dbcur = db.cursor()
            changes = self._update_state_table_tree_impl(dbcur, tree_id)
        assert changes == 1, f"{self.STATE_TABLE} update: expected 1Δ, got {changes}"

    def _update_state_table_tree_impl(self, dbcur, tree_id):
        """
        Write the given tree ID to the state table.
        tree_id - str, the hex SHA of the tree at HEAD.
        """
        raise NotImplementedError()

    def reset(
        self,
        target_tree_or_commit,
        *,
        force=False,
        paths=None,
        track_changes_as_dirty=False,
    ):
        """
        Resets the working copy to the given target-tree (or the tree pointed to by the given target-commit).

        If there are uncommitted changes, raises InvalidOperation, unless force=True is given
        (in which case the changes are discarded)

        If track_changes_as_dirty=False (the default) the tree ID in the sno-state table gets set to the
        new tree ID and the tracking table is left empty. If it is True, the old tree ID is kept and the
        tracking table is used to record all the changes, so that they can be committed later.
        """
        if not force:
            self.check_not_dirty()

        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        commit = None
        if isinstance(target_tree_or_commit, pygit2.Commit):
            commit = target_tree_or_commit
            target_tree = commit.tree
        else:
            commit = None
            target_tree = target_tree_or_commit
        target_tree_id = target_tree.id.hex

        # base_tree is the tree the working copy is based on.
        # If the working copy exactly matches base_tree, it is clean and has an empty tracking table.
        base_tree_id = self.get_db_tree()
        base_tree = self.repo[base_tree_id]
        repo_tree_id = self.repo.head.peel(pygit2.Tree).hex

        L.debug(
            "reset(): WorkingCopy base_tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree: %s",
            base_tree_id,
            repo_tree_id,
            target_tree_id,
        )
        L.debug(
            f"reset(): commit={commit.id if commit else 'none'} track_changes_as_dirty={track_changes_as_dirty}",
        )

        repo_structure = RepositoryStructure(self.repo)
        base_datasets = {
            ds.table_name: ds
            for ds in self._filter_by_paths(repo_structure.iter_at(base_tree), paths)
        }
        if base_tree == target_tree:
            target_datasets = base_datasets
        else:
            target_datasets = {
                ds.table_name: ds
                for ds in self._filter_by_paths(
                    repo_structure.iter_at(target_tree), paths
                )
            }

        table_inserts = target_datasets.keys() - base_datasets.keys()
        table_deletes = base_datasets.keys() - target_datasets.keys()
        table_updates = base_datasets.keys() & target_datasets.keys()
        table_updates_unsupported = set()

        for table in table_updates:
            base_ds = base_datasets[table]
            ds_version = base_ds.VERSION

            # Do we support changing the WC metadata to back to base_ds metadata?
            rev_wc_meta_diff = self.diff_db_to_tree_meta(base_ds)
            update_supported = self._is_meta_update_supported(
                ds_version, rev_wc_meta_diff
            )

            # And, do we support then changing it from base_ds metadata to target_ds metadata?
            target_ds = target_datasets[table]
            if target_ds != base_ds:
                rev_rev_meta_diff = base_ds.diff_meta(target_ds)
                update_supported = update_supported and self._is_meta_update_supported(
                    ds_version, rev_rev_meta_diff
                )

            if not update_supported:
                table_updates_unsupported.add(table)

        for table in table_updates_unsupported:
            table_updates.remove(table)
            table_inserts.add(table)
            table_deletes.add(table)

        L.debug(
            "reset(): table_inserts: %s, table_deletes: %s, table_updates %s",
            table_inserts,
            table_deletes,
            table_updates,
        )

        structural_changes = table_inserts | table_deletes
        if track_changes_as_dirty and structural_changes:
            # We don't yet support tracking changes as dirty if we delete, create, or rewrite an entire table.
            structural_changes_text = "\n".join(structural_changes)
            raise NotYetImplemented(
                "Sorry, this operation is not possible when there are structural changes."
                f"Structural changes are affecting:\n{structural_changes_text}"
            )

        # Delete old tables
        if table_deletes:
            self.drop_table(
                target_tree_or_commit, *[base_datasets[d] for d in table_deletes]
            )
        # Write new tables
        if table_inserts:
            # Note: write_full doesn't work if called from within an existing db session.
            self.write_full(
                target_tree_or_commit, *[target_datasets[d] for d in table_inserts]
            )

        with self.session(bulk=1) as db:
            dbcur = db.cursor()

            for table in table_updates:
                base_ds = base_datasets[table]
                target_ds = target_datasets[table]
                self._update_table(
                    base_ds,
                    target_ds,
                    dbcur,
                    commit,
                    track_changes_as_dirty=track_changes_as_dirty,
                )

            if not track_changes_as_dirty:
                # update the tree id
                self._update_state_table_tree_impl(dbcur, target_tree_id)

    def _filter_by_paths(self, datasets, paths):
        """Filters the datasets so that only those matching the paths are returned."""
        if paths:
            return [ds for ds in datasets if ds.path.startswith(paths)]
        else:
            return datasets

    def _update_table(
        self, base_ds, target_ds, dbcur, commit=None, track_changes_as_dirty=False
    ):
        """
        Update the given table in working copy from its current state to target_ds.
        The table must exist in the working copy in the source and continue to exist in the destination,
        and not have any unsupported meta changes - see _is_meta_update_supported.
        base_ds - the dataset that this working copy table is currently based on.
        target_ds - the target desired state for this working copy table.
        dbcur - database cursor.
        commit - the commit that contains target_ds, if any.
        track_changes_if_dirty - whether to track changes made from base_ds -> target_ds as WC edits.
        """

        self._apply_meta_diff(base_ds, ~self.diff_db_to_tree_meta(base_ds), dbcur)
        # WC now has base_ds structure and so we can write base_ds features to WC.
        self._reset_dirty_rows(base_ds, dbcur)

        if target_ds != base_ds:
            self._apply_meta_diff(base_ds, base_ds.diff_meta(target_ds), dbcur)
            # WC now has target_ds structure and so we can write target_ds features to WC.
            self._apply_feature_diff(base_ds, target_ds, dbcur, track_changes_as_dirty)

    def _apply_feature_diff(
        self, base_ds, target_ds, dbcur, track_changes_as_dirty=False
    ):
        """
        Change the features of this working copy from their current state, base_ds - to the desired state, target_ds.
        base_ds - dataset containing the features that match the WC table currently.
        target_ds - dataset containing the desired features of the WC table.
        dbcur - database cursor.
        track_changes_as_dirty - whether to track these changes as working-copy edits in the tracking table.
        """
        feature_diff_index = base_ds.feature_tree.diff_to_tree(target_ds.feature_tree)
        if not feature_diff_index:
            return

        L.debug("Applying feature diff: about %s changes", len(feature_diff_index))

        delete_pks = []
        insert_and_update_pks = []

        for d in feature_diff_index.deltas:
            if d.old_file and d.old_file.path.startswith(base_ds.META_PATH):
                continue
            if d.new_file and d.new_file.path.startswith(base_ds.META_PATH):
                continue

            if d.status == pygit2.GIT_DELTA_DELETED:
                delete_pks.append(base_ds.decode_path_to_1pk(d.old_file.path))
            elif d.status in (pygit2.GIT_DELTA_ADDED, pygit2.GIT_DELTA_MODIFIED):
                insert_and_update_pks.append(
                    target_ds.decode_path_to_1pk(d.new_file.path)
                )
            else:
                # RENAMED, COPIED, IGNORED, TYPECHANGE, UNMODIFIED, UNREADABLE, UNTRACKED
                raise NotImplementedError(f"Delta status: {d.status_char()}")

        if not track_changes_as_dirty:
            # We don't want to track these changes as working copy edits - they will be part of the new WC base.
            ctx = self._suspend_triggers(dbcur, base_ds)
        else:
            # We want to track these changes as working copy edits so they can be committed later.
            ctx = contextlib.nullcontext()

        with ctx:
            self.delete_features(dbcur, base_ds, delete_pks)
            self.write_features(dbcur, target_ds, insert_and_update_pks)

    def _is_meta_update_supported(self, dataset_version, meta_diff):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported - even in datasets v1 - if we drop and rewrite the table,
        but of course it is less efficient).
        meta_diff - DeltaDiff object containing the meta changes.
        """

        # By default, no meta updates are supported (without dropping and rewriting).
        # Subclasses can override to support various types of meta updates.
        return not meta_diff

    def _apply_meta_diff(self, dataset, meta_diff, dbcur):
        """
        Change the metadata of this working copy according to the given meta diff.
        Not all changes are possible or supported - see _is_meta_update_supported.
        dataset - which table to update.
        meta_diff - a DeltaDiff object containing meta-item deltas for this dataset.
        dbcur - database cursor.
        """
        L.debug("Meta diff: %s changes", len(meta_diff))
        table_name = dataset.table_name
        for key in meta_diff:
            func_key = key.replace("/", "_").replace(".", "_")
            func = getattr(self, f"_apply_meta_{func_key}")
            delta = meta_diff[key]
            func(table_name, delta.old_value, delta.new_value, dbcur)

    def _reset_dirty_rows(self, base_ds, dbcur):
        """
        Reset the dirty rows recorded in the tracking table to match the originals from the dataset.
        base_ds - the dataset this WC table is based on.
        dbcur - database cursor.
        """
        track_count = self._execute_dirty_rows_query(dbcur, base_ds)
        dirty_pk_list = [r[0] for r in dbcur]
        if not dirty_pk_list:
            return

        # We're resetting the dirty rows so we don't track these changes in the tracking table.
        with self._suspend_triggers(dbcur, base_ds):
            # todo: suspend/remove spatial index
            L.debug("Cleaning up dirty rows...")

            count = self.delete_features(dbcur, base_ds, dirty_pk_list)
            L.debug(
                "_reset_dirty_rows(): removed %s features, tracking Δ count=%s",
                count,
                track_count,
            )
            count = self.write_features(
                dbcur, base_ds, dirty_pk_list, ignore_missing=True
            )
            L.debug(
                "_reset_dirty_rows(): wrote %s features, tracking Δ count=%s",
                count,
                track_count,
            )

            self._reset_tracking_table_for_dataset(dbcur, base_ds)
