import contextlib
import logging
from enum import Enum, auto

import click
import pygit2
import sqlalchemy as sa

from kart.core import peel_to_commit_and_tree
from kart.exceptions import (
    InvalidOperation,
    NotFound,
    NotYetImplemented,
    BadStateError,
    UNCOMMITTED_CHANGES,
    NO_DATA,
    NO_WORKING_COPY,
    BAD_WORKING_COPY_STATE,
)
from kart.key_filters import RepoKeyFilter
from kart.output_util import get_input_mode, InputMode
from kart.sqlalchemy.upsert import Upsert as upsert


# General code for working copies.
# Nothing specific to tabular working copies, nor file-based working copies.


L = logging.getLogger("kart.working_copy")


class PartType(Enum):
    """Different types of working copy part currently supported by Kart."""

    TABULAR = auto()  # Table-based / vector eg GPKG or database server.
    WORKDIR = auto()  # File-based.


ALL_PART_TYPES = set(PartType)


class WorkingCopyDirty(Exception):
    """Exception to abort immediately if working copy is dirty."""

    pass


class WorkingCopy:
    """
    Interface through which the various working copy types are accessed.
    Currently only the tabular working copy is accessible here.
    """

    DONT_RESET = object()

    def __init__(self, repo):
        self.repo = repo

    def exists(self):
        """Returns True if any part of the WC exists - see assert_exists below."""
        return any(self.parts())

    def assert_exists(self, error_message=None):
        """
        Make sure that the working copy exists, print the provided error message if it does not, plus some extra context.
        Now that the working copy can potentially have more than one part, its not simple to say if it exists or not.
        If a particular part doesn't exist, we would generally treat that the same as if that part existed but had no diffs.
        However, if the user has spefically asked for a WC operation, and no WC parts at all exist, we want to warn the user.
        That is what this function is for.
        """

        if self.exists():
            return

        error_message = f"{error_message}:\n" if error_message else ""

        if self.repo.head_is_unborn:
            raise NotFound(
                f'{error_message}Empty repository.\n  (use "kart import" to add some data)',
                exit_code=NO_DATA,
            )
        if self.repo.is_bare:
            raise NotFound(
                f'{error_message}Repository is "bare": it has no working copy.',
                exit_code=NO_WORKING_COPY,
            )
        raise NotFound(
            f'{error_message}Repository has no working copy.\n  (use "kart create-workingcopy")',
            exit_code=NO_WORKING_COPY,
        )

    def check_not_dirty(self, help_message=None):
        """Checks that all parts of the working copy have no changes. Otherwise, raises InvalidOperation"""
        for p in self.parts():
            p.check_not_dirty(help_message=help_message)

    def assert_matches_head_tree(self):
        """
        Checks that all parts of the working copy are based on the HEAD tree, according to their kart-state tables.
        Otherwise, tries to help the user fix the problem and/or raises BadStateError.
        """
        self.assert_matches_tree(self.repo.head_tree)

    def assert_matches_tree(self, tree):
        """
        Checks that all parts of the working copy are based on the given tree, according to their kart-state tables.
        Otherwise, tries to help the user fix the problem and/or raises BadStateError.
        """
        for p in self.parts():
            p.assert_matches_tree(tree)

    def parts(self):
        """Yields extant working-copy parts. Raises an error if a corrupt or uncontactable part is encountered."""
        if self.repo.is_bare:
            return
        for part in self._all_parts_including_nones():
            if part is not None:
                yield part

    def _all_parts_including_nones(self):
        yield self.tabular
        yield self.workdir

    @property
    def tabular(self):
        """Return the tabular working copy of the Kart repository, or None if it does not exist."""
        if not hasattr(self, "_tabular"):
            self._tabular = self.get_tabular()
        return self._tabular

    @property
    def workdir(self):
        """Return the tabular working copy of the Kart repository, or None if it does not exist."""
        if not hasattr(self, "_workdir"):
            self._workdir = self.get_workdir()
        return self._workdir

    def get_tabular(
        self,
        allow_uncreated=False,
        allow_invalid_state=False,
        allow_unconnectable=False,
    ):
        """
        Gets the tabular working copy but with more fine-grained control.
        See TableWorkingCopy.get for more details.
        """

        from kart.tabular.working_copy.base import TableWorkingCopy

        return TableWorkingCopy.get(
            self.repo,
            allow_uncreated=allow_uncreated,
            allow_invalid_state=allow_invalid_state,
            allow_unconnectable=allow_unconnectable,
        )

    def get_workdir(self, allow_uncreated=False, allow_invalid_state=False):
        from kart.workdir import FileSystemWorkingCopy

        return FileSystemWorkingCopy.get(
            self.repo,
            allow_uncreated=allow_uncreated,
            allow_invalid_state=allow_invalid_state,
        )

    def workdir_diff_cache(self):
        """Returns a WorkdirDiffCache for caching the results of certain workdir operations over the course of a single diff."""
        w = self.workdir
        return w.workdir_diff_cache() if w else None

    def create_parts_if_missing(
        self, parts_to_create, reset_to=DONT_RESET, non_checkout_datasets=None
    ):
        """
        Creates the given parts if they are missing and can be created. Returns any created parts themselves.
        parts_to_create is a collection of PartType enum values.
        """
        created_parts = []
        if self.repo.is_bare:
            return created_parts

        for part_type in parts_to_create:
            assert part_type in PartType
            if part_type == PartType.TABULAR:
                if self.create_and_initialise_tabular():
                    created_parts.append(self.tabular)
            elif part_type == PartType.WORKDIR:
                if self.create_and_initialise_workdir():
                    created_parts.append(self.workdir)

        if reset_to != self.DONT_RESET:
            for p in created_parts:
                p.reset(reset_to, non_checkout_datasets=non_checkout_datasets)

        return created_parts

    def create_and_initialise_tabular(self):
        """
        Create the tabular part of the working copy if it currently doesn't exist
        but is configured so that we know where to put it. Otherwise, this has no effect.
        """
        from kart.tabular.working_copy import TableWorkingCopyStatus

        t = self.get_tabular(allow_uncreated=True)
        if t and not (t.status() & TableWorkingCopyStatus.INITIALISED):
            click.echo(f"Creating {t.WORKING_COPY_TYPE_NAME} working copy at {t} ...")
            t.create_and_initialise()
            self._tabular = t
            return True
        return False

    def create_and_initialise_workdir(self):
        """Create the workdir part of the working copy if it currently doesn't exist."""
        from kart.workdir import FileSystemWorkingCopyStatus

        w = self.get_workdir(allow_uncreated=True)
        if w and not (w.status() == FileSystemWorkingCopyStatus.CREATED):
            if not w.check_if_reflink_okay():
                return False
            click.echo(
                f"Creating {w.WORKING_COPY_TYPE_NAME} working copy in {w.path.stem} folder"
            )
            w.create_and_initialise()
            self._workdir = w
            return True
        return False

    def delete_tabular(self):
        """
        Deletes the tabular working copy - from disk or from a server - and removes the cached reference to it.
        Leaves the repo config unchanged (ie, running checkout will recreate a working copy in the same place).
        """
        t = self.get_tabular(allow_invalid_state=True)
        if t:
            t.delete()
        self._safe_delattr("_tabular")

    def delete_workdir(self):
        w = self.get_workdir(allow_invalid_state=True)
        if w:
            w.delete()
        self._safe_delattr("_workdir")

    def _safe_delattr(self, name):
        if hasattr(self, name):
            delattr(self, name)

    def reset_to_head(
        self,
        *,
        create_parts_if_missing=(),
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
        non_checkout_datasets=None,
        only_update_checkout_datasets=False,
        quiet=False,
    ):
        """Reset all working copy parts to the head commit. See reset() below."""

        self.reset(
            self.repo.head_commit,
            create_parts_if_missing=create_parts_if_missing,
            repo_key_filter=repo_key_filter,
            track_changes_as_dirty=track_changes_as_dirty,
            rewrite_full=rewrite_full,
            non_checkout_datasets=non_checkout_datasets,
            only_update_checkout_datasets=only_update_checkout_datasets,
            quiet=quiet,
        )

    def reset(
        self,
        commit_or_tree,
        *,
        create_parts_if_missing=(),
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
        non_checkout_datasets=None,
        only_update_checkout_datasets=False,
        quiet=False,
    ):
        """
        Resets the working copy to the given target-commit (or target-tree).
        This is called when we want to move content from the Kart repo ODB into the working copy - ie, during
        create-workingcopy, checkout, switch, restore, reset.

        Any existing changes which match the repo_key_filter will be discarded. Existing changes which do not
        math the repo_key_filter will be kept.

        If track_changes_as_dirty=False (the default) the tree ID in the kart_state table gets set to the
        new tree ID and the tracking table is left empty. If it is True, the old tree ID is kept and the
        tracking table is used to record all the changes, so that they can be committed later.

        If rewrite_full is True, then every dataset currently being tracked will be dropped, and all datasets
        present at commit_or_tree will be written from scratch using write_full.
        Since write_full honours the current repo spatial filter, this also ensures that the working copy spatial
        filter is up to date.

        non_checkout_datasets is the set of datasets that the user has configured not to be checked out - loaded
        from repo.non_checkout_datasets. (Supplied as an argument only to avoid reloading it from the config).

        If only_update_checkout_datasets is True, then only those datasets which have recently moved into or out of
        repo.non_checkout_datasets will be updated (ie, fully-written or deleted). Each dataset part independently tracks
        what the set of non_checkout_datasets were at last call to reset(), so each part handles this independently.
        """

        created_parts = ()
        if create_parts_if_missing:
            # Even we're only partially resetting the WC, we still need to do a full reset on anything that
            # is newly created since it won't otherwise contain any data yet. The extra parameters (repo_key_filter
            # and track_changes_as_dirty) don't have any effect for a WC part that is newly created.
            created_parts = self.create_parts_if_missing(
                create_parts_if_missing,
                reset_to=commit_or_tree,
                non_checkout_datasets=non_checkout_datasets,
            )

        for p in self.parts():
            if p in created_parts:
                # This part was already handled above.
                continue

            if not quiet:
                click.echo(f"Updating {p} ...")
            p.reset(
                commit_or_tree,
                repo_key_filter=repo_key_filter,
                track_changes_as_dirty=track_changes_as_dirty,
                rewrite_full=rewrite_full,
                non_checkout_datasets=non_checkout_datasets,
                only_update_checkout_datasets=only_update_checkout_datasets,
                quiet=quiet,
            )

    def soft_reset_after_commit(
        self,
        commit_or_tree,
        *,
        mark_as_clean=None,
        now_outside_spatial_filter=None,
        committed_diff=None,
        quiet=False,
    ):
        """
        Like a reset, this marks the working copy as now being based on the given target-tree (or the tree in the given
        target-commit). Unlike a reset, this (mostly) doesn't update the working copy contents - this is called after a
        commit operation, so the overall flow of dataset contents is from working copy into the Kart repo ODB. However,
        we still need to tidy up a few things afterwards:
        - the working copy is now based on the newly created commit, not the previous commit which is now the parent.
        - all of the dataset contents that were committed should no longer be tracked as dirty - it can be marked as clean.
        - newly committed features which are outside the spatial filter should be removed from the working copy, since they
          are no longer dirty and now no different to anything else outside the spatial filter.
        - anything which was automatically modified so that it could be committed as part of the commit operation will now
          have to be updated in the working copy to match what was actually committed.

        mark_as_clean - a RepoKeyFilter of what was committed and can be marked as clean. Most commonly, this is simply
            RepoKeyFilter.MATCH_ALL
        now_outside_spatial_filter - a RepoKeyFilter of the newly committed features that can simply be dropped since
            they are outside the spatial filter.
        committed_diff - the diff which was committed, contains info about automatically modified pieces (if any).
        """
        if now_outside_spatial_filter and not quiet:
            # TODO we currently only check if vector features match the filter - no other dataset types are supported.
            total_count = now_outside_spatial_filter.recursive_len()
            click.echo(
                f"Removing {total_count} features from the working copy that no longer match the spatial filter..."
            )

        for p in self.parts():
            p.soft_reset_after_commit(
                commit_or_tree,
                mark_as_clean=mark_as_clean,
                now_outside_spatial_filter=now_outside_spatial_filter,
                committed_diff=committed_diff,
            )

    def matches_spatial_filter_hash(self, spatial_filter_hash):
        """Returns True iff the spatial-filter-hash stored in every part matches the given hash."""
        for p in self.parts():
            if p.get_spatial_filter_hash() != spatial_filter_hash:
                return False
        return True

    def matches_non_checkout_datasets(self, non_checkout_datasets):
        for p in self.parts():
            if p.get_non_checkout_datasets() != non_checkout_datasets:
                return False
        return True

    def parts_status(self):
        from kart.sqlalchemy import DbType

        result = {
            "tabular": {
                "location": self.repo.workingcopy_location,
                "type": DbType.from_spec(self.repo.workingcopy_location).json_name,
                "status": "ok" if self.tabular else "notFound",
            },
            "workdir": {
                "status": "ok" if self.workdir else "notFound",
            },
        }
        return result


class WorkingCopyPart:
    """Abstract base class for a particular part of a working copy - eg the tabular part, or the file-based part."""

    @property
    def WORKING_COPY_TYPE_NAME(self):
        """Human readable name of this type of working copy, eg "PostGIS"."""
        raise NotImplementedError()

    @property
    def SUPPORTED_DATASET_TYPE(self):
        """The dataset type or types that this working copy supports."""
        raise NotImplementedError()

    def check_not_dirty(self, help_message=None):
        """Raises an InvalidOperation if this working-copy part is dirty."""
        if not self.is_dirty():
            return

        if not help_message:
            help_message = "Commit these changes (`kart commit`) or discard these changes (`kart restore`) first."
        raise InvalidOperation(
            f"You have uncommitted changes in your working copy.\n{help_message}",
            exit_code=UNCOMMITTED_CHANGES,
        )

    def is_dirty(self):
        """Returns True if this part has uncommitted changes, False if it does not."""
        raise NotImplementedError()

    def assert_matches_head_tree(self):
        """
        Checks that this part of the working copy is based on the HEAD tree, according to the kart-state table.
        Otherwise, tries to help the user fix the problem and/or raises BadStateError.
        """
        self.assert_matches_tree(self.repo.head_tree)

    def assert_matches_tree(self, expected_tree):
        """
        Checks that this part of the working copy is based on the given tree, according to the kart-state table.
        Otherwise, tries to help the user fix the problem and/or raises BadStateError.
        """
        if expected_tree is None or isinstance(expected_tree, str):
            expected_tree_id = expected_tree
        else:
            expected_tree_id = expected_tree.peel(pygit2.Tree).hex
        actual_tree_id = self.get_tree_id()

        if actual_tree_id != expected_tree_id:
            handle_working_copy_tree_mismatch(
                self.WORKING_COPY_TYPE_NAME, actual_tree_id, expected_tree_id
            )

    def state_session(self):
        """Opens a session for reading or writing to the state table."""
        raise NotImplementedError()

    # The following functions all depend on a piece of commonality between all working copy parts:
    # They all have a table called something like "kart_state" - the exact definition is found at
    # self.kart_tables.kart_state - and a session to it can be opened using self.state_session()

    def get_tree_id(self):
        """Returns the hex tree ID from the state table."""
        return self.get_kart_state_value("*", "tree")

    def get_spatial_filter_hash(self):
        """Returns the spatial filter hash from the state table."""
        return self.get_kart_state_value("*", "spatial-filter-hash")

    def get_non_checkout_datasets(self):
        kart_state = self.kart_tables.kart_state
        with self.state_session() as sess:
            r = sess.execute(
                sa.select([kart_state.c.table_name]).where(
                    sa.and_(
                        kart_state.c.key == "checkout", kart_state.c.value == "false"
                    )
                )
            )
            return set(row[0] for row in r)

    def get_kart_state_value(self, table_name, key):
        """Looks up a value from the kart-state table."""
        kart_state = self.kart_tables.kart_state
        with self.state_session() as sess:
            return sess.scalar(
                sa.select([kart_state.c.value]).where(
                    sa.and_(
                        kart_state.c.table_name == table_name, kart_state.c.key == key
                    )
                )
            )

    def update_state_table_tree(self, tree):
        """Write the given tree to the kart-state table."""
        tree_id = tree.id.hex if isinstance(tree, pygit2.Tree) else tree
        L.info(f"Tree sha: {tree_id}")
        with self.state_session() as sess:
            self._update_state_table_tree(sess, tree_id)

    def _update_state_table_tree(self, sess, tree_id):
        """
        Write the given tree ID to the state table.

        sess - state-table sqlalchemy session.
        tree_id - str, the hex SHA of the tree at HEAD.
        """
        r = sess.execute(
            upsert(self.kart_tables.kart_state),
            {"table_name": "*", "key": "tree", "value": tree_id or ""},
        )
        return r.rowcount

    def _update_state_table_spatial_filter_hash(self, sess, spatial_filter_hash):
        """
        Write the given spatial filter hash to the state table.

        sess - state-table sqlalchemy session.
        spatial_filter_hash - str, a hash of the spatial filter.
        """
        kart_state = self.kart_tables.kart_state
        if spatial_filter_hash:
            r = sess.execute(
                upsert(kart_state),
                {
                    "table_name": "*",
                    "key": "spatial-filter-hash",
                    "value": spatial_filter_hash,
                },
            )
        else:
            r = sess.execute(
                sa.delete(kart_state).where(kart_state.c.key == "spatial-filter-hash")
            )
        return r.rowcount

    def _update_state_table_non_checkout_datasets(self, sess, non_checkout_datasets):
        kart_state = self.kart_tables.kart_state
        sess.execute(sa.delete(kart_state).where(kart_state.c.key == "checkout"))
        if non_checkout_datasets:
            sess.execute(
                kart_state.insert(),
                [
                    {"table_name": ds_path, "key": "checkout", "value": "false"}
                    for ds_path in sorted(non_checkout_datasets)
                ],
            )

    def _is_noncheckout_dataset(self, sess, dataset):
        dataset = dataset.path if hasattr(dataset, "path") else str(dataset)
        kart_state = self.kart_tables.kart_state
        value = sess.scalar(
            sa.select([kart_state.c.value]).where(
                sa.and_(
                    kart_state.c.table_name == dataset, kart_state.c.key == "checkout"
                )
            )
        )
        return value == "false"

    def reset(
        self,
        commit_or_tree,
        *,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
        non_checkout_datasets=None,
        only_update_checkout_datasets=False,
        quiet=False,
    ):
        """
        Resets the working copy to the given target-tree (or the tree pointed to by the given target-commit).

        Any existing changes which match the repo_key_filter will be discarded. Existing changes which do not
        match the repo_key_filter will be kept.

        If track_changes_as_dirty=False (the default) the tree ID in the kart_state table gets set to the
        new tree ID and the tracking table is left empty. If it is True, the old tree ID is kept and the
        tracking table is used to record all the changes, so that they can be committed later.

        If rewrite_full is True, then every dataset currently being tracked will be dropped, and all datasets
        present at commit_or_tree will be written from scratch using write_full.
        Since write_full honours the current repo spatial filter, this also ensures that the working copy spatial
        filter is up to date.
        """

        if rewrite_full:
            # These aren't supported when we're doing a full rewrite.
            assert repo_key_filter.match_all and not track_changes_as_dirty

        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        if commit_or_tree is not None:
            target_commit, target_tree = peel_to_commit_and_tree(commit_or_tree)
            target_tree_id = target_tree.id.hex
        else:
            target_commit = None
            target_tree = None
            target_tree_id = None

        # base_tree is the tree the working copy is based on.
        # If the working copy exactly matches base_tree, then it is clean.

        base_tree_id = self.get_tree_id()
        base_tree = self.repo[base_tree_id] if base_tree_id else None
        repo_head_tree_id = self.repo.head_tree.hex if self.repo.head_tree else None

        L.debug(
            "reset(): WorkingCopy base_tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree: %s",
            base_tree_id,
            repo_head_tree_id,
            target_tree_id,
        )
        L.debug("reset(): track_changes_as_dirty=%s", track_changes_as_dirty)

        base_datasets = self.repo.datasets(
            base_tree,
            repo_key_filter=repo_key_filter,
            filter_dataset_type=self.SUPPORTED_DATASET_TYPE,
        ).datasets_by_path()
        if base_tree == target_tree:
            target_datasets = base_datasets
        else:
            target_datasets = self.repo.datasets(
                target_tree,
                repo_key_filter=repo_key_filter,
                filter_dataset_type=self.SUPPORTED_DATASET_TYPE,
            ).datasets_by_path()

        ds_inserts = target_datasets.keys() - base_datasets.keys()
        ds_deletes = base_datasets.keys() - target_datasets.keys()
        ds_updates = base_datasets.keys() & target_datasets.keys()

        self._handle_non_checkout_dataset_changes(
            ds_inserts=ds_inserts,
            ds_deletes=ds_deletes,
            ds_updates=ds_updates,
            non_checkout_datasets=non_checkout_datasets,
            only_update_checkout_datasets=only_update_checkout_datasets,
        )

        if rewrite_full:
            # No updates are "supported" since we are rewriting everything.
            ds_updates_unsupported = set(ds_updates)
        else:
            ds_updates_unsupported = self._find_unsupported_updates(
                ds_updates, base_datasets, target_datasets
            )

        for ds_path in ds_updates_unsupported:
            ds_updates.remove(ds_path)
            ds_inserts.add(ds_path)
            ds_deletes.add(ds_path)

        structural_changes = ds_inserts | ds_deletes
        is_new_target_tree = base_tree != target_tree
        self._check_for_unsupported_structural_changes(
            structural_changes,
            is_new_target_tree,
            track_changes_as_dirty,
            repo_key_filter,
        )

        with self.state_session() as sess:
            if ds_inserts or ds_updates or ds_deletes:
                self._do_reset_datasets(
                    base_datasets=base_datasets,
                    target_datasets=target_datasets,
                    ds_inserts=ds_inserts,
                    ds_deletes=ds_deletes,
                    ds_updates=ds_updates,
                    base_tree=base_tree,
                    target_tree=target_tree,
                    target_commit=target_commit,
                    repo_key_filter=repo_key_filter,
                    track_changes_as_dirty=track_changes_as_dirty,
                    quiet=quiet,
                )

            if not track_changes_as_dirty:
                self._update_state_table_tree(sess, target_tree_id)
            self._update_state_table_spatial_filter_hash(
                sess, self.repo.spatial_filter.hexhash
            )
            self._update_state_table_non_checkout_datasets(sess, non_checkout_datasets)

    def _do_reset_datasets(
        self,
        *,
        base_datasets,
        target_datasets,
        ds_inserts,
        ds_updates,
        ds_deletes,
        base_tree=None,
        target_tree=None,
        target_commit=None,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        quiet=False,
    ):
        """
        Actually does the work required by reset(), once we've decided which datasets need which kind of updates.

        base_datasets - the state that this working copy is currently based on - a dict {dataset_path: dataset}
        target_datasets - the target state for the working copies to be modified - a dict {dataset_path: dataset}
        ds_inserts - the set of datasets that need to be written from scratch
        ds_updates - the set of datasets that are to be left in place (but maybe modified to match target_datasets state)
        ds_deletes - the set of datasets that need to be completely removed.
        base_tree - the tree that this workingcopy is currently based on
        target_tree - the target state of this working copy
        target_commit - the working copy may use target-commit metadata to update timestamps, if available.
        repo_key_filter - used to only update certain parts of certain datasets.
        track_changes_as_dirty - changes applied will be recorded as dirty in the tracking table or index.
        quiet - whether to show progress output.

        ds_inserts and ds_deletes may share some datasets in common, in which case these are to be first removed and
        then recreated. This is slower than updating, but always works, whereas updating the dataset in place may
        not support certain types of updates; see eg the limited capabilities of Sqlite ALTER TABLE:
        https://www.sqlite.org/lang_altertable.html
        """
        raise NotImplementedError()

    def _find_unsupported_updates(self, ds_updates, base_datasets, target_datasets):
        """
        Given a set of datasets we intend to reset from their current state to the target_datasets state using an
        update operation, return the set of datasets for which this is not possible and they will instead need
        to be rewritten from scratch (eg, certain schema changes may be unsupported except as full rewrites).
        """
        # Subclasses to override where needed.
        return set()

    def soft_reset_after_commit(
        self,
        commit_or_tree,
        *,
        mark_as_clean=None,
        now_outside_spatial_filter=None,
    ):
        raise NotImplementedError()

    def _check_for_unsupported_structural_changes(
        self,
        structural_changes,
        is_new_target_tree,
        track_changes_as_dirty,
        repo_key_filter,
    ):
        """
        Because we have a mixed strategy for keeping track of changes - sometimes we compare the working-copy
        directly to the original (generally for meta changes) and sometimes we maintain an index file or a
        list of dirty features - we don't currently support every possible combination of resets.
        """
        # TODO - the problem is that the tracking table is no longer accurate if we destroy and recreate the
        # whole table, but with enough extra logic, we could actually update it to be accurate.
        if track_changes_as_dirty and structural_changes and is_new_target_tree:
            # We don't yet support tracking changes as dirty if we delete, create, or rewrite an entire table.
            structural_changes_text = "\n".join(structural_changes)
            raise NotYetImplemented(
                "Sorry, this operation is not yet supported when there are structural changes."
                f" Structural changes are affecting:\n{structural_changes_text}"
            )

        unsupported_filters = set()
        for ds_path in structural_changes:
            ds_filter = repo_key_filter[ds_path]
            feature_filter = ds_filter.get("feature", ds_filter.child_type())
            if not ds_filter.match_all and not feature_filter.match_all:
                unsupported_filters.add(ds_path)

        if unsupported_filters:
            raise NotYetImplemented(
                "Sorry, this type of filter is not yet supported when there are structural changes."
                f" Unfilterable structural changes are affecting:\n{unsupported_filters}"
            )

    def _handle_non_checkout_dataset_changes(
        self,
        *,
        ds_inserts,
        ds_deletes,
        ds_updates,
        non_checkout_datasets,
        only_update_checkout_datasets,
    ):
        """
        Modify the planned list of datasets to create, delete, and update in the event that certain datasets
        have recently been moved into or out of the set of non_checkout_datasets.
        """

        # Current set of non_checkout_datasets as requested by caller.
        new_set = non_checkout_datasets or set()
        # The value of repo.non_checkout_datasets as stored at last reset() is stored in the state table:
        old_set = self.get_non_checkout_datasets()

        # This might looks a bit backwards - these are sets of things *not* to check out:
        # we need to insert a dataset if it was in the old-set but not on the new-set.
        ds_inserts_due_to_config_changes = old_set - new_set
        ds_deletes_due_to_config_changes = new_set - old_set

        # We don't add anything any of (ds_inserts, ds_deletes, ds_updates) if it is not already present in
        # at least one of those lists, since this indicates it is not currently relevant to this working copy part
        # (ie, the wrong type of dataset, or non-existent at the current / previous commit).

        # Insert (rather than update) a dataset if it is newly removed from the no-checkout list.
        ds_inserts |= ds_inserts_due_to_config_changes & ds_updates
        ds_inserts -= ds_deletes_due_to_config_changes

        # Delete (rather than update) a dataset if it is newly added to the no-checkout list.
        ds_deletes |= ds_deletes_due_to_config_changes & ds_updates
        ds_deletes -= ds_inserts_due_to_config_changes

        # We can only update a dataset if it was already checked out and still will be checked out.
        # That means if it is or was on the list of non_checkout_datasets, it shouldn't be on our update list.
        ds_updates -= old_set
        ds_updates -= new_set

        if only_update_checkout_datasets:
            ds_inserts &= ds_inserts_due_to_config_changes
            ds_deletes &= ds_deletes_due_to_config_changes
            ds_updates.clear()


def handle_working_copy_tree_mismatch(wc_type_name, actual_tree_id, expected_tree_id):
    actual_tree_id = f"tree {actual_tree_id}" if actual_tree_id else "the empty tree"
    expected_tree_id = (
        f"tree {expected_tree_id}" if expected_tree_id else "the empty tree"
    )

    summary = (
        f"The {wc_type_name} working copy appears to be out of sync with the repository"
    )
    message = [
        f"{summary}:",
        f"  * The working copy's own records show it is tracking {actual_tree_id};",
        f"  * Based on the repository it should be tracking {expected_tree_id}.",
        "The simplest fix is generally to recreate the working copy (losing any uncommitted changes in the process.)",
    ]

    if get_input_mode() != InputMode.INTERACTIVE:
        message.append("\nTo do so, try the following command:")
        message.append("\tkart create-workingcopy --delete-existing --discard-changes")
        raise BadStateError("\n".join(message), exit_code=BAD_WORKING_COPY_STATE)

    click.echo("\n".join(message))
    click.echo()
    if not click.confirm("Do you want to recreate the working copy?"):
        raise BadStateError(f"{summary}.", exit_code=BAD_WORKING_COPY_STATE)

    from kart.create_workingcopy import create_workingcopy

    click.echo("Recreating working copy ...")
    ctx = click.get_current_context()
    subctx = click.Context(ctx.command, parent=ctx)
    subctx.obj = ctx.obj
    subctx.invoke(create_workingcopy, delete_existing=True, discard_changes=True)

    orig_command = f"{ctx.command_path} {' '.join(ctx.unparsed_args)}"
    click.echo(f"\nContinuing with the original command: {orig_command}\n")
