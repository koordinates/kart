import click
from enum import Enum, auto
import pygit2
import os
import sys

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

# General code for working copies.
# Nothing specific to tabular working copies, nor file-based working copies.


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
        for part in self._all_parts_inluding_nones():
            if part is not None:
                yield part

    def _all_parts_inluding_nones(self):
        yield self.tabular
        if os.environ.get("X_KART_POINT_CLOUDS"):
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

    def create_parts_if_missing(self, parts_to_create, reset_to=DONT_RESET):
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
                p.reset(reset_to)

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
        quiet=False,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
    ):
        """Reset all working copy parts to the head commit. See reset() below."""

        self.reset(
            self.repo.head_commit,
            create_parts_if_missing=create_parts_if_missing,
            quiet=quiet,
            repo_key_filter=repo_key_filter,
            track_changes_as_dirty=track_changes_as_dirty,
            rewrite_full=rewrite_full,
        )

    def reset(
        self,
        commit_or_tree,
        *,
        create_parts_if_missing=(),
        quiet=False,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
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
        """

        created_parts = ()
        if create_parts_if_missing:
            # Even we're only partially resetting the WC, we still need to do a full reset on anything that
            # is newly created since it won't otherwise contain any data yet. The extra parameters (repo_key_filter
            # and track_changes_as_dirty) don't have any effect for a WC part that is newly created.
            created_parts = self.create_parts_if_missing(
                create_parts_if_missing, reset_to=commit_or_tree
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
                quiet=quiet,
            )

    def soft_reset_after_commit(
        self,
        commit_or_tree,
        *,
        quiet=False,
        mark_as_clean=None,
        now_outside_spatial_filter=None,
        committed_diff=None,
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


class WorkingCopyPart:
    """Abstract base class for a particular part of a working copy - eg the tabular part, or the file-based part."""

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

    def get_tree_id(self):
        return self.get_kart_state_value("*", "tree")

    def get_spatial_filter_hash(self):
        return self.get_kart_state_value("*", "spatial-filter-hash")

    def get_kart_state_value(self, table_name, key):
        """Looks up a value from the kart-state table."""
        raise NotImplementedError()

    def reset(
        self,
        commit_or_tree,
        *,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
        quiet=False,
    ):
        raise NotImplementedError()

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


def handle_working_copy_tree_mismatch(wc_type_name, actual_tree_id, expected_tree_id):
    actual_tree_id = f"tree {actual_tree_id}" if actual_tree_id else "the empty tree"
    expected_tree_id = (
        f"tree {expected_tree_id}" if expected_tree_id else "the empty tree"
    )

    message = [
        f"The {wc_type_name} working copy appears to be out of sync with the repository.",
        f"The working copy's own records show it is tracking {actual_tree_id};",
        f"based on the repository it should be tracking {expected_tree_id}.",
        "The most straight-forward fix is generally to recreate the working copy from the ground up (losing any uncommitted changes in the process.)",
    ]

    if get_input_mode() != InputMode.INTERACTIVE:
        message.append("To do so, try the following command:")
        message.append("kart create-workingcopy --delete-existing --discard-changes")
        raise BadStateError("\n".join(message), exit_code=BAD_WORKING_COPY_STATE)

    click.echo("\n".join(message))
    click.echo()
    if not click.confirm(
        "Do you want to recreate the working copy from the ground up?"
    ):
        raise BadStateError(message[0], exit_code=BAD_WORKING_COPY_STATE)

    from kart.create_workingcopy import create_workingcopy

    click.echo("Recreating working copy ...")
    ctx = click.get_current_context()
    subctx = click.Context(ctx.command, parent=ctx)
    subctx.obj = ctx.obj
    subctx.invoke(create_workingcopy, delete_existing=True, discard_changes=True)
    sys.exit(0)
