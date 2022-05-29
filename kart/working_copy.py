import pygit2

from kart.exceptions import (
    InvalidOperation,
    NotFound,
    UNCOMMITTED_CHANGES,
    NO_DATA,
    NO_WORKING_COPY,
)

# General code for working copies.
# Nothing specific to tabular working copies, nor file-based working copies.


class WorkingCopyDirty(Exception):
    """Exception to abort immediately if working copy is dirty."""

    pass


class WorkingCopyTreeMismatch(ValueError):
    """Error for if the tree id stored in state table doesn't match the one at HEAD."""

    def __init__(self, working_copy_tree_id, expected_tree_id):
        self.working_copy_tree_id = working_copy_tree_id
        self.expected_tree_id = expected_tree_id

    def __str__(self):
        return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.expected_tree_id}"


class WorkingCopy:
    """
    Interface through which the various working copy types are accessed.
    Currently only the tabular working copy is accessible here.
    """

    def __init__(self, repo):
        self.repo = repo

    def assert_exists(self, error_message=None):
        """
        Make sure that the working copy exists, print the provided error message if it does not, plus some extra context.
        Now that the working copy can potentially have more than one part, its not simple to say if it exists or not.
        If a particular part doesn't exist, we would generally treat that the same as if that part existed but had no diffs.
        However, if the user has spefically asked for a WC operation, and no WC parts at all exist, we want to warn the user.
        That is what this function is for.
        """

        if any(self.parts()):
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
        Otherwise, raises WorkingCopyTreeMismatch.
        """
        self.assert_matches_tree(self.repo.head_tree)

    def assert_matches_tree(self, tree):
        """
        Checks that all parts of the working copy are based on the given tree, according to their kart-state tables.
        Otherwise, raises WorkingCopyTreeMismatch.
        """
        for p in self.parts():
            p.assert_matches_tree(tree)

    def parts(self):
        """Yields extant working-copy parts. Raises an error if a corrupt or uncontactable part is encountered."""
        for part in self._all_parts_inluding_nones():
            if part is not None:
                yield part

    def _all_parts_inluding_nones(self):
        # TODO - add more parts here. Next part to be added is the file-based working copy for point clouds.
        yield self.tabular

    @property
    def tabular(self):
        """Return the tabular working copy of the Kart repository, or None if it does not exist."""
        if not hasattr(self, "_tabular"):
            self._tabular = self.get_tabular()
        return self._tabular

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

    def delete_tabular(self):
        """
        Deletes the tabular working copy - from disk or from a server - and removes the cached reference to it.
        Leaves the repo config unchanged (ie, running checkout will recreate a working copy in the same place).
        """
        t = self.get_tabular(allow_invalid_state=True)
        if t:
            t.delete()
        del self._tabular


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
        """Raises a WorkingCopyTreeMismatch if kart_state refers to a different tree and not the HEAD tree."""
        self.assert_matches_tree(self.repo.head_tree)

    def assert_matches_tree(self, expected_tree):
        """Raises a WorkingCopyTreeMismatch if kart_state refers to a different tree and not the given tree."""
        if expected_tree is None or isinstance(expected_tree, str):
            expected_tree_id = expected_tree
        else:
            expected_tree_id = expected_tree.peel(pygit2.Tree).hex
        actual_tree_id = self.get_tree_id()

        if actual_tree_id != expected_tree_id:
            raise WorkingCopyTreeMismatch(actual_tree_id, expected_tree_id)

    def get_tree_id(self):
        return self.get_kart_state_value("*", "tree")

    def get_spatial_filter_hash(self):
        return self.get_kart_state_value("*", "spatial-filter-hash")

    def get_kart_state_value(self, table_name, key):
        """Looks up a value from the kart-state table."""
        raise NotImplementedError()
