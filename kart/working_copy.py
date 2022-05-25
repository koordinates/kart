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
