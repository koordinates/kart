import contextlib

import pygit2


class ObjectBuilder:
    """
    Useful for creating new commit, tree, and blob objects.

    A lot like a pygit2.TreeBuilder, but more powerful - the client can buffer any number of writes to any paths
    whereas a pygit2.TreeBuilder only lets you modify one tree at a time.
    Also a bit like a pygit2.Index, but much more performant since it uses dicts instead of sorted vectors.
    Conflicts are not detected.
    """

    def __init__(self, repo, initial_root_tree):
        """
        repo - The repository containing the initial_root_tree.
        initial_root_tree - the root tree that is being modified. All paths are specified relative to this tree.
            The root tree at a particular commit is a good choice, since modifying this tree and its children is the
            only way to create a new commit based on an old commit.
        """
        self.repo = repo
        self.root_tree = (
            initial_root_tree if initial_root_tree is not None else _empty_tree(repo)
        )

        self.root_dict = {}
        self.cur_path = []
        self.path_stack = []

    def _resolve_path(self, path):
        """
        Resolve the given a path relative to the current path.
        The given path can be a string like "a/b/c" or a list like ["a", "b", "c"].
        """
        if isinstance(path, str):
            if path.startswith("/"):
                raise RuntimeError("ObjectBuilder does not support absolute paths")
            if "\\" in path:
                raise RuntimeError(f"Paths should be '/' separated: {path}")
            path = path.strip("/").split("/")
        return self.cur_path + path

    @contextlib.contextmanager
    def chdir(self, path):
        """
        Change the current directory used to resolve paths by the given relative path.
        Returns a context manager - when the context manager is closed, the original current directory is restored.

        Example:
        >>> with object_builder.chdir("a/b/c/.sno-dataset"):
        >>>    # Make edits inside a/b/c/.sno-dataset:
        >>>    object_builder.remove("meta/title")
        >>> # Context manager closes, current path is reset to the default.
        """
        path = self._resolve_path(path)
        self.path_stack.append(self.cur_path)
        self.cur_path = path
        try:
            yield
        finally:
            self.cur_path = self.path_stack.pop()

    def insert(self, path, writeable):
        """Writes the given data - a tree, a blob, a bytes, or None - at the given relative path."""
        path = self._resolve_path(path)
        self._ensure_writeable(writeable)

        cur_dict = self.root_dict
        for name in path[:-1]:
            cur_dict = cur_dict.setdefault(name, {})
            if not isinstance(cur_dict, dict):
                raise RuntimeError(
                    f"Expected dict at {path} but found {type(cur_dict)}"
                )

        cur_dict[path[-1]] = writeable

    def remove(self, path):
        """Delete the data at the given relative path."""
        self.insert(path, None)

    def flush(self):
        """
        Writes new versions of git trees for all changes that are buffered in memory.
        Releases the changes in memory for garbage collection.
        A new version of the root tree is returned - this tree should be committed by the client if these changes are
        to persist. Alternatively, more changes can be made and flushed before committing.
        """
        self.root_tree = copy_and_modify_tree(self.repo, self.root_tree, self.root_dict)
        self.root_dict = {}
        return self.root_tree

    def commit(self, ref_name, author, committer, message, parent_oids):
        """Create a new commit that points to the current root tree (once all the changes have been flushed)."""
        tree_oid = self.flush().id
        commit_oid = self.repo.create_commit(
            ref_name, author, committer, message, tree_oid, parent_oids
        )
        return self.repo[commit_oid]

    def _ensure_writeable(self, writeable):
        if not isinstance(writeable, (pygit2.Tree, pygit2.Blob, bytes, type(None))):
            raise ValueError(f"Expected a writeable type but found {type(writeable)}")


def copy_and_modify_tree(repo, tree, changes):
    """
    Given a tree, and a nested dictionary of changes to be made to that tree, returns a modified copy of that tree.
    Each dicts keys are path components, and the leaf values must be the desired new value at that path -
    either pygit2.Tree, a pygi2.Blob, a bytes, or None (None means delete the data at the specified path).
    Conflicts are not detected.
    """
    if tree is None:
        tree = _empty_tree(repo)
    if not changes:
        return tree

    tree_builder = repo.TreeBuilder(tree)
    for name, new_value in changes.items():
        if isinstance(new_value, dict):
            try:
                subtree = tree / name
            except KeyError:
                subtree = None
            subtree = copy_and_modify_tree(repo, subtree, new_value)
            tree_builder.insert(name, subtree.oid, pygit2.GIT_FILEMODE_TREE)
        elif isinstance(new_value, pygit2.Tree):
            tree_builder.insert(name, new_value.oid, pygit2.GIT_FILEMODE_TREE)
        elif isinstance(new_value, pygit2.Blob):
            tree_builder.insert(name, new_value.oid, pygit2.GIT_FILEMODE_BLOB)
        elif isinstance(new_value, bytes):
            blob_oid = repo.create_blob(new_value)
            tree_builder.insert(name, blob_oid, pygit2.GIT_FILEMODE_BLOB)
        elif new_value is None:
            try:
                tree_builder.remove(name)
            except pygit2.GitError:
                pass  # Conflicts are not detected.

    tree_oid = tree_builder.write()
    tree = repo[tree_oid]
    return tree


def _empty_tree(repo):
    """Returns the empty tree object for this repo."""
    return repo.get(repo.TreeBuilder().write())
