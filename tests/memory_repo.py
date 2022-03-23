class MemoryTree:
    """
    Test-only fake directory tree structure, behaves a lot like pygit2.Tree
    Contains a dict - all_blobs - of all file data contained anywhere in this tree and its descendants.
    Supports only two operators:
    ("some/path" in self) return True if a descendant tree or blob exists at the given path.
    self / "some/path" returns either a descendant MemoryTree, or a descendant MemoryBlob.
    More complex directory navigation is not supported.
    """

    def __init__(self, all_blobs):
        self.all_blobs = all_blobs

    @property
    def type_str(self):
        return "tree"

    def __contains__(self, path):
        path = path.strip("/")
        if path in self.all_blobs:
            return True
        dir_path = path + "/"
        return any((p.startswith(dir_path) for p in self.all_blobs))

    def __truediv__(self, path):
        path = path.strip("/")
        if path in self.all_blobs:
            return MemoryBlob(self.all_blobs[path])

        dir_path = path + "/"
        dir_path_len = len(dir_path)
        subtree = {
            p[dir_path_len:]: data
            for p, data in self.all_blobs.items()
            if p.startswith(dir_path)
        }
        if not subtree:
            raise KeyError(f"Path not found: {path}")
        return MemoryTree(subtree)


class MemoryBlob(bytes):
    """Test-only implementation of pygit2.Blob. Supports self.data and memoryview(self)."""

    @property
    def data(self):
        return self

    @property
    def type_str(self):
        return "blob"


class MemoryRepo:
    """Test-only repo that supports only repo.empty_tree - more properties can be added."""

    empty_tree = MemoryTree({})
