import pygit2


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

    _GIT_STATUS_TO_PYGIT2 = {
        "A": pygit2.GIT_DELTA_ADDED,
        "D": pygit2.GIT_DELTA_DELETED,
        "M": pygit2.GIT_DELTA_MODIFIED,
    }

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

    @classmethod
    def from_git_name_status(cls, status_char, path):
        status = cls._GIT_STATUS_TO_PYGIT2[status_char]
        old_path = None
        new_path = None
        if status_char in "AM":
            new_path = path
        if status_char in "MD":
            old_path = path
        return RawDiffDelta(status, status_char, old_path, new_path)
