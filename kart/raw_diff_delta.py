import pygit2
from dataclasses import dataclass
from enum import IntEnum


@dataclass
class RawDiffFile:
    """
    Like a pygit2.DiffFile, but constructible.
    """

    id: pygit2.Oid
    path: str


## FIXME: remove this when upgrading pygit2 (it's in pygit2/enums.py from pygit2 1.14.0 onwards)
class DeltaStatus(IntEnum):
    """
    What type of change is described by a DiffDelta?

    `RENAMED` and `COPIED` will only show up if you run
    `find_similar()` on the Diff object.

    `TYPECHANGE` only shows up given `INCLUDE_TYPECHANGE`
    in the DiffOption option flags (otherwise type changes
    will be split into ADDED / DELETED pairs).
    """

    UNMODIFIED = pygit2.GIT_DELTA_UNMODIFIED
    "no changes"

    ADDED = pygit2.GIT_DELTA_ADDED
    "entry does not exist in old version"

    DELETED = pygit2.GIT_DELTA_DELETED
    "entry does not exist in new version"

    MODIFIED = pygit2.GIT_DELTA_MODIFIED
    "entry content changed between old and new"

    RENAMED = pygit2.GIT_DELTA_RENAMED
    "entry was renamed between old and new"

    COPIED = pygit2.GIT_DELTA_COPIED
    "entry was copied from another old entry"

    IGNORED = pygit2.GIT_DELTA_IGNORED
    "entry is ignored item in workdir"

    UNTRACKED = pygit2.GIT_DELTA_UNTRACKED
    "entry is untracked item in workdir"

    TYPECHANGE = pygit2.GIT_DELTA_TYPECHANGE
    "type of entry changed between old and new"

    UNREADABLE = pygit2.GIT_DELTA_UNREADABLE
    "entry is unreadable"

    CONFLICTED = pygit2.GIT_DELTA_CONFLICTED
    "entry in the index is conflicted"


@dataclass
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

    old_file: RawDiffFile | None
    new_file: RawDiffFile | None
    status: DeltaStatus
    status_char: str

    @classmethod
    def of(cls, old_file, new_file, *, reverse=False):
        if reverse:
            old_file, new_file = new_file, old_file

        if old_file is None:
            status = pygit2.GIT_DELTA_ADDED
            status_char = "A"
        elif new_file is None:
            status = pygit2.GIT_DELTA_DELETED
            status_char = "D"
        else:
            status = pygit2.GIT_DELTA_MODIFIED
            status_char = "M"

        return cls(
            status=status,
            status_char=status_char,
            old_file=old_file,
            new_file=new_file,
        )
