from collections import namedtuple

import pygit2

from .exceptions import NotFound, NO_COMMIT


class CommitWithReference:
    """
    Simple struct containing a commit, and optionally the reference used to find the commit.
    When struct is passed around in place of sole commit, then any code that uses the commit is able to give a better
    human-readable name to describe it.
    Example:
    >>> cwr = CommitWithReference.resolve(repo, "master")
    >>> cwr.commit.id
    fa003dadb153248d282230a05add62cdb012f926
    >>> cwr.reference.shortname
    'master'
    >>> str(cwr)
    '"master" (fa003dadb153248d282230a05add62cdb012f926)'
    """

    def __init__(self, commit, reference=None):
        self.commit = commit
        self.reference = reference

    def __str__(self):
        if self.reference is not None:
            return f'"{self.reference.shorthand}" ({self.commit.id.hex})'
        else:
            return f"({self.commit.id.hex})"

    def __repr__(self):
        if self.reference is not None:
            return f"<CommitWithReference commit={self.commit.id.hex} reference={self.reference.name}>"
        else:
            return f"<CommitWithReference commit={self.commit.id.hex} reference=None>"

    @property
    def id(self):
        return self.commit.id

    @property
    def tree(self):
        return self.commit.tree

    @property
    def shorthand(self):
        if self.reference is not None:
            return self.reference.shorthand
        return self.id.hex

    @property
    def shorthand_with_type(self):
        if self.reference is not None:
            if self.reference.name.startswith("refs/heads/"):
                return f'branch "{self.reference.shorthand}"'
            elif self.reference.name.startswith("refs/tags/"):
                return f'tag "{self.reference.shorthand}"'
            else:
                return f'"{self.reference.shorthand}"'
        return self.id.hex

    @staticmethod
    def resolve(repo, refish):
        """
        Alias for resolve_refish that returns a CommitWithReference, and,
        which raises NO_COMMIT if no commit is found with that ID / at that branch / etc.

        Refish could be:
        - a CommitWithReference or Commit object
        - branch name
        - tag name
        - remote branch
        - 'HEAD', 'HEAD~1', 'HEAD^', etc
        - '84e684e7c283163a2abe0388603705cc7fa02fc1' or '84e684e' - commit ref
        - 'refs/tags/1.2.3' some other refspec
        but not: branch ID or blob ID
        """
        if isinstance(refish, CommitWithReference):
            return refish
        elif isinstance(refish, pygit2.Commit):
            return CommitWithReference(refish)

        try:
            obj, reference = repo.resolve_refish(refish)
            commit = obj.peel(pygit2.Commit)
            return CommitWithReference(commit, reference)
        except (KeyError, pygit2.InvalidSpecError):
            raise NotFound(f"No commit found at {refish}", exit_code=NO_COMMIT)


# pygit2 always has this order - we use it too for consistency,
# and so we can meaningfully zip() our tuples with theirs
_ANCESTOR_OURS_THEIRS_ORDER = ("ancestor", "ours", "theirs")


class AncestorOursTheirs(namedtuple("AncestorOursTheirs", _ANCESTOR_OURS_THEIRS_ORDER)):
    """
    When merging two commits, we can end up with three versions of lots of things -
    commits, repository-structures, datasets, features, primary keys.
    The 3 versions  are the common ancestor, and 2 versions to be merged, "ours" and "theirs".
    Like pygit2, we keep the 3 versions always in the same order - ancestor, ours, theirs.
    """

    NAMES = _ANCESTOR_OURS_THEIRS_ORDER
    CHARS = tuple(n[0] for n in NAMES)

    @staticmethod
    def partial(*, ancestor=None, ours=None, theirs=None):
        """Supply some or all keyword arguments: ancestor, ours, theirs"""
        return AncestorOursTheirs(ancestor, ours, theirs)

    def __or__(self, other):
        # We don't allow any field to be set twice
        assert not self.ancestor or not other.ancestor
        assert not self.ours or not other.ours
        assert not self.theirs or not other.theirs
        result = AncestorOursTheirs(
            ancestor=self.ancestor or other.ancestor,
            ours=self.ours or other.ours,
            theirs=self.theirs or other.theirs,
        )
        return result


AncestorOursTheirs.EMPTY = AncestorOursTheirs(None, None, None)
