from collections import namedtuple


class CommitWithReference:
    """
    Simple struct containing a commit, and optionally the reference used to find the commit.
    When struct is passed around in place of sole commit, then any code that uses the commit is able to give a better
    human-readable name to describe it.
    Example:
    >>> cwr = CommitWithReference.resolve_refish(repo, "master")
    >>> cwr.commit.id
    fa003dadb153248d282230a05add62cdb012f926
    >>> cwr.reference.shortname
    'master'
    >>> str(cwr)
    '"master" (fa003dadb153248d282230a05add62cdb012f926)'
    """

    def __init__(self, c, r=None):
        if type(c) == tuple:
            assert len(c) == 2
            assert r is None
            self.commit = c[0]
            self.reference = c[1]
        elif type(c) == CommitWithReference:
            assert r is None
            self.commit = c.commit
            self.reference = c.reference
        else:
            self.commit = c
            self.reference = r

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
    def resolve_refish(repo, refish):
        return CommitWithReference(repo.resolve_refish(refish))


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
