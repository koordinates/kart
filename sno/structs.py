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


# When merging two commits, we have three versions of lots of things - these three versions
# are the common ancestor, and the two versions to be merged, "ours" and "theirs".
AncestorOursTheirs = namedtuple("AncestorOursTheirs", ("ancestor", "ours", "theirs"))

AncestorOursTheirs.names = AncestorOursTheirs._fields
AncestorOursTheirs.chars = tuple(n[0] for n in AncestorOursTheirs.names)
