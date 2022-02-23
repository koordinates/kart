import pygit2

from .exceptions import NO_COMMIT, NotFound


class CommitWithReference:
    """
    Simple struct containing a commit, and optionally the reference used to find the commit.
    When struct is passed around in place of sole commit, then any code that uses the commit
    is able to give a better human-readable name to describe it.
    Example:
    >>> cwr = CommitWithReference.resolve(repo, "main")
    >>> cwr.commit.id
    fa003dadb153248d282230a05add62cdb012f926
    >>> cwr.reference.shortname
    'main'
    >>> str(cwr)
    '"main" (fa003dadb153248d282230a05add62cdb012f926)'
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
    def short_id(self):
        return self.commit.short_id

    @property
    def tree(self):
        return self.commit.tree

    @property
    def shorthand(self):
        if self.reference is not None:
            return self.reference.shorthand
        return self.commit.short_id

    @property
    def reference_type(self):
        if self.reference is None:
            return None
        elif self.reference.name.startswith("refs/heads/"):
            return "branch"
        elif self.reference.name.startswith("refs/tags/"):
            return "tag"
        return None

    @property
    def branch_shorthand(self):
        return self.reference.shorthand if self.reference_type == "branch" else None

    @staticmethod
    def resolve(repo, refish):
        """
        Alias for resolve_refish that returns a CommitWithReference, and,
        which raises NO_COMMIT if no commit is found with that ID / at that branch / etc.

        Refish could be:
        - a pygit2 OID object
        - branch name
        - tag name
        - remote branch
        - 'HEAD', 'HEAD~1', 'HEAD^', etc
        - '84e684e7c283163a2abe0388603705cc7fa02fc1' or '84e684e' - commit ref
        - 'refs/tags/1.2.3' some other refspec
        but not: branch ID or blob ID
        """
        if isinstance(refish, pygit2.Oid):
            refish = refish.hex
        try:
            obj, reference = repo.resolve_refish(refish)
            commit = obj.peel(pygit2.Commit)
            return CommitWithReference(commit, reference)
        except (KeyError, pygit2.InvalidSpecError):
            raise NotFound(f"No commit found at {refish}", exit_code=NO_COMMIT)
