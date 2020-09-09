import re
import subprocess

import pygit2

from .timestamps import tz_offset_to_minutes


def get_head_tree(repo):
    """Returns the tree at the current repo HEAD."""
    if repo.is_empty:
        return None
    try:
        return repo.head.peel(pygit2.Tree)
    except pygit2.GitError:
        # This happens when the repo is not empty, but the current HEAD has no commits.
        return None


def get_head_commit(repo):
    """Returns the commit at the current repo HEAD."""
    if repo.is_empty:
        return None
    try:
        return repo.head.peel(pygit2.Commit)
    except pygit2.GitError:
        # This happens when the repo is not empty, but the current HEAD has no commits.
        return None


def get_head_branch(repo):
    """
    Returns the branch that HEAD is currently on.
    If HEAD is detached, returns None
    """
    if repo.head_is_detached:
        return None
    return repo.references["HEAD"].target


_GIT_VAR_OUTPUT_RE = re.compile(
    r"^(?P<name>.*) <(?P<email>[^>]*)> (?P<time>\d+) (?P<offset>[+-]?\d+)$"
)


def _signature(repo, var_name, **overrides):
    # 'git var' lets us use the environment variables to
    # control the user info, e.g. GIT_AUTHOR_DATE.
    # libgit2/pygit2 doesn't handle those env vars at all :(
    output = subprocess.check_output(
        ["git", "var", var_name], cwd=repo.path, encoding="utf8"
    )
    m = _GIT_VAR_OUTPUT_RE.match(output)
    kwargs = m.groupdict()
    kwargs["time"] = int(kwargs["time"])
    kwargs["offset"] = tz_offset_to_minutes(kwargs["offset"])
    kwargs.update(overrides)
    return pygit2.Signature(**kwargs)


def author_signature(repo, **overrides):
    return _signature(repo, "GIT_AUTHOR_IDENT", **overrides)


def committer_signature(repo, **overrides):
    return _signature(repo, "GIT_COMMITTER_IDENT", **overrides)
