import re
import subprocess

import pygit2

from .timestamps import tz_offset_to_minutes

EMPTY_TREE_ID = '4b825dc642cb6eb9a060e54bf8d69288fbee4904'


def replace_subtree(repo, root_tree, subpath, subtree_or_blob):
    """
    Given a root tree, creates a new root tree by replacing whatever's
    at the given path with the given subtree or blob.

    If the given blob is None, that path is deleted.
    This never conflicts. If the containing dirs don't exist, they are created.

    Returns the new root tree.
    """
    if isinstance(subpath, str):
        subpath = [piece for piece in subpath.split('/') if piece]
    if not subpath:
        # replace root_tree with subtree
        return subtree_or_blob
    else:
        [head, *rest] = subpath
        builder = repo.TreeBuilder(root_tree)
        remove = False
        try:
            old_subtree = root_tree / head
            remove = True
        except KeyError:
            old_subtree = repo.get(EMPTY_TREE_ID)
        replaced = replace_subtree(repo, old_subtree, rest, subtree_or_blob)
        if remove:
            builder.remove(head)
        if replaced is not None:
            # insert/replace
            typ = (
                pygit2.GIT_FILEMODE_TREE
                if isinstance(replaced, pygit2.Tree)
                else pygit2.GIT_FILEMODE_BLOB
            )
            if isinstance(replaced, pygit2.Tree):
                replaced = replaced.oid
            builder.insert(head, replaced, typ)

        return repo.get(builder.write())


_GIT_VAR_OUTPUT_RE = re.compile(
    r"^(?P<name>.*) <(?P<email>[^>]*)> (?P<time>\d+) (?P<offset>[+-]?\d+)$"
)


def _signature(repo, var_name, **overrides):
    # 'git var' lets us use the environment variables to
    # control the user info, e.g. GIT_AUTHOR_DATE.
    # libgit2/pygit2 doesn't handle those env vars at all :(
    output = subprocess.check_output(
        ['git', 'var', var_name], cwd=repo.path, encoding='utf8'
    )
    m = _GIT_VAR_OUTPUT_RE.match(output)
    kwargs = m.groupdict()
    kwargs['time'] = int(kwargs['time'])
    kwargs['offset'] = tz_offset_to_minutes(kwargs['offset'])
    kwargs.update(overrides)
    return pygit2.Signature(**kwargs)


def author_signature(repo, **overrides):
    return _signature(repo, 'GIT_AUTHOR_IDENT', **overrides)


def committer_signature(repo, **overrides):
    return _signature(repo, 'GIT_COMMITTER_IDENT', **overrides)
