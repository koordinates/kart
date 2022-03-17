import re

import pygit2


POINTER_PATTERN = re.compile(rb"^oid sha256:([0-9a-fA-F]{64})$", re.MULTILINE)


def get_hash_from_pointer_file(pointer_file):
    if isinstance(pointer_file, pygit2.Blob):
        pointer_file = memoryview(pointer_file)
    match = POINTER_PATTERN.search(pointer_file)
    if match:
        return str(match.group(1), encoding="utf8")
    return None


def get_local_path_from_lfs_hash(repo, lfs_hash):
    return (
        repo.gitdir_path / "lfs" / "objects" / lfs_hash[0:2] / lfs_hash[2:4] / lfs_hash
    )
