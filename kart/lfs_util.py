import logging
import re

import pygit2

L = logging.getLogger(__name__)

POINTER_PATTERN = re.compile(rb"^oid sha256:([0-9a-fA-F]{64})$", re.MULTILINE)


def pointer_file_to_json(pointer_file_bytes):
    if isinstance(pointer_file_bytes, pygit2.Blob):
        pointer_file_bytes = pointer_file_bytes.data
    pointer_file_str = pointer_file_bytes.decode("utf8")
    result = {}
    for line in pointer_file_str.splitlines():
        if not line:
            continue
        parts = line.split(" ", maxsplit=1)
        if len(parts) < 2:
            L.warn(f"Error parsing pointer file:\n{line}")
            continue
        key, value = parts
        result[key] = value
    return result


def get_hash_from_pointer_file(pointer_file_bytes):
    """Given a pointer-file Blob or bytes object, extracts the sha256 hash from it."""
    if isinstance(pointer_file_bytes, pygit2.Blob):
        pointer_file_bytes = memoryview(pointer_file_bytes)
    match = POINTER_PATTERN.search(pointer_file_bytes)
    if match:
        return str(match.group(1), encoding="utf8")
    return None


def get_local_path_from_lfs_hash(repo, lfs_hash):
    """Given a sha256 LFS hash, finds where the object would be stored in the local LFS cache."""
    return (
        repo.gitdir_path / "lfs" / "objects" / lfs_hash[0:2] / lfs_hash[2:4] / lfs_hash
    )
