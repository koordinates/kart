import hashlib
import logging
from pathlib import Path

import re

import pygit2

L = logging.getLogger(__name__)

POINTER_PATTERN = re.compile(rb"^oid sha256:([0-9a-fA-F]{64})$", re.MULTILINE)


_BUF_SIZE = 1 * 1024 * 1024  # 1MB


def get_hash_and_size_of_file(path):
    """Given a path to a file, calculates and returns its SHA256 hash and length in bytes."""
    if not isinstance(path, Path):
        path = Path(path)
    assert path.is_file()

    size = path.stat().st_size
    sha256 = hashlib.sha256()
    with open(str(path), "rb") as src:
        while True:
            data = src.read(_BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest(), size


def get_hash_and_size_of_file_while_copying(src_path, dest_path, allow_overwrite=False):
    """
    Given a path to a file, calculates and returns its SHA256 hash and length in bytes,
    while copying it to the given destination.
    """
    if not isinstance(src_path, Path):
        src_path = Path(src_path)
    assert src_path.is_file()

    if not isinstance(dest_path, Path):
        dest_path = Path(dest_path)

    if allow_overwrite:
        assert not dest_path.is_dir()
    else:
        assert not dest_path.exists()

    size = src_path.stat().st_size
    sha256 = hashlib.sha256()
    with open(str(src_path), "rb"), open(str(dest_path)) as (src, dest):
        while True:
            data = input.read(_BUF_SIZE)
            if not data:
                break
            sha256.update(data)
            dest.write(data)
    return sha256.hexdigest(), size


def pointer_file_bytes_to_dict(pointer_file_bytes, result=None):
    if isinstance(pointer_file_bytes, pygit2.Blob):
        pointer_file_bytes = pointer_file_bytes.data
    pointer_file_str = pointer_file_bytes.decode("utf8")
    if result is None:
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


def dict_to_pointer_file_bytes(pointer_dict):
    blob = bytearray()
    for key, value in sorted(
        pointer_dict.items(), key=lambda kv: (kv[0] != "version", kv)
    ):
        # TODO - LFS doesn't support our fancy pointer files yet. Hopefully fix this in LFS.
        if key not in ("version", "oid", "size"):
            continue
        blob += f"{key} {value}\n".encode("utf8")
    return blob


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
    if lfs_hash.startswith("sha256:"):
        lfs_hash = lfs_hash[7:]  # len("sha256:")
    return (
        repo.gitdir_path / "lfs" / "objects" / lfs_hash[0:2] / lfs_hash[2:4] / lfs_hash
    )
