import base64
import hashlib
import logging
from pathlib import Path
import re
import subprocess
import uuid

import pygit2

from kart.serialise_util import msg_pack, msg_unpack

L = logging.getLogger(__name__)

POINTER_PATTERN = re.compile(rb"^oid sha256:([0-9a-fA-F]{64})$", re.MULTILINE)

_BUF_SIZE = 1 * 1024 * 1024  # 1MB

_STANDARD_LFS_KEYS = set(("version", "oid", "size"))
_EMPTY_SHA256 = "sha256:" + ("0" * 64)


def install_lfs_hooks(repo):
    if not (repo.gitdir_path / "hooks" / "pre-push").is_file():
        subprocess.check_call(
            ["git", "-C", str(repo.gitdir_path), "lfs", "install", "hooks"]
        )


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
    with open(str(src_path), "rb") as src, open(str(dest_path), "wb") as dest:
        while True:
            data = src.read(_BUF_SIZE)
            if not data:
                break
            sha256.update(data)
            dest.write(data)
    return sha256.hexdigest(), size


def dict_to_pointer_file_bytes(pointer_dict, only_standard_keys=True):
    if "version" not in pointer_dict:
        pointer_dict["version"] = "https://git-lfs.github.com/spec/v1"
    if not pointer_dict["oid"].startswith("sha256:"):
        pointer_dict["oid"] = f"sha256:{pointer_dict['oid']}"

    if not only_standard_keys or not (pointer_dict.keys() - _STANDARD_LFS_KEYS):
        return _dict_to_pointer_file_bytes_simple(pointer_dict)

    extra_values = dict(
        (k, v) for k, v in sorted(pointer_dict.items()) if k not in _STANDARD_LFS_KEYS
    )
    encoded_extra_values = _encode_extra_values(extra_values)

    # The lfs spec requires keys after `version` to be sorted alphabetically.
    result = (
        f"version {pointer_dict['version']}\n"
        f"ext-0-kart-encoded.{encoded_extra_values} {_EMPTY_SHA256}\n"
        f"oid {pointer_dict['oid']}\n"
        f"size {pointer_dict['size']}\n"
    )
    return result.encode("utf8")


def _encode_extra_values(extra_values):
    packed = msg_pack(extra_values)
    # Using only the chars: [A-Z][a-z][0-9] . -
    return base64.b64encode(packed, altchars=b'.-').rstrip(b'=').decode('ascii')


def _decode_extra_values(encoded_extra_values):
    packed = base64.b64decode(
        (encoded_extra_values + '==').encode('ascii'), altchars=b'.-'
    )
    return msg_unpack(packed)


def _dict_to_pointer_file_bytes_simple(pointer_dict):
    blob = bytearray()
    for key, value in sorted(
        pointer_dict.items(), key=lambda kv: (kv[0] != "version", kv)
    ):
        blob += f"{key} {value}\n".encode("utf8")
    return blob


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
        key, value = parts
        if key.startswith("ext-0-kart-encoded."):
            result.update(_decode_extra_values(key[len("ext-0-kart-encoded.") :]))
        elif key == "size":
            result[key] = int(value)
        else:
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
    if lfs_hash.startswith("sha256:"):
        lfs_hash = lfs_hash[7:]  # len("sha256:")
    return (
        repo.gitdir_path / "lfs" / "objects" / lfs_hash[0:2] / lfs_hash[2:4] / lfs_hash
    )


def copy_file_to_local_lfs_cache(repo, source_path, conversion_func=None):
    """
    Given the path to a file, copies it to the appropriate location in the local LFS cache based on its sha256 hash.
    Optionally takes a conversion function which can convert the file while copying it - this saves us doing an extra
    copy after the convert operation, if we just write the converted version to where we would copy it.
    """

    lfs_tmp_path = repo.gitdir_path / "lfs" / "objects" / "tmp"
    lfs_tmp_path.mkdir(parents=True, exist_ok=True)

    tmp_object_path = lfs_tmp_path / str(uuid.uuid4())
    if conversion_func is None:
        # We can find the hash while copying in this case.
        # TODO - check if this is actually any faster.
        oid, size = get_hash_and_size_of_file_while_copying(
            source_path, tmp_object_path
        )
    else:
        conversion_func(source_path, tmp_object_path)
        oid, size = get_hash_and_size_of_file(tmp_object_path)

    actual_object_path = get_local_path_from_lfs_hash(repo, oid)
    actual_object_path.parents[0].mkdir(parents=True, exist_ok=True)
    tmp_object_path.rename(actual_object_path)

    return {
        "version": "https://git-lfs.github.com/spec/v1",
        "oid": f"sha256:{oid}",
        "size": size,
    }
