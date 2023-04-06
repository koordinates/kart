import base64
import hashlib
import json
import logging
from pathlib import Path
import re
import shutil
import stat
import uuid

import pygit2
from reflink import ReflinkImpossibleError

from kart.serialise_util import msg_pack, msg_unpack
from kart.reflink_util import reflink

L = logging.getLogger(__name__)

GIT_LFS_SPEC_V1 = "https://git-lfs.github.com/spec/v1"

POINTER_PATTERN = re.compile(rb"^oid sha256:([0-9a-fA-F]{64})$", re.MULTILINE)

_BUF_SIZE = 1 * 1024 * 1024  # 1MB

_STANDARD_LFS_KEYS = set(("version", "oid", "size"))
_EMPTY_SHA256 = "sha256:" + ("0" * 64)


PRE_PUSH_HOOK = "\n".join(["#!/bin/sh", 'kart lfs+ pre-push "$@"', ""])


def install_lfs_hooks(repo):
    pre_push_hook = repo.gitdir_path / "hooks" / "pre-push"
    if not pre_push_hook.is_file():
        pre_push_hook.parent.mkdir(parents=True, exist_ok=True)
        pre_push_hook.write_text(PRE_PUSH_HOOK)
        pre_push_hook.chmod(
            pre_push_hook.stat().st_mode | stat.S_IXOTH | stat.S_IXGRP | stat.S_IXUSR
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


def normalise_pointer_file_dict(
    pointer_dict, include_nonstandard_keys=True, include_version=False
):
    """
    Normalise a pointer-file dict ready for showing to the user. Keys are sorted, lists are comma-separated.
    Very similar to what we'd write to disk, but we don't include the LFS version - that's just noise.
    """
    return _process_pointer_file_dict(
        pointer_dict,
        include_nonstandard_keys=include_nonstandard_keys,
        include_version=include_version,
    )


def merge_pointer_file_dicts(
    *pointer_dicts, include_nonstandard_keys=True, include_version=False
):
    """Merge more than one pointer-file dict while normalising them. See normalise_pointer_file_dict."""
    return _process_pointer_file_dict(
        *pointer_dicts,
        include_nonstandard_keys=include_nonstandard_keys,
        include_version=include_version,
    )


_TRANSIENT_KEYS = (
    "name",
    "sourceName",
    "sourceFormat",
    "sourceSize",
    # Some of the PAM keys aren't actually transient but they should be stored in a separate pointer file:
    # make sure we don't serialize them in the tile pointer file using the "pam" prefix.
    "pamName",
    "pamSourceName",
    "pamOid",
    "pamSize",
)


def dict_to_pointer_file_bytes(pointer_dict, include_nonstandard_keys=True):
    """
    Normalise a pointer-file-dict ready for writing to disk, then encode it to a bytestring.
    In this case we do want to write the LFS version, the non-standard keys will have to be specially encoded,
    and there are some keys that are transient, which we never write to disk.
    """
    return _process_pointer_file_dict(
        pointer_dict,
        include_nonstandard_keys=include_nonstandard_keys,
        drop_keys=_TRANSIENT_KEYS,
        encode_to_bytes=True,
    )


def merge_dicts_to_pointer_file_bytes(*pointer_dicts, include_nonstandard_keys=True):
    """Merge more than one pointer-file dict while normalising and encoding them."""
    return _process_pointer_file_dict(
        *pointer_dicts,
        include_nonstandard_keys=include_nonstandard_keys,
        drop_keys=_TRANSIENT_KEYS,
        encode_to_bytes=True,
    )


def _process_pointer_file_dict(
    *pointer_dicts,
    include_nonstandard_keys=True,
    include_version=True,
    drop_keys=(),
    encode_to_bytes=False,
):
    result = {}
    for key in _iter_pointer_file_keys(
        *pointer_dicts,
        include_nonstandard_keys=True,
        include_version=include_version,
        drop_keys=drop_keys,
    ):
        value = next((p.get(key) for p in pointer_dicts if key in p), None)
        if key == "version" and value is None:
            value = GIT_LFS_SPEC_V1
        elif (
            key == "oid" and isinstance(value, str) and not value.startswith("sha256:")
        ):
            value = "sha256:" + value
        result[key] = value

    if not encode_to_bytes:
        return result

    # Any time we're serializing it to bytes, we should be including the version.
    assert include_version is True

    if len(result) == 3:
        result_str = (
            f"version {result['version']}\n"
            f"oid {result['oid']}\n"
            f"size {result['size']}\n"
        )
    else:
        standard = {
            "version": result.pop("version"),
            "oid": result.pop("oid"),
            "size": result.pop("size"),
        }
        nonstandard = result
        nonstandard_encoded = _encode_nonstandard_keys(nonstandard)
        result_str = (
            f"version {standard['version']}\n"
            f"ext-0-kart-encoded.{nonstandard_encoded} {_EMPTY_SHA256}\n"
            f"oid {standard['oid']}\n"
            f"size {standard['size']}\n"
        )
    return result_str.encode("utf8")


def _iter_pointer_file_keys(
    *pointer_dicts,
    include_nonstandard_keys=True,
    include_version=True,
    drop_keys=(),
):
    # Ordering - see Git LFS specification.
    # 1. version (which we sometimes skip, since its pretty meaningless to the user)
    # 2. non-standard-keys, sorted alphabetically
    # 3. oid
    # 4. size
    if include_version:
        yield "version"
    if include_nonstandard_keys:
        sorted_key_iter = (
            sorted(pointer_dicts[0])
            if len(pointer_dicts) == 1
            else sorted(set().union(*pointer_dicts))
        )
        for key in sorted_key_iter:
            if key in ("version", "oid", "size"):
                continue
            if drop_keys and key in drop_keys:
                continue
            yield key
    yield "oid"
    yield "size"


def _encode_nonstandard_keys(nonstandard_dict):
    packed = msg_pack(nonstandard_dict)
    # Using only the chars: [A-Z][a-z][0-9] . -
    return base64.b64encode(packed, altchars=b".-").rstrip(b"=").decode("ascii")


def _decode_extra_values(encoded_extra_values):
    packed = base64.b64decode(
        (encoded_extra_values + "==").encode("ascii"), altchars=b".-"
    )
    return msg_unpack(packed)


def _dict_to_pointer_file_bytes_simple(pointer_dict):
    blob = bytearray()
    for key, value in sorted(
        pointer_dict.items(), key=lambda kv: (kv[0] != "version", kv)
    ):
        blob += f"{key} {value}\n".encode("utf8")
    return blob


def pointer_file_bytes_to_dict(
    pointer_file_bytes, result=None, *, decode_extra_values=True
):
    if hasattr(pointer_file_bytes, "data"):
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
            if decode_extra_values:
                result.update(_decode_extra_values(key[len("ext-0-kart-encoded.") :]))
        elif key == "size":
            result[key] = int(value)
        else:
            result[key] = value
    return result


def get_hash_from_pointer_file(pointer_file_bytes):
    """Given a pointer-file Blob or bytes object, extracts the sha256 hash from it."""
    if isinstance(pointer_file_bytes, dict):
        # Already decoded - just trim off the sha256:
        oid = pointer_file_bytes["oid"]
        if oid.startswith("sha256:"):
            oid = oid[7:]  # len("sha256:")
        return oid

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


def copy_file_to_local_lfs_cache(
    repo, source_path, conversion_func=None, oid_and_size=None
):
    """
    Given the path to a file, copies it to the appropriate location in the local LFS cache based on its sha256 hash.
    Optionally takes a conversion function which can convert the file while copying it - this saves us doing an extra
    copy after the convert operation, if we just write the converted version to where we would copy it.
    Optionally takes the oid and size of the source, if this is known, to avoid recomputing it.
    """

    lfs_tmp_path = repo.gitdir_path / "lfs" / "objects" / "tmp"
    lfs_tmp_path.mkdir(parents=True, exist_ok=True)

    tmp_object_path = lfs_tmp_path / str(uuid.uuid4())
    if conversion_func is not None:
        conversion_func(source_path, tmp_object_path)
    else:
        try:
            reflink(source_path, tmp_object_path)
        except (ReflinkImpossibleError, NotImplementedError):
            if oid_and_size:
                shutil.copy(source_path, tmp_object_path)
            else:
                # We can find the hash while copying in this case.
                # TODO - check if this is actually any faster.
                oid_and_size = get_hash_and_size_of_file_while_copying(
                    source_path, tmp_object_path
                )

    if not oid_and_size:
        oid_and_size = get_hash_and_size_of_file(tmp_object_path)
    oid, size = oid_and_size

    actual_object_path = get_local_path_from_lfs_hash(repo, oid)

    # Move tmp_object_path to actual_object_path in a robust way -
    # check to see if its already there:
    if actual_object_path.is_file():
        if actual_object_path.stat().st_size != size:
            actual_object_path.unlink()

    if not actual_object_path.is_file():
        actual_object_path.parents[0].mkdir(parents=True, exist_ok=True)
        tmp_object_path.rename(actual_object_path)

    if not oid.startswith("sha256:"):
        oid = "sha256:" + oid

    return {
        "version": GIT_LFS_SPEC_V1,
        "oid": oid,
        "size": size,
    }
