import base64
import hashlib
import json
import logging
import struct

from msgspec import msgpack as _msgpack

from kart.geometry import Geometry

L = logging.getLogger("kart.serialise_util")

# Extension code for Geometry objects.
_EXTENSION_G = ord("G")


def _msg_pack_default(obj):
    if isinstance(obj, Geometry):
        return _msgpack.Ext(_EXTENSION_G, bytes(obj))
    if isinstance(obj, tuple):
        return list(obj)
    raise NotImplementedError


def _msg_unpack_ext_hook(code: int, data: memoryview):
    if code == _EXTENSION_G:
        return Geometry.of(data)
    else:
        L.warning("Unexpected msgpack extension: %d", code)
        return _msgpack.Ext(code, data)


_msgpack_decoder = _msgpack.Decoder(ext_hook=_msg_unpack_ext_hook)
_msgpack_encoder = _msgpack.Encoder(enc_hook=_msg_pack_default)


msg_pack = _msgpack_encoder.encode
msg_unpack = _msgpack_decoder.decode


# json_pack and json_unpack have the same signature and capabilities as msg_pack and msg_unpack,
# but their storage format is less compact and more human-readable.
def json_pack(data):
    """data (any type) -> bytes"""
    return json.dumps(data).encode("utf8")


def json_unpack(bytestring):
    """bytes -> data (any type)"""
    # The input encoding should be UTF-8, UTF-16 or UTF-32.
    return json.loads(bytestring)


def b64encode_str(bytestring):
    """bytes -> urlsafe str"""
    return base64.urlsafe_b64encode(bytestring).decode("ascii")


def b64decode_str(b64_str):
    """urlsafe str -> bytes"""
    if b64_str.startswith("base64:"):
        b64_str = b64_str[7:]  # len("base64:") = 7
    return base64.urlsafe_b64decode(b64_str)


def sha256(*data):
    """*data (str or bytes) -> sha256. Irreversible."""
    h = hashlib.sha256()
    for d in data:
        h.update(ensure_bytes(d))
    return h


def b64hash(*data):
    """*data (str or bytes) -> base64 str. Irreversible."""
    # We only return 160 bits of the hash, same as git hashes - more is overkill.
    return b64encode_str(sha256(*data).digest()[:20])


def hexhash(*data):
    """*data (str or bytes) -> hex str. Irreversible."""
    # We only return 160 bits of the hash, same as git hashes - more is overkill.
    return sha256(*data).hexdigest()[:40]


def uint32hash(*data):
    b = sha256(*data).digest()[:4]
    return struct.unpack(">I", b)[0]


def ensure_bytes(data):
    """data (str or bytes) -> bytes. Utf-8."""
    if isinstance(data, str):
        return data.encode("utf8")
    return data


def ensure_text(data):
    """data (str or bytes) -> str. Utf-8."""
    if isinstance(data, bytes):
        return data.decode("utf8")
    return data
