from .exceptions import InvalidOperation
from .serialise_util import (
    msg_pack,
    b64encode_str,
    b64hash,
    hexhash,
)


class PathEncoder:
    @staticmethod
    def get(*, scheme, **kwargs):
        if scheme == "msgpack/hash":
            return MsgpackHashPathEncoder(**kwargs)
        elif scheme == "int":
            return IntPathEncoder(**kwargs)
        else:
            raise InvalidOperation(
                f"This repo uses {scheme!r} feature path scheme, which isn't supported by this version of Kart"
            )

    def encode_filename(self, pk_values):
        packed_pk = msg_pack(pk_values)
        return b64encode_str(packed_pk)


class MsgpackHashPathEncoder(PathEncoder):
    """
    Encodes paths by msgpacking and hashing the primary key value(s),
    then evenly distributing the features across a hierarchy of trees,
    based on the hash value.
    """

    def __init__(self, *, levels: int, branches: int, encoding: str):
        if encoding == "hex":
            assert branches in (16, 256)
            self._tree_stride = 2 if branches == 256 else 1
            self._hash = hexhash
        elif encoding == "base64":
            assert branches == 64
            self._tree_stride = 1
            self._hash = b64hash
        else:
            raise InvalidOperation(
                f"This repo uses {encoding!r} path encoding, which isn't supported by this version of Kart"
            )

        self.branches = branches
        self.encoding = encoding
        self.levels = levels

    def encode_pks_to_path(self, pk_values):
        """
        Given some pk values, returns the path the feature should be written to.
        pk_values should be a list or tuple of pk values.
        """
        packed_pk = msg_pack(pk_values)
        pk_hash = self._hash(packed_pk)

        parts = [
            pk_hash[i * self._tree_stride : (i + 1) * self._tree_stride]
            for i in range(self.levels)
        ]
        parts.append(self.encode_filename(pk_values))
        return "/".join(parts)


# https://datatracker.ietf.org/doc/html/rfc3548.html#section-4
_BASE64_URLSAFE_ALPHABET = (
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)
MAX_B64_INT = 2 ** 29


def b64encode_int(integer):
    """
    Encodes an integer to a string using exactly five bytes from the urlsafe base64 alphabet.
    Raises ValueError if the integer is outside the valid range.
    """
    if not (-MAX_B64_INT < integer <= MAX_B64_INT):
        raise ValueError(
            f"{integer} should be between {-MAX_B64_INT +1} and {MAX_B64_INT}"
        )

    result = bytearray(bytes(5))
    for index in range(4, -1, -1):
        integer, mod = divmod(integer, 64)
        result[index] = _BASE64_URLSAFE_ALPHABET[mod]
    return result.decode("ascii")


class IntPathEncoder(PathEncoder):
    """
    Encodes paths for integers by just using a modulus of the branch factor.
    This provides much better repo packing characteristics than the hashing encoders,
    but can only be used for (single-field) integer PKs.
    """

    def __init__(self, *, levels: int, branches: int, encoding: str):
        if encoding == "base64":
            assert branches == 64
        else:
            raise InvalidOperation(
                f"This repo uses {encoding!r} path encoding, which isn't supported by this version of Kart"
            )
        if levels > 5:
            raise InvalidOperation(
                f"This repo uses {levels!r} path levels, which isn't supported by this version of Kart"
            )
        self.branches = branches
        self._mod_value = branches ** levels
        self.encoding = encoding
        self.levels = levels

    def encode_pks_to_path(self, pk_values):
        assert len(pk_values) == 1
        pk = pk_values[0]
        encoded = b64encode_int((pk // self.branches) % self._mod_value)
        # take chars from the end. so "ABCDE" with two levels becomes "DE"
        parts = [encoded[i] for i in range(-self.levels, 0)]
        parts.append(self.encode_filename(pk_values))
        return "/".join(parts)
