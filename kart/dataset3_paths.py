import random
import subprocess
from .exceptions import InvalidOperation
from .serialise_util import (
    msg_pack,
    b64encode_str,
    b64hash,
    hexhash,
)


# https://datatracker.ietf.org/doc/html/rfc3548.html#section-4
_BASE64_URLSAFE_ALPHABET = (
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)
_BASE64_URLSAFE_ALPHABET_DECODE_MAP = {
    _BASE64_URLSAFE_ALPHABET[i]: i for i in range(64)
}
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


def b64decode_int(s):
    """
    Takes a 5-character string, decodes it using base64 as an integer.
    Reverse of b64encode_int
    """
    assert len(s) == 5

    result = 0
    for i, byt in enumerate(s.encode()):
        val = _BASE64_URLSAFE_ALPHABET_DECODE_MAP[byt]
        result += val * 64 ** (4 - i)
    if result > 2 ** 29:
        result -= 2 ** 30
    return result


class PathEncoder:
    """
    A system for encoding a feature's primary key to a particular path.
    Originally hex-hash[0:2]/hex-hash[2:4]/base64-encoded-message-packed-primary-key -
    - that is, 2 levels, and a branch factor of 256 at each level.
    But, this system proved costly for massive repos and the resulting massive trees.
    Now we prefer 4 levels, a branch factor of 64, and clustering similar PKs where possible.
    """

    PATH_STRUCTURE_ITEM = "path-structure.json"
    PATH_STRUCTURE_PATH = "meta/" + PATH_STRUCTURE_ITEM

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

    @property
    def theoretical_max_trees(self):
        """
        Returns the number of trees this structure can possibly store.
        """
        return self.branches ** self.levels

    def encode_filename(self, pk_values):
        packed_pk = msg_pack(pk_values)
        return b64encode_str(packed_pk)

    def encode_path_structure_data(self, relative):
        assert relative is True
        return self.PATH_STRUCTURE_PATH, self.to_dict()

    def to_dict(self):
        return {
            "scheme": self.scheme,
            "branches": self.branches,
            "levels": self.levels,
            "encoding": self.encoding,
        }

    def tree_names(self):
        """Yields all possible tree names according to this encoding + branch-factor."""
        if self.encoding == "hex":
            format_spec = f"{{:0{self._tree_stride}x}}"
            for i in range(self.branches):
                yield format_spec.format(i)
        elif self.encoding == "base64":
            for c in _BASE64_URLSAFE_ALPHABET:
                yield chr(c)


class MsgpackHashPathEncoder(PathEncoder):
    """
    Encodes paths by msgpacking and hashing the primary key value(s),
    then evenly distributing the features across a hierarchy of trees,
    based on the hash value.
    """

    DISTRIBUTED_FEATURES = True

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

        self.scheme = "msgpack/hash"
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

    def sample_subtrees(self, num_trees, *, max_tree_id=None):
        total_subtrees = self.theoretical_max_trees
        if num_trees >= total_subtrees:
            # sample all trees
            yield "."
            return
        stride = total_subtrees / num_trees
        assert stride > 1
        for i in range(num_trees):
            tree_idx = round(i * stride)
            encoded = b64encode_int(tree_idx)
            # take chars from the end. so "ABCDE" with two levels becomes "DE"
            parts = [encoded[i] for i in range(-self.levels, 0)]
            yield "/".join(parts)


class IntPathEncoder(PathEncoder):
    """
    Encodes paths for integers by just using a modulus of the branch factor.
    This provides much better repo packing characteristics than the hashing encoders,
    but can only be used for (single-field) integer PKs.
    """

    DISTRIBUTED_FEATURES = False

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

        self.scheme = "int"
        self.branches = branches
        self.encoding = encoding
        self.levels = levels
        self._mod_value = self.theoretical_max_trees

    def encode_pks_to_path(self, pk_values):
        assert len(pk_values) == 1
        pk = pk_values[0]
        encoded = b64encode_int((pk // self.branches) % self._mod_value)
        # take chars from the end. so "ABCDE" with two levels becomes "DE"
        parts = [encoded[i] for i in range(-self.levels, 0)]
        parts.append(self.encode_filename(pk_values))
        return "/".join(parts)

    def _nonrecursive_diff(self, tree_a, tree_b):
        """
        Returns a dict mapping names to OIDs which differ between the trees.
        (either the key is present in both, and the OID is different,
        or the key is only present in one of the trees)
        """
        a = {obj.name: obj for obj in tree_a}
        b = {obj.name: obj for obj in tree_b}
        all_names = set(a.keys() | b.keys())
        return {k: (a.get(k), b.get(k)) for k in all_names}

    def max_tree_id(self, repo, base_feature_tree, target_feature_tree):
        """
        Looks at a few trees to determine the maximum integer ID of the trees in the given diff.
        Used as an upper bound for feature count sampling.

        e.g if the only tree is 'A/A/A/A', returns 0
        """
        max_tree_path = self._max_feature_tree_path(
            repo, base_feature_tree, target_feature_tree
        )
        return b64decode_int("A" + "".join(max_tree_path.split("/")))

    def _max_feature_tree_path(
        self, repo, base_feature_tree, target_feature_tree, *, depth=0
    ):
        """
        Returns the path of the tree containing the greatest PK,
        relative to the given feature tree.
        """
        if base_feature_tree == target_feature_tree:
            return None

        diff = self._nonrecursive_diff(
            base_feature_tree, target_feature_tree or repo.EMPTY_TREE
        )
        max_path = max(diff.keys())
        if depth == self.levels - 1:
            return max_path
        else:
            a, b = diff[max_path]
            return (
                f"{max_path}/{self._max_feature_tree_path(repo, a, b, depth=depth + 1)}"
            )

    def sample_subtrees(self, num_trees, *, max_tree_id=None):
        if max_tree_id is None:
            total_subtrees = self.branches ** self.levels
        else:
            total_subtrees = max_tree_id
        if num_trees >= total_subtrees:
            # sample all trees
            yield "."
            return

        stride = total_subtrees / num_trees
        assert stride > 1
        for i in range(num_trees):
            tree_idx = round(i * stride)
            encoded = b64encode_int(tree_idx)
            # take chars from the end. so "ABCDE" with two levels becomes "DE"
            parts = [encoded[i] for i in range(-self.levels, 0)]
            yield "/".join(parts)


# The encoder that was previously used for all datasets.
PathEncoder.LEGACY_ENCODER = PathEncoder.get(
    scheme="msgpack/hash", branches=256, levels=2, encoding="hex"
)

# The encoder now used for datasets with a single integer PK value.
PathEncoder.INT_PK_ENCODER = PathEncoder.get(
    scheme="int", branches=64, levels=4, encoding="base64"
)

# The encoder now used for all other datasets.
PathEncoder.GENERAL_ENCODER = PathEncoder.get(
    scheme="msgpack/hash", branches=64, levels=4, encoding="base64"
)
