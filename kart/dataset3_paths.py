import binascii

from .exceptions import NotYetImplemented
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

# Return value indicating the caller should sample all trees within a particular tree - so, the current directory, "." -
# as opposed to sampling a particular tree relative to the current directory, ie "A/B/C/D"
SAMPLE_ALL_TREES = "."


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
    Takes a string, decodes it using base64 as an integer.
    Reverse of b64encode_int
    """
    assert len(s)

    result = 0
    try:
        for i, byt in enumerate(s.encode("ascii")[::-1]):
            val = _BASE64_URLSAFE_ALPHABET_DECODE_MAP[byt]
            result += val * 64 ** i
    except KeyError:
        raise binascii.Error('Non-base64 digit found')
    if result > 2 ** 29:
        result -= 2 ** 30
    return result


class PathEncoder:
    """
    A system for transforming a primary key to a path (which can be transformed back into a primary key again).
    The path structure attempts to spread out features so that every tree has at most a relatively small number
    of children, so that neighbouring primary keys also tend to be neighbours in trees, and so that small datasets
    have few trees. This is achieved by placing features in a nested structure of a few levels of trees, such that
    a low branch factor can still branch out into millions of features.

    - A dataset with a single integer primary key will get encoded with 4 tree levels and a branch factor of 64 at each
      level. Sequential PKs tend to end up in the same tree.
    - Anything else (multiple PK fields, strings, etc) gets the PKs values hashed first, before encoding into a similar
      64-branch 4-level structure.

    Before 0.10, Kart used a two-level, 256-branch structure and hashed *all* PKs first. However, this system proved
    costly for massive repos (roughly, repos with greater than 16 milllion features), resulting in significant
    repository bloat. Existing repos created before this change (Datasets V2 repos) continue to use the old system,
    which Kart continues to support.
    """

    PATH_STRUCTURE_ITEM = "path-structure.json"

    @staticmethod
    def get(*, scheme, **kwargs):
        if scheme == "msgpack/hash":
            return MsgpackHashPathEncoder(**kwargs)
        elif scheme == "int":
            return IntPathEncoder(**kwargs)
        else:
            raise NotYetImplemented(
                f"Sorry, this repo uses {scheme!r} feature path scheme, which isn't supported by this version of Kart"
            )

    @property
    def max_trees(self):
        """
        Returns the number of trees this structure can possibly store.
        """
        return self.branches ** self.levels

    def _encode_file_name_from_packed_pk(self, packed_pk):
        return b64encode_str(packed_pk)

    def encode_filename(self, pk_values):
        return self._encode_file_name_from_packed_pk(msg_pack(pk_values))

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
            base64_alphabet = _BASE64_URLSAFE_ALPHABET.decode("ascii")
            for i in range(64):
                yield base64_alphabet[i]


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
            raise NotYetImplemented(
                f"Sorry, this repo uses {encoding!r} path encoding, which isn't supported by this version of Kart"
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
        parts.append(self._encode_file_name_from_packed_pk(packed_pk))
        return "/".join(parts)

    def sample_subtrees(self, num_trees, *, max_tree_id=None):
        """
        Yields a sample set of outermost trees such as might contain features for sampling,
        for feature count estimation. Eg: ["A/A/D/E", "A/A/H/J", ...]
        Yields num_trees trees, max_tree_id is ignored since features are distributed uniformly
        and randomly all over the structure.
        """
        total_subtrees = self.max_trees
        if num_trees >= total_subtrees:
            yield SAMPLE_ALL_TREES
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
            raise NotYetImplemented(
                f"Sorry, this repo uses {encoding!r} path encoding, which isn't supported by this version of Kart"
            )
        if levels > 5:
            raise NotYetImplemented(
                f"Sorry, this repo uses {levels!r} path levels, which isn't supported by this version of Kart"
            )

        self.scheme = "int"
        self.branches = branches
        self.encoding = encoding
        self.levels = levels
        self._mod_value = self.max_trees

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
        all_names = a.keys() | b.keys()
        return {k: (a.get(k), b.get(k)) for k in all_names if a.get(k) != b.get(k)}

    def max_tree_id(self, repo, base_feature_tree, target_feature_tree):
        """
        Looks at a few trees to determine the maximum integer ID of the trees in the given diff.
        Used as an upper bound for feature count sampling.

        e.g if the only tree is 'A/A/A/A', returns 0
        """
        max_tree_path = self._max_feature_tree_path(
            repo, base_feature_tree, target_feature_tree
        )
        return b64decode_int("".join(max_tree_path.split("/")))

    def _max_feature_tree_path(
        self, repo, base_feature_tree, target_feature_tree, *, depth=0
    ):
        """
        Returns the path of the tree containing the greatest PK,
        relative to the given feature tree. Recurses to self.levels.
        """
        base_feature_tree = base_feature_tree or repo.empty_tree
        target_feature_tree = target_feature_tree or repo.empty_tree

        if base_feature_tree == target_feature_tree:
            return None

        diff = self._nonrecursive_diff(base_feature_tree, target_feature_tree)
        # Diff is always non-empty since the trees must differ.
        max_path = max(diff.keys())
        if depth == self.levels - 1:
            return max_path
        else:
            a, b = diff[max_path]
            return (
                f"{max_path}/{self._max_feature_tree_path(repo, a, b, depth=depth + 1)}"
            )

    def sample_subtrees(self, num_trees, *, max_tree_id=None):
        """
        Yields a sample set of outermost trees such as might contain features for sampling,
        for feature count estimation. Eg: ["A/A/D/E", "A/A/H/J", ...]
        Yields num_trees trees. All returned trees will be *before* the max_tree_id supplied,
        since primary keys used are generally clustered at the start of the path structure.
        """
        if max_tree_id is None:
            total_subtrees = self.branches ** self.levels
        else:
            total_subtrees = max_tree_id
        if num_trees >= total_subtrees:
            yield SAMPLE_ALL_TREES
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
