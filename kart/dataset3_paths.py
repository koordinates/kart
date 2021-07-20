import functools
import math

from .exceptions import NotYetImplemented
from .serialise_util import (
    msg_pack,
    b64encode_str,
    b64hash,
    hexhash,
)
from .utils import chunk


_LOWERCASE_HEX_ALPHABET = "0123456789abcdef"

# https://datatracker.ietf.org/doc/html/rfc3548.html#section-4
_BASE64_URLSAFE_ALPHABET = (
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
)

# Return value indicating the caller should sample all trees within a particular tree - so, the current directory, "." -
# as opposed to sampling a particular tree relative to the current directory, ie "A/B/C/D"
SAMPLE_ALL_TREES = "."


@functools.lru_cache()
def _make_decode_map(alphabet):
    return {char: i for i, char in enumerate(iter(alphabet))}


def _make_format_str(length, separator="", group_length=1):
    placeholders = ("{%d}" % i for i in reversed(range(length)))
    groups = ("".join(c) for c in chunk(placeholders, group_length))
    return separator.join(groups)


def _calculate_group_length(encoding, base, branches):
    group_length = int(math.log(max(branches, 1)) / math.log(max(base, 1)))
    if base ** group_length != branches:
        raise ValueError(
            f"Invalid path specification: {encoding} encoding and {branches} branches are incompatible"
        )
    return group_length


class FixedLengthIntEncoder:
    """
    This class encodes an integer into a fixed length string using a supplied alphabet + base (eg hexadecimal).
    As a trivial example, the alphabet could be "01" (so, base-2 aka binary), and the fixed length could be 3.
    This class would then encode the integers 0 to 7 as 0="000", 1="001", 2="010", ... up to 7="111".
    For inputs of 8 or greater (or less than 0), the outputs simply repeat.

    This is used by IntPathEncoder to spread PKs into a tree structure in a predictable and compact way.
    """

    def __init__(self, alphabet, length, separator="", group_length=1):
        # Separator chars must be distinct from alphabet chars.
        assert not set(alphabet) & set(separator)

        self.alphabet = alphabet
        self.base = len(alphabet)
        self.length = length

        self.format_str = _make_format_str(length, separator, group_length)
        self.decode_map = _make_decode_map(alphabet)

    def encode_int(self, integer):
        def gen():
            for i in range(self.length):
                nonlocal integer
                integer, remainder = divmod(integer, self.base)
                yield self.alphabet[remainder]

        return self.format_str.format(*gen())

    def decode_int(self, string):
        """
        Inverse of encode_int. Skips characters that it doesn't recognise. Always returns the smallest possible
        non-negative answer. (Since encode_int maps all possible integers onto self.base ** self.length output values,
        it means that the output values are repeated for inputs greater than that number, or less than zero.)
        """
        result = 0
        coefficient = 1

        for c in reversed(string):
            value = self.decode_map.get(c)
            if value is not None:
                result += coefficient * value
                coefficient *= self.base

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
            return MsgpackHashPathEncoder(scheme=scheme, **kwargs)
        elif scheme == "int":
            return IntPathEncoder(scheme=scheme, **kwargs)
        else:
            raise NotYetImplemented(
                f"Sorry, this repo uses {scheme!r} feature path scheme, which isn't supported by this version of Kart"
            )

    def __init__(self, *, scheme: str, levels: int, branches: int, encoding: str):
        self.scheme = scheme
        self.levels = levels
        self.branches = branches
        self.encoding = encoding

        if encoding == "hex":
            self.alphabet = _LOWERCASE_HEX_ALPHABET
            self._hash = hexhash
        elif encoding == "base64":
            self.alphabet = _BASE64_URLSAFE_ALPHABET
            self._hash = b64hash
        else:
            raise NotYetImplemented(
                f"Sorry, this repo uses {encoding!r} path encoding, which isn't supported by this version of Kart"
            )

        base = len(self.alphabet)
        self.group_length = _calculate_group_length(encoding, base, branches)

        self.max_trees = self.branches ** self.levels

        self._path_int_encoder = FixedLengthIntEncoder(
            self.alphabet, self.levels * self.group_length, "/", self.group_length
        )
        self._single_tree_int_encoder = FixedLengthIntEncoder(
            self.alphabet, self.group_length
        )

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
        for i in range(self.branches):
            yield self._single_tree_int_encoder.encode_int(i)

    def sample_subtrees(self, num_trees, *, max_tree_id=None):
        """
        Yields a sample set of outermost trees such as might contain features for sampling,
        for feature count estimation. Eg: ["A/A/D/E", "A/A/H/J", ...]
        Yields num_trees trees. All returned trees will be *before* the max_tree_id supplied -
        useful if primary keys are clustered at the low end of the tree structure (see IntPathEncoder).
        """
        if max_tree_id is None:
            total_subtrees = self.max_trees
        else:
            total_subtrees = max_tree_id
        if num_trees >= total_subtrees:
            yield SAMPLE_ALL_TREES
            return

        stride = total_subtrees / num_trees
        assert stride > 1
        for i in range(num_trees):
            tree_idx = round(i * stride)
            yield self._path_int_encoder.encode_int(tree_idx)


class MsgpackHashPathEncoder(PathEncoder):
    """
    Encodes paths by msgpacking and hashing the primary key value(s),
    then evenly distributing the features across a hierarchy of trees,
    based on the hash value.
    """

    DISTRIBUTED_FEATURES = True

    def encode_pks_to_path(self, pk_values):
        """
        Given some pk values, returns the path the feature should be written to.
        pk_values should be a list or tuple of pk values.
        """
        packed_pk = msg_pack(pk_values)
        pk_hash = self._hash(packed_pk)

        parts = [
            pk_hash[i * self.group_length : (i + 1) * self.group_length]
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
        yield from super().sample_subtrees(num_trees, max_tree_id=None)


class IntPathEncoder(PathEncoder):
    """
    Encodes paths for integers by just using a modulus of the branch factor.
    This provides much better repo packing characteristics than the hashing encoders,
    but can only be used for (single-field) integer PKs.
    """

    DISTRIBUTED_FEATURES = False

    def encode_pks_to_path(self, pk_values):
        assert len(pk_values) == 1
        pk = pk_values[0]
        tree_path = self._path_int_encoder.encode_int(
            (pk // self.branches) % self.max_trees
        )
        filename = self.encode_filename(pk_values)
        return f"{tree_path}/{filename}"

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
        return self._path_int_encoder.decode_int(max_tree_path)

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
