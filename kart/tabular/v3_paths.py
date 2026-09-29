import functools
import logging
import math
import random
from collections import defaultdict

import pygit2

from kart.exceptions import NotYetImplemented
from kart.serialise_util import b64encode_str, b64hash, hexhash, msg_pack
from kart.utils import chunk

L = logging.getLogger("kart.tabular.v3_paths")

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
    if base**group_length != branches:
        raise ValueError(
            f"Invalid path specification: {encoding} encoding and {branches} branches are incompatible - "
            f"branches must be a power of {base}, eg: {valid_branch_counts_str(encoding)}"
        )
    return group_length


def valid_branch_counts(encoding, max_count=4096):
    """Yields the branch-counts that are compatible with the given encoding, in increasing order."""
    base = len(alphabet_for_encoding(encoding))
    branches = base
    while branches <= max_count:
        yield branches
        branches *= base


def valid_branch_counts_str(encoding):
    return ", ".join(str(b) for b in valid_branch_counts(encoding))


def alphabet_for_encoding(encoding):
    if encoding == "hex":
        return _LOWERCASE_HEX_ALPHABET
    elif encoding == "base64":
        return _BASE64_URLSAFE_ALPHABET
    raise NotYetImplemented(
        f"Sorry, this repo uses {encoding!r} path encoding, which isn't supported by this version of Kart"
    )


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

    LEGACY_ENCODER: "PathEncoder"
    INT_PK_ENCODER: "PathEncoder"
    GENERAL_ENCODER: "PathEncoder"

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

    def with_overrides(self, overrides):
        """
        Returns a PathEncoder that is the same as this one, except for the given overrides.
        overrides is a dict which may contain any of "encoding", "levels" and "branches" -
        entries that are missing or None are left as they are in this encoder.
        The scheme is never overridden, since it is determined by the type of the primary key.
        Returns self if there is nothing to override.
        """
        if not overrides:
            return self
        params = self.to_dict()
        changes = {
            k: v
            for k, v in overrides.items()
            if k in ("encoding", "levels", "branches") and v is not None
        }
        if not changes or all(params[k] == v for k, v in changes.items()):
            return self
        params.update(changes)
        return PathEncoder.get(**params)

    def __init__(self, *, scheme: str, levels: int, branches: int, encoding: str):
        self.scheme = scheme
        self.levels = levels
        self.branches = branches
        self.encoding = encoding

        self.alphabet = alphabet_for_encoding(encoding)
        self._hash = hexhash if encoding == "hex" else b64hash

        if not isinstance(levels, int) or levels < 1:
            raise ValueError(
                f"Invalid path specification: {levels} levels - levels must be a positive integer"
            )

        base = len(self.alphabet)
        self.group_length = _calculate_group_length(encoding, base, branches)

        self.max_trees = self.branches**self.levels

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

    def __eq__(self, other):
        return isinstance(other, PathEncoder) and self.to_dict() == other.to_dict()

    def __hash__(self):
        return hash(tuple(sorted(self.to_dict().items())))

    def __repr__(self):
        return f"<{type(self).__name__}: {self.encoding} {self.levels}x{self.branches}>"

    def _nonrecursive_diff(self, tree_a, tree_b):
        """
        Returns a dict mapping names to OIDs which differ between the trees.
        (either the key is present in both, and the OID is different,
        or the key is only present in one of the trees)
        """
        a = {obj.name: obj for obj in tree_a} if tree_a else {}
        b = {obj.name: obj for obj in tree_b} if tree_b else {}
        all_names = sorted(list(set(a.keys() | b.keys())))

        return {k: (a.get(k), b.get(k)) for k in all_names if a.get(k) != b.get(k)}


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

    def _num_expected_distributed_tree_blobs(self, num_samples, branch_factor):
        """
        Returns the expected number of children in a tree of the given size.
        """
        # https://docs.google.com/document/d/11CeJKbiNQoLmhDcYIM68cJSA_nKBHW7kYVybh2N-Lww/edit#heading=h.7z95y6hc62gn
        return math.log(1 - num_samples / branch_factor) / math.log(
            1 - 1 / branch_factor
        )

    def _recursive_diff_estimate(
        self, tree1, tree2, branch_count, total_samples_to_take
    ):
        """
        Samples some subtrees of the given two trees, and returns an estimate of the number
        of features that are expected to differ.

        Takes advantage of the fact that features are evenly distributed amongst trees
        - all we need to do is drill down from the root tree until the number of
        children of the inspected subtree is much less than the branch factor.
        Then we just sample a few trees at that level, take the average number
        of subtrees, and multiply by the appropriate exponent of the branch factor
        to get a total number of blobs.
        """
        diff = self._nonrecursive_diff(tree1, tree2)

        diff_size = len(diff)
        if diff_size < branch_count / 2:
            estimated_blobs = self._num_expected_distributed_tree_blobs(
                diff_size, branch_count
            )
            L.debug(
                f"Found {diff_size} diffs for an estimate of {estimated_blobs} blobs."
            )
            return estimated_blobs, 1

        L.debug(f"Found {diff_size} diffs, checking next level:")

        total_subsample_size = 0
        total_subsamples_taken = 0
        total_samples_taken = 0
        for tree1, tree2 in diff.values():
            if isinstance(tree1, pygit2.Blob) or isinstance(tree2, pygit2.Blob):
                subsample_size = 1
                samples_taken = 1
            else:
                subsample_size, samples_taken = self._recursive_diff_estimate(
                    tree1, tree2, branch_count, total_samples_to_take
                )
            total_subsample_size += subsample_size
            total_subsamples_taken += 1
            total_samples_taken += samples_taken
            if total_samples_taken >= total_samples_to_take:
                break

        return (
            1.0 * diff_size * total_subsample_size / total_subsamples_taken,
            total_samples_taken,
        )

    def diff_estimate(self, tree1, tree2, branch_count, total_samples_to_take):
        diff_count, samples_taken = self._recursive_diff_estimate(
            tree1, tree2, branch_count, total_samples_to_take
        )
        return int(round(diff_count))


class IntPathEncoder(PathEncoder):
    """
    Encodes paths for integers by just using a modulus of the branch factor.
    This provides much better repo packing characteristics than the hashing encoders,
    but can only be used for (single-field) integer PKs.
    """

    DISTRIBUTED_FEATURES = False

    def encode_pks_to_path(self, pk_values):
        if len(pk_values) != 1:
            raise TypeError("IntPathEncoder can only encode a single integer value")
        if not isinstance(pk_values[0], int):
            raise TypeError("IntPathEncoder can only encode a single integer value")

        pk = int(pk_values[0])
        tree_path = self._path_int_encoder.encode_int(
            (pk // self.branches) % self.max_trees
        )
        filename = self.encode_filename(pk_values)
        return f"{tree_path}/{filename}"

    def _recursive_depth_first_diff_estimate(
        self, tree1, tree2, *, path, paths_fully_explored, diffs_by_path, rand
    ):
        """
        Dives as deep as possible into the diff for the given trees, returning one
        feature-count sample.

        Dives into a random branch at each level, but without replacement;
        any bottom-level branch that has already been sampled will be avoided.

        Returns 0 if all branches at the current level have already been sampled.
        """
        try:
            diff = diffs_by_path[path]
        except KeyError:
            diff = self._nonrecursive_diff(tree1, tree2)
            diffs_by_path[path] = diff

        if not diff:
            # This can happen after a delete-all-features commit - at some level there may be an empty tree.
            # Since tree1=None and tree2=EMPTY_TREE, we'll get here (tree1 != tree2) but diff will actually be empty.
            paths_fully_explored.add(path)
            return 0

        diff_items = list(diff.items())

        rand.shuffle(diff_items)
        child1, child2 = diff_items[0][1]
        if isinstance(child1, pygit2.Blob) or isinstance(child2, pygit2.Blob):
            # we're at the bottom level
            paths_fully_explored.add(path)
            return len(diff)

        for name, (child1, child2) in diff_items:
            child_path = f"{path}/{name}"
            if child_path in paths_fully_explored:
                continue
            num_features = self._recursive_depth_first_diff_estimate(
                child1,
                child2,
                path=child_path,
                paths_fully_explored=paths_fully_explored,
                diffs_by_path=diffs_by_path,
                rand=rand,
            )
            if not num_features:
                # no (new) features found in a subtree. try another subtree
                continue
            else:
                # by recursing into a subtree, we found some new features.
                # return these to the root level as a sample.
                return num_features
        else:
            # this path was already fully sampled by a previous call to this function,
            # so we haven't sampled any new trees.
            paths_fully_explored.add(path)
            return 0

    def diff_estimate(
        self,
        tree1,
        tree2,
        branch_count,
        total_samples_to_take,
    ):
        """
        Samples some subtrees of the given two trees, and returns an estimate of the number
        of features that are expected to differ.

        This is a lot harder than the equivalent method on MsgpackHashPathEncoder
        because there is no reliable distribution of features across trees.
        """
        # start with a deterministic random state.
        # Otherwise we'll get unreproducible results.
        rand = random.Random(0)

        paths_fully_explored = set()
        diffs_by_path = {}
        samples = []
        for i in range(total_samples_to_take):
            num_features = self._recursive_depth_first_diff_estimate(
                tree1,
                tree2,
                path="",
                paths_fully_explored=paths_fully_explored,
                diffs_by_path=diffs_by_path,
                rand=rand,
            )
            if num_features:
                samples.append(num_features)
            else:
                # we sampled the entire diff, so this will be an exact result
                return sum(samples)

        num_samples = len(samples)
        max_level = max(x.count("/") for x in diffs_by_path.keys())

        # keyed by level, the total number of trees encountered
        # (we didn't sample all of them, but we know they exist)
        # doesn't include the deepest level.
        # e.g. {0: 1, 1: 1, 2: 1, 3: 34}
        trees_seen_at_level = defaultdict(int)

        # keyed by level, the total number of trees we sampled
        # (not including the deepest level)
        # e.g. {0: 1, 1: 1, 2: 1, 3: 16}
        trees_sampled_at_level = defaultdict(int)
        for k, v in diffs_by_path.items():
            level = k.count("/")
            if level < max_level:
                trees_seen_at_level[level] += len(v)
            if level:
                trees_sampled_at_level[level - 1] += 1
        assert trees_sampled_at_level[max_level - 1] == num_samples

        # now we have sampled a bunch of deepest-level trees to figure out how many
        # blobs are in them, we can work backwards. how many trees do we expect to exist?
        num_features = sum(samples)
        for level in trees_sampled_at_level.keys():
            # if we only sampled half of the trees at this level, then multiply the
            # current total feature count (FC) by 2.
            # Even though trees_seen_at_level only has the *known* trees,
            # doing this repeatedly until we get to the root level will still result
            # in a total FC estimate which approaches the actual FC.
            level_multiplier = (
                trees_seen_at_level[level] / trees_sampled_at_level[level]
            )
            num_features *= level_multiplier

        return int(round(num_features))

    def find_start_of_unassigned_range(self, dataset):
        """
        Looks at a few trees to determine where new features can be inserted (returns the start
        of a large empty range that won't collide with any existing features, this will usually
        be the number one higher than ALL existing PK values).
        """

        # NOTE - currently partial clones are missing only blobs, not trees, so we at least
        # know the names of all the blobs, which is sufficient for this code. If we ever
        # implement partial clones with missing trees, we will need to fetch some trees here.

        feature_tree = dataset.feature_tree
        if not feature_tree:
            return 0
        best_empty_range_size = empty_range_size = 0
        best_last_seen = last_seen = None
        for t in self.tree_names():
            if t in feature_tree:
                if empty_range_size > best_empty_range_size:
                    best_empty_range_size = empty_range_size
                    best_last_seen = last_seen
                last_seen = t
                empty_range_size = 0
            else:
                empty_range_size += 1

        if empty_range_size > best_empty_range_size:
            best_last_seen = last_seen

        if best_last_seen is None:
            # Every possible tree is already occupied, which means the PK range has wrapped
            # around (or is exactly full) - so the gaps between trees tell us nothing about
            # where the largest PK is. This can happen for a dataset with more than
            # branches ** (levels + 1) features. Fall back to finding the largest PK the
            # slow-but-certain way - if we guessed wrong here, we would hand out PKs that
            # are already in use.
            return self._find_max_pk_by_scanning(dataset) + 1

        current_tree = feature_tree[best_last_seen]

        # Tree names are self.group_length characters long - not necessarily a single character -
        # so we search through all the possible tree names, not through the alphabet.
        reversed_tree_names = list(self.tree_names())[::-1]

        while any(current_tree) and next(iter(current_tree)).type_str == "tree":
            max_child = next(
                current_tree[t] for t in reversed_tree_names if t in current_tree
            )
            current_tree = max_child

        if not any(current_tree):
            return 0

        max_pk = max(dataset.decode_path_to_1pk(c.name) for c in current_tree)
        return max_pk + 1

    def _find_max_pk_by_scanning(self, dataset):
        """
        Returns the largest PK in the given dataset by looking at every feature path,
        or -1 if it has no features. Only used as a fallback when the structure of the
        feature tree doesn't tell us where the largest PK is.
        """
        L.debug(
            "All %d trees are occupied in %s - scanning all features to find the largest PK",
            self.branches,
            dataset.path,
        )
        max_pk = -1
        for blob in dataset.feature_blobs():
            max_pk = max(max_pk, dataset.decode_path_to_1pk(blob.name))
        return max_pk


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

# The encoders that a dataset can end up with if the user doesn't specify a path-structure.
_DEFAULT_ENCODERS = (PathEncoder.INT_PK_ENCODER, PathEncoder.GENERAL_ENCODER)

# The encodings / branch-counts that a dataset can end up with if the user doesn't specify one.
DEFAULT_ENCODINGS = frozenset(e.encoding for e in _DEFAULT_ENCODERS)
DEFAULT_BRANCH_COUNTS = frozenset(e.branches for e in _DEFAULT_ENCODERS)

MAX_LEVELS = 8


def validate_path_structure_overrides(*, encoding=None, levels=None, branches=None):
    """
    Given the path-structure parameters the user has asked for - any or all of which may be None,
    meaning "use the default" - returns them as a dict of overrides suitable for
    PathEncoder.with_overrides, or None if the user didn't ask for anything.
    Raises ValueError with a user-facing message if the given parameters can't be used.
    """
    overrides = {
        k: v
        for k, v in (
            ("encoding", encoding),
            ("levels", levels),
            ("branches", branches),
        )
        if v is not None
    }
    if not overrides:
        return None

    if encoding is not None and encoding not in ("hex", "base64"):
        raise ValueError(
            f"Invalid path encoding {encoding!r} - must be one of: hex, base64"
        )

    if levels is not None and levels < 1:
        raise ValueError(f"Invalid number of path levels {levels} - must be at least 1")
    if levels is not None and levels > MAX_LEVELS:
        raise ValueError(
            f"Invalid number of path levels {levels} - must be at most {MAX_LEVELS}"
        )

    # The encoding and the branch-count have to be compatible with each other. Either of them
    # may be unspecified, in which case it keeps whichever value the default encoder for this
    # dataset turns out to have - so an unspecified value has to work with all of the defaults.
    encodings = [encoding] if encoding is not None else sorted(DEFAULT_ENCODINGS)
    branch_counts = (
        [branches] if branches is not None else sorted(DEFAULT_BRANCH_COUNTS)
    )
    for e in encodings:
        valid = set(valid_branch_counts(e))
        for b in branch_counts:
            if b in valid:
                continue
            if branches is not None:
                raise ValueError(
                    f"Invalid number of path branches {b} for {e} path encoding - "
                    f"must be one of: {valid_branch_counts_str(e)}"
                )
            # The user asked for an encoding that the default branch-count doesn't work with,
            # so they need to supply a branch-count too.
            raise ValueError(
                f"{e} path encoding requires --path-branches to be specified as well - "
                f"must be one of: {valid_branch_counts_str(e)}"
            )

    return overrides
