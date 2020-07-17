from collections import namedtuple
import logging
import re
import sys
from pathlib import Path

import click

from .diff_output import *  # noqa - used from globals()
from .exceptions import (
    InvalidOperation,
    NotFound,
    NO_WORKING_COPY,
    UNCATEGORIZED_ERROR,
)
from .filter_util import build_feature_filter, UNFILTERED
from .repo_files import RepoState
from .structure import RepositoryStructure


L = logging.getLogger("sno.diff")


class Conflict(Exception):
    pass


class KeyValue(namedtuple("KeyValue", ("key", "value"))):
    """A key-value pair. A delta is made of two of these - one old, one new."""

    @staticmethod
    def of(obj):
        """Ensures that the given object is a KeyValue, or None."""
        if isinstance(obj, (KeyValue, type(None))):
            return obj
        elif isinstance(obj, tuple):
            return KeyValue(*obj)
        raise ValueError(f"Expected (key, value) tuple - got f{type(obj)}")


class Delta(namedtuple("Delta", ("old", "new"))):
    """
    An object changes from old to new. Either old or new can be None, for insert or delete operations.
    When present, old and new are both key-value pairs.
    The key identifies which object changed (so, should be a filename / address / primary key),
    and the value is the changed object's entire contents.
    If the old_key is different to the new_key, this means the object moved in this delta, ie a rename operation.
    Deltas can be concatenated together, if they refer to the same object - eg an delete + insert = update (usually).
    Deltas can be inverted, which just means old and new are swapped.
    """

    def __new__(cls, old, new):
        return super().__new__(cls, KeyValue.of(old), KeyValue.of(new))

    def __init__(self, old, new):
        super().__init__()
        if self.old is None and self.new is None:
            raise ValueError("Empty Delta")
        elif self.old is None:
            self.type = "insert"
        elif self.new is None:
            self.type = "delete"
        else:
            self.type = "update"

    @staticmethod
    def insert(new):
        return Delta(None, new)

    @staticmethod
    def update(old, new):
        return Delta(old, new)

    @staticmethod
    def maybe_update(old, new):
        return Delta(old, new) if old != new else None

    @staticmethod
    def delete(old):
        return Delta(old, None)

    def __invert__(self):
        return Delta(self.new, self.old)

    @property
    def old_key(self):
        return self.old.key if self.old is not None else None

    @property
    def new_key(self):
        return self.new.key if self.new is not None else None

    @property
    def key(self):
        # To be stored in a Diff, a Delta needs a single key.
        # This mostly works, but isn't perfect when renames are involved.
        return self.old_key or self.new_key

    def __add__(self, other):
        """Concatenate this delta with the subsequent delta, return the result as a single delta."""
        # Note: this method assumes that the deltas being concatenated are related,
        # ie that self.new == other.old. Don't try to concatenate arbitrary deltas together.

        if self.type == "insert":
            # ins + ins -> Conflict
            # ins + upd -> ins
            # ins + del -> noop
            if other.type == "insert":
                raise Conflict()
            elif other.type == "update":
                return Delta.insert(other.new)
            elif other.type == "delete":
                return None

        elif self.type == "update":
            # upd + ins -> Conflict
            # upd + upd -> upd?
            # upd + del -> del
            if other.type == "insert":
                raise Conflict()
            elif other.type == "update":
                return Delta.maybe_update(self.old, other.new)
            elif other.type == "delete":
                return Delta.delete(self.old)

        elif self.type == "delete":
            # del + ins -> upd?
            # del + del -> Conflict
            # del + upd -> Conflict
            if other.type == "insert":
                return Delta.maybe_update(self.old, other.new)
            else:
                raise Conflict()


class Diff:
    """
    A Diff is a set of Deltas, or a set of Diffs. A Diff's children are stored by key.
    When two diffs are concatenated, all their children with matching keys are recursively concatenated.
    The children of a Diff are responsible for knowing their own keys - this is a requirement
    for Deltas in particular, since a Delta needs to be able to change key when inverted.
    So, if a Diff will be added to a parent Diff, it needs to know its own key - the root Diff can be unnamed.
    """

    def __init__(self, self_key=None, children=()):
        try:
            hash(self_key)
        except Exception:
            raise ValueError(f"Bad key for Diff object: {self_key}")

        self.self_key = self_key
        self.child_type = None
        self.children = {}
        for child in children:
            self.add_child(child)

    def __contains__(self, child_key):
        return child_key in self.children

    def __getitem__(self, child_key):
        return self.children[child_key]

    def __setitem__(self, key, value):
        raise RuntimeError(
            "Diff doesn't support __setitem__: use Diff.add_child(diff_or_delta)"
        )

    def get(self, child_key):
        return self.children.get(child_key)

    def add_child(self, child):
        if self.child_type is None:
            self.child_type = type(child)
        elif type(child) != self.child_type:
            raise ValueError(
                f"A Diff's children should be all one type - all Diffs or all Deltas. {type(child)} added to {self}"
            )
        if child.key is None:
            raise ValueError(f"Can't add child with no key to parent: {child}")
        self.children[child.key] = child

    def copy(self):
        result = Diff(self.self_key)
        result.children = self.children.copy()
        return result

    @property
    def key(self):
        return self.self_key

    def keys(self):
        return self.children.keys()

    def values(self):
        return self.children.values()

    def items(self):
        return self.children.items()

    def __iter__(self):
        yield from iter(self.children)

    def __eq__(self, other):
        if not isinstance(other, Diff):
            return False
        return self.self_key == other.self_key and self.children == other.children

    def __len__(self):
        return len(self.children)

    def __bool__(self):
        return any(bool(child) for child in self.children.values())

    def __str__(self):
        return f"Diff({self.self_key}, children={{{','.join(str(k) for k in self.children.keys())}}}))"

    __repr__ = __str__

    def __invert__(self):
        return Diff(self.self_key, (~c for c in self.children.values()))

    def __add__(self, other):
        """Concatenate this Diff to the subsequent Diff, by concatenating all children with matching keys."""
        # FIXME: this algorithm isn't perfect when renames are involved.

        result = []
        for child_key in self.children.keys() | other.children.keys():
            lhs = self.children.get(child_key)
            rhs = other.children.get(child_key)
            if lhs and rhs:
                both = lhs + rhs
                if both:
                    result.append(both)
            else:
                result.append(lhs or rhs)
        return Diff(self.self_key, result)

    def __iadd__(self, other):
        self.children = (self + other).children
        return self

    def to_filter(self):
        """
        Returns the set of all keys of all the Deltas that are children of this Diff.
        If the children are Diffs, not Deltas, returns instead dict of {child.key: child.to_filter()}
        """
        if self.child_type is Diff:
            # Any children are Diffs: recursively call to_filter() on them too.
            return {child.key: child.to_filter() for child in self.children.values()}
        else:
            # Any children are Deltas. Create a filter of all their keys:
            result = set()
            for delta in self.children.values():
                if delta.old is not None:
                    result.add(str(delta.old.key))
                if delta.new is not None:
                    result.add(str(delta.new.key))
            return result

    def type_counts(self):
        if self.child_type is Diff:
            # Any children are Diffs: recursively call to_filter() on them too.
            return {child.key: child.type_counts() for child in self.children.values()}
        else:
            # Any children are Deltas. Create a filter of all their keys:
            result = {}
            for delta in self.children.values():
                delta_type = delta.type
                result.setdefault(delta_type, 0)
                result[delta_type] += 1
            # Pluralise type names:
            return {f"{delta_type}s": value for delta_type, value in result.items()}


def get_dataset_diff(
    base_rs, target_rs, working_copy, dataset_path, pk_filter=UNFILTERED
):
    diff = Diff(dataset_path)

    if base_rs != target_rs:
        # diff += base_rs<>target_rs
        base_ds = base_rs.get(dataset_path)
        target_ds = target_rs.get(dataset_path)

        params = {}
        if not base_ds:
            base_ds, target_ds = target_ds, base_ds
            params["reverse"] = True

        diff_cc = base_ds.diff(target_ds, pk_filter=pk_filter, **params)
        L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
        diff += diff_cc

    if working_copy:
        # diff += target_rs<>working_copy
        target_ds = target_rs.get(dataset_path)
        diff_wc = working_copy.diff_db_to_tree(target_ds, pk_filter=pk_filter)
        L.debug(
            "commit<>working_copy diff (%s): %s", dataset_path, repr(diff_wc),
        )
        diff += diff_wc

    return diff


def get_repo_diff(base_rs, target_rs, feature_filter=UNFILTERED):
    """Generates a Diff for every dataset in both RepositoryStructures."""
    all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

    if feature_filter is not UNFILTERED:
        all_datasets = all_datasets.intersection(feature_filter.keys())

    result = Diff()
    for dataset in sorted(all_datasets):
        ds_diff = get_dataset_diff(
            base_rs, target_rs, None, dataset, feature_filter[dataset]
        )
        if ds_diff:
            result.add_child(ds_diff)
    return result


def get_common_ancestor(repo, rs1, rs2):
    for rs in rs1, rs2:
        if not rs.head_commit:
            raise click.UsageError(
                f"The .. operator works on commits, not trees - {rs.id} is a tree. (Perhaps try the ... operator)"
            )
    ancestor_id = repo.merge_base(rs1.id, rs2.id)
    if not ancestor_id:
        raise InvalidOperation(
            "The .. operator tries to find the common ancestor, but no common ancestor was found. Perhaps try the ... operator."
        )
    return RepositoryStructure.lookup(repo, ancestor_id)


def diff_with_writer(
    ctx,
    diff_writer,
    *,
    output_path='-',
    exit_code,
    json_style="pretty",
    commit_spec,
    filters,
):
    """
    Calculates the appropriate diff from the arguments,
    and writes it using the given writer contextmanager.

      ctx: the click context
      diff_writer: One of the `diff_output_*` contextmanager factories.
                   When used as a contextmanager, the diff_writer should yield
                   another callable which accepts (dataset, diff) arguments
                   and writes the output by the time it exits.
      output_path: The output path, or a file-like object, or the string '-' to use stdout.
      exit_code:   If True, the process will exit with code 1 if the diff is non-empty.
      commit_spec: The commit-ref or -refs to diff.
      filters:     Limit the diff to certain datasets or features.
    """
    from .working_copy import WorkingCopy

    try:
        if isinstance(output_path, str) and output_path != "-":
            output_path = Path(output_path).expanduser()

        repo = ctx.obj.get_repo(allowed_states=RepoState.ALL_STATES)

        # Parse <commit> or <commit>...<commit>
        commit_spec = commit_spec or "HEAD"
        commit_parts = re.split(r"(\.{2,3})", commit_spec)

        if len(commit_parts) == 3:
            # Two commits specified - base and target. We diff base<>target.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0] or "HEAD")
            target_rs = RepositoryStructure.lookup(repo, commit_parts[2] or "HEAD")
            if commit_parts[1] == "..":
                # A   C    A...C is A<>C
                #  \ /     A..C  is B<>C
                #   B      (git log semantics)
                base_rs = get_common_ancestor(repo, base_rs, target_rs)
            working_copy = None
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = RepositoryStructure.lookup(repo, commit_parts[0])
            target_rs = RepositoryStructure.lookup(repo, "HEAD")
            working_copy = WorkingCopy.open(repo)
            if not working_copy:
                raise NotFound(
                    "No working copy, use 'checkout'", exit_code=NO_WORKING_COPY
                )
            working_copy.assert_db_tree_match(target_rs.tree)

        # Parse [<dataset>[:pk]...]
        feature_filter = build_feature_filter(filters)

        base_str = base_rs.id
        target_str = "working-copy" if working_copy else target_rs.id
        L.debug('base=%s target=%s', base_str, target_str)

        all_datasets = {ds.path for ds in base_rs} | {ds.path for ds in target_rs}

        if feature_filter is not UNFILTERED:
            all_datasets = all_datasets.intersection(feature_filter.keys())

        writer_params = {
            "repo": repo,
            "base": base_rs,
            "target": target_rs,
            "output_path": output_path,
            "dataset_count": len(all_datasets),
            "json_style": json_style,
        }

        L.debug(
            "base_rs %s == target_rs %s: %s",
            repr(base_rs),
            repr(target_rs),
            base_rs == target_rs,
        )

        num_changes = 0
        with diff_writer(**writer_params) as w:
            for dataset_path in all_datasets:
                diff = get_dataset_diff(
                    base_rs,
                    target_rs,
                    working_copy,
                    dataset_path,
                    feature_filter[dataset_path],
                )
                dataset = base_rs.get(dataset_path) or target_rs.get(dataset_path)
                num_changes += len(diff)
                L.debug("overall diff (%s): %s", dataset_path, repr(diff))
                w(dataset, diff)

    except click.ClickException as e:
        L.debug("Caught ClickException: %s", e)
        if exit_code and e.exit_code == 1:
            e.exit_code = UNCATEGORIZED_ERROR
        raise
    except Exception as e:
        L.debug("Caught non-ClickException: %s", e)
        if exit_code:
            click.secho(f"Error: {e}", fg="red", file=sys.stderr)
            raise SystemExit(UNCATEGORIZED_ERROR) from e
        else:
            raise
    else:
        if exit_code and num_changes:
            sys.exit(1)


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json", "geojson", "quiet", "html"]),
    default="text",
    help=(
        "Output format. 'quiet' disables all output and implies --exit-code.\n"
        "'html' attempts to open a browser unless writing to stdout ( --output=- )"
    ),
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with codes similar to diff(1). That is, it exits with 1 if there were differences and 0 means no differences.",
)
@click.option(
    "--output",
    "output_path",
    help="Output to a specific file/directory instead of stdout.",
    type=click.Path(writable=True, allow_dash=True),
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with -o json or -o geojson",
)
@click.argument("commit_spec", required=False, nargs=1)
@click.argument("filters", nargs=-1)
def diff(ctx, output_format, output_path, exit_code, json_style, commit_spec, filters):
    """
    Show changes between two commits, or between a commit and the working copy.

    COMMIT_SPEC -

    - if not supplied, the default is HEAD, to diff between HEAD and the working copy.

    - if a single ref is supplied: commit-A - diffs between commit-A and the working copy.

    - if supplied with the form: commit-A...commit-B - diffs between commit-A and commit-B.

    - if supplied with the form: commit-A..commit-B - diffs between (the common ancestor of
    commit-A and commit-B) and (commit-B).

    To list only particular conflicts, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """

    diff_writer = globals()[f"diff_output_{output_format}"]
    if output_format == "quiet":
        exit_code = True

    return diff_with_writer(
        ctx,
        diff_writer,
        output_path=output_path,
        exit_code=exit_code,
        json_style=json_style,
        commit_spec=commit_spec,
        filters=filters,
    )
