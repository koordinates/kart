from collections import namedtuple
import json
import logging
import re
import sys

import click
import pygit2

from .diff_output import repr_row
from .exceptions import InvalidOperation, NotYetImplemented, MERGE_CONFLICT
from .structs import AncestorOursTheirs, CommitWithReference
from .structure import RepositoryStructure


L = logging.getLogger("sno.conflicts")


class ConflictIndex:
    """
    Like a pygit2.Index, but every conflict has a short key independent of its path,
    and the entire index including conflicts can be serialised to a tree.
    Conflicts are easier to modify than in a pygit2.Index (where they are backed by C iterators).
    When serialised to a tree, conflicts will be added in a special .conflicts/ directory.
    """

    # We could use pygit2.IndexEntry everywhere but it has unhelpful __eq__ and __repr__ behaviour.
    # So we have this equivalent struct.
    Entry = namedtuple("Entry", ("path", "id", "mode"))

    def __init__(self, index, serialised=False):
        self.entries = {}
        self.conflicts = {}

        if serialised and index.conflicts:
            raise RuntimeError(
                "pygit2.Index.conflicts should be empty if index has been serialised"
            )

        for entry in index:
            if entry.path.startswith(".conflicts/"):
                if not serialised:
                    raise RuntimeError(
                        ".conflicts/ directory shouldn't exist if index has not been serialised"
                    )
                key, conflict_part = self._deserialise_conflict_part(entry)
                self.conflicts.setdefault(key, AncestorOursTheirs.EMPTY)
                self.add_conflict(key, self.conflicts.get(key) | conflict_part)
            else:
                self.add(entry)

        if index.conflicts:
            for key, conflict3 in enumerate(index.conflicts):
                self.add_conflict(str(key), conflict3)

    def __eq__(self, other):
        if not isinstance(other, ConflictIndex):
            return False
        return self.entries == other.entries and self.conflicts == other.conflicts

    def __repr__(self):
        contents = json.dumps(
            {"entries": self.entries, "conflicts": self.conflicts},
            default=lambda o: str(o),
            indent=2,
        )
        return f'<ConflictIndex {contents}>'

    def add(self, index_entry):
        index_entry = self._ensure_entry(index_entry)
        self.entries[index_entry.path] = index_entry

    def remove(self, path):
        del self.entries[path]

    def __iter__(self):
        return iter(self.entries.values())

    def __getitem__(self, path):
        return self.entries[path]

    def __setitem__(self, path, index_entry):
        assert path == index_entry.path
        self.entries[path] = index_entry

    def add_conflict(self, key, conflict):
        if not isinstance(key, str):
            raise TypeError("conflict key must be str", type(key))
        self.conflicts[key] = self._ensure_conflict(conflict)

    def remove_conflict(self, key):
        del self.conflicts[key]

    def _serialise_conflict(self, key, conflict):
        result = []
        for version, entry in zip(AncestorOursTheirs.NAMES, conflict):
            if not entry:
                continue
            result_path = f".conflicts/{key}/{version}/{entry.path}"
            result.append(self.Entry(result_path, entry.id, entry.mode))
        return result

    _PATTERN = re.compile(
        r"^.conflicts/(?P<key>.+?)/(?P<version>ancestor|ours|theirs)/(?P<path>.+)$"
    )

    def _deserialise_conflict_part(self, index_entry):
        match = self._PATTERN.match(index_entry.path)
        if not match:
            raise RuntimeError(f"Couldn't deserialise conflict: {index_entry.path}")

        key = match.group("key")
        version = match.group("version")
        result_path = match.group("path")
        result_entry = self.Entry(result_path, index_entry.id, index_entry.mode)
        result = AncestorOursTheirs.partial(**{version: result_entry})
        return key, result

    def conflicts_as_entries(self):
        for key, conflict3 in self.conflicts.items():
            for index_entry in self._serialise_conflict(key, conflict3):
                yield index_entry

    @classmethod
    def read(cls, path):
        index = pygit2.Index(path)
        return ConflictIndex(index, serialised=True)

    def write(self, path):
        index = pygit2.Index(path)
        index.clear()

        for e in self.entries.values():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        for e in self.conflicts_as_entries():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        return index.write()

    def _ensure_entry(self, entry):
        if entry is None or isinstance(entry, self.Entry):
            return entry
        elif isinstance(entry, pygit2.IndexEntry):
            return self.Entry(entry.path, entry.id, entry.mode)
        else:
            raise TypeError(
                "Expected entry to be type Entry or IndexEntry", type(entry)
            )

    def _ensure_conflict(self, conflict):
        if isinstance(conflict, AncestorOursTheirs):
            return conflict
        elif isinstance(conflict, tuple):
            return AncestorOursTheirs(
                self._ensure_entry(conflict[0]),
                self._ensure_entry(conflict[1]),
                self._ensure_entry(conflict[2]),
            )
        else:
            raise TypeError(
                "Expected conflict to be type AncestorOursTheirs or tuple",
                type(conflict),
            )


def first_true(iterable):
    """Returns the value from the iterable that is truthy."""
    return next(filter(None, iterable))


class InputMode:
    DEFAULT = 0
    INTERACTIVE = 1
    NO_INPUT = 2


def get_input_mode():
    if sys.stdin.isatty() and sys.stdout.isatty():
        return InputMode.INTERACTIVE
    elif sys.stdin.isatty() and not sys.stdout.isatty():
        return InputMode.NO_INPUT
    elif is_empty_stream(sys.stdin):
        return InputMode.NO_INPUT
    else:
        return InputMode.DEFAULT


def is_empty_stream(stream):
    if stream.seekable():
        pos = stream.tell()
        if stream.read(1) == "":
            return True
        stream.seek(pos)
    return False


def interactive_pause(prompt):
    """Like click.pause() but waits for the Enter key specifically."""
    click.prompt(prompt, prompt_suffix="", default="", show_default=False)


def _safe_get_dataset_for_index_entry(repo_structure, index_entry):
    """Gets the dataset that a pygit2.IndexEntry refers to, or None"""
    if index_entry is None:
        return None
    try:
        return repo_structure.get_for_index_entry(index_entry)
    except KeyError:
        return None


def _safe_get_feature(dataset, pk):
    """Gets the dataset's feature with a particular primary key, or None"""
    try:
        _, feature = dataset.get_feature(pk)
        return feature
    except KeyError:
        return None


def resolve_merge_conflicts(repo, merge_index, ancestor, ours, theirs, dry_run=False):
    """
    Supports resolution of basic merge conflicts, fails in more complex unsupported cases.

    repo - a pygit2.Repository
    merge_index - a pygit2.Index containing the attempted merge and merge conflicts.
    ancestor, ours, theirs - each is a either a pygit2.Commit, or a CommitWithReference.
    """

    # Shortcut used often below
    def aot(generator_or_tuple):
        return AncestorOursTheirs(*generator_or_tuple)

    # We have three versions of lots of objects - ancestor, ours, theirs.
    commit_with_refs3 = AncestorOursTheirs(
        CommitWithReference(ancestor),
        CommitWithReference(ours),
        CommitWithReference(theirs),
    )
    commits3 = aot(cwr.commit for cwr in commit_with_refs3)
    repo_structures3 = aot(
        RepositoryStructure(repo, commit=c.commit) for c in commit_with_refs3
    )

    conflict_pks = {}
    for index_entries3 in merge_index.conflicts:
        datasets3 = aot(
            _safe_get_dataset_for_index_entry(rs, ie)
            for rs, ie in zip(repo_structures3, index_entries3)
        )
        dataset = first_true(datasets3)
        dataset_path = dataset.path
        if None in datasets3:
            for cwr, ds in zip(commit_with_refs3, datasets3):
                presence = "present" if ds is not None else "absent"
                click.echo(f"{cwr}: {dataset_path} is {presence}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts where features are added or removed isn't supported yet"
            )

        pks3 = aot(
            ds.index_entry_to_pk(ie) for ds, ie in zip(datasets3, index_entries3)
        )
        if "META" in pks3:
            click.echo(f"Merge conflict found in metadata for {dataset_path}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts in metadata isn't supported yet"
            )
        pk = first_true(pks3)
        if pks3.count(pk) != 3:
            click.echo(
                f"Merge conflict found where primary keys have changed in {dataset_path}"
            )
            for cwr, pk in zip(commit_with_refs3, pks3):
                click.echo(f"{cwr}: {dataset_path}:{pk}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts where primary keys have changed isn't supported yet"
            )
        conflict_pks.setdefault(dataset_path, [])
        conflict_pks[dataset_path].append(pk)

    num_conflicts = sum(len(pk_list) for pk_list in conflict_pks.values())
    click.echo(f"\nFound {num_conflicts} conflicting features:")
    for dataset_path, pks in conflict_pks.items():
        click.echo(f"{len(pks)} in {dataset_path}")
    click.echo()

    # Check for dirty working copy before continuing - we don't want to fail after interactive part.
    ours_rs = repo_structures3.ours
    ours_rs.working_copy.reset(commits3.ours, ours_rs)

    # At this point, the failure should be dealt with so we can start resolving conflicts interactively.
    # We don't want to fail during conflict resolution, since then we would lose all the user's work.
    # TODO: Support other way(s) of resolving conflicts.
    input_mode = get_input_mode()
    if dry_run:
        click.echo("Printing conflicts but not resolving due to --dry-run")
    elif input_mode == InputMode.INTERACTIVE:
        interactive_pause(
            "Press enter to begin resolving merge conflicts, or Ctrl+C to abort at any time..."
        )
    elif input_mode == InputMode.NO_INPUT:
        click.echo(
            "Printing conflicts but not resolving - run from an interactive terminal to resolve"
        )

    # For each conflict, print and maybe resolve it.
    for dataset_path, pks in sorted(conflict_pks.items()):
        datasets3 = aot(rs[dataset_path] for rs in repo_structures3)
        ours_ds = datasets3.ours
        for pk in sorted(pks):
            feature_name = f"{dataset_path}:{ours_ds.primary_key}={pk}"
            features3 = aot(_safe_get_feature(d, pk) for d in datasets3)
            print_conflict(feature_name, features3, commit_with_refs3)

            if not dry_run and input_mode != InputMode.NO_INPUT:
                index_path = f"{dataset_path}/{ours_ds.get_feature_path(pk)}"
                resolve_conflict_interactive(feature_name, merge_index, index_path)

    if dry_run:
        raise InvalidOperation(
            "Run without --dry-run to resolve merge conflicts", exit_code=MERGE_CONFLICT
        )
    elif input_mode == InputMode.NO_INPUT:
        raise InvalidOperation(
            "Use an interactive terminal to resolve merge conflicts",
            exit_code=MERGE_CONFLICT,
        )

    # Conflicts are resolved
    assert not merge_index.conflicts
    return merge_index


def print_conflict(feature_name, features3, commit_with_refs3):
    """
    Prints 3 versions of a feature.
    feature_name - the name of the feature.
    features3 - AncestorOursTheirs tuple containing three versions of a feature.
    commit_with_refs3 - AncestorOursTheirs tuple containing a CommitWithReference
        for each of the three versions.
    """
    click.secho(f"\n=========== {feature_name} ==========", bold=True)
    for name, feature, cwr in zip(
        AncestorOursTheirs.NAMES, features3, commit_with_refs3
    ):
        prefix = "---" if name == "ancestor" else "+++"
        click.secho(f"{prefix} {name:>9}: {cwr}")
        if feature is not None:
            prefix = "- " if name == "ancestor" else "+ "
            fg = "red" if name == "ancestor" else "green"
            click.secho(repr_row(feature, prefix=prefix), fg=fg)


_aot_choice = click.Choice(choices=AncestorOursTheirs.CHARS)


def resolve_conflict_interactive(feature_name, merge_index, index_path):
    """
    Resolves the conflict at merge_index.conflicts[index_path] by asking
    the user version they prefer - ancestor, ours or theirs.
    feature_name - the name of the feature at index_path.
    merge_index - a pygit2.Index with conflicts.
    index_path - a path where merge_index has a conflict.
    """
    char = click.prompt(
        f"For {feature_name} accept which version - ancestor, ours or theirs",
        type=_aot_choice,
    )
    choice = AncestorOursTheirs.CHARS.index(char)
    index_entries3 = merge_index.conflicts[index_path]
    del merge_index.conflicts[index_path]
    merge_index.add(index_entries3[choice])
