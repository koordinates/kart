import logging
import sys

import click

from .diff_output import repr_row
from .exceptions import InvalidOperation, NotYetImplemented, MERGE_CONFLICT
from .structs import AncestorOursTheirs, CommitWithReference
from .structure import RepositoryStructure


L = logging.getLogger("sno.conflicts")


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


def aot(*args):
    """Creates an AncestorOursTheirs - from 3 positional args, an array, a tuple, or a generator."""
    if len(args) == 1:
        return AncestorOursTheirs(*tuple(args[0]))
    else:
        return AncestorOursTheirs(*args)


def resolve_merge_conflicts(repo, merge_index, ancestor, ours, theirs, dry_run=False):
    """
    Supports resolution of basic merge conflicts, fails in more complex unsupported cases.

    repo - a pygit2.Repository
    merge_index - a pygit2.Index containing the attempted merge and merge conflicts.
    ancestor, ours, theirs - each is a either a pygit2.Commit, or a CommitWithReference.
    """

    # We have three versions of lots of objects - ancestor, ours, theirs.
    commit_with_refs3 = aot(
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
        AncestorOursTheirs.names, features3, commit_with_refs3
    ):
        prefix = "---" if name == "ancestor" else "+++"
        click.secho(f"{prefix} {name:>9}: {cwr}")
        if feature is not None:
            prefix = "- " if name == "ancestor" else "+ "
            fg = "red" if name == "ancestor" else "green"
            click.secho(repr_row(feature, prefix=prefix), fg=fg)


_aot_choice = click.Choice(choices=AncestorOursTheirs.chars)


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
    choice = AncestorOursTheirs.chars.index(char)
    index_entries3 = merge_index.conflicts[index_path]
    del merge_index.conflicts[index_path]
    merge_index.add(index_entries3[choice])
