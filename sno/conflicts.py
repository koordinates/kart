import logging
import sys
from collections import namedtuple

import click

from .diff_output import repr_row
from .exceptions import InvalidOperation, NotYetImplemented
from .structure import RepositoryStructure


L = logging.getLogger("sno.conflicts")


CommitWithReference = namedtuple("CommitWithReference", ("commit", "reference"))


def first_true(iterable):
    """Returns the value from the iterable that is truthy."""
    return next(filter(None, iterable))


def is_interactive_terminal():
    if sys.stdin.isatty() and sys.stdout.isatty():
        return True
    elif sys.stdin.isatty() and not sys.stdout.isatty():
        raise InvalidOperation(
            "Redirecting stdout but not stdin breaks the interactive prompts"
        )
    return False


def interactive_pause(prompt):
    """Like click.pause() but waits for the Enter key specifically."""
    click.prompt(prompt, prompt_suffix="", default="", show_default=False)


def is_empty_stream(stream):
    if stream.seekable():
        pos = stream.tell()
        if stream.read(1) == "":
            return True
        stream.seek(pos)
    return False


def _get_long_label(commit, reference):
    """Given a commit and a reference (can be None), returns a label"""
    if reference is not None:
        return f'"{reference.shorthand}" ({commit.id.hex})'
    else:
        return f"({commit.id.hex})"


def _safe_get_dataset_for_index_entry(repo_structure, index_entry):
    """Gets the dataset that a pygit2.IndexEntry refers to, or None"""
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


# We have three versions of lots of objects - the 3 versions are ancestor, ours, theirs.
# This namedtuple helps us keep track of them all.
AncestorOursTheirs = namedtuple("AncestorOursTheirs", ("ancestor", "ours", "theirs"))

AncestorOursTheirs.names = AncestorOursTheirs._fields
AncestorOursTheirs.chars = tuple(n[0] for n in AncestorOursTheirs.names)


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
    args3 = aot(ancestor, ours, theirs)
    commits3 = aot(getattr(arg, "commit", arg) for arg in args3)
    refs3 = aot(getattr(arg, "reference", None) for arg in args3)
    labels3 = aot(_get_long_label(c, r) for c, r in zip(commits3, refs3))
    repo_structures3 = aot(RepositoryStructure(repo, commit=c) for c in commits3)

    conflict_pks = {}
    for index_entries3 in merge_index.conflicts:
        datasets3 = aot(
            _safe_get_dataset_for_index_entry(rs, ie)
            for rs, ie in zip(repo_structures3, index_entries3)
        )
        dataset = first_true(datasets3)
        dataset_path = dataset.path
        if None in datasets3:
            for label, ds in zip(labels3, datasets3):
                presence = "present" if ds is not None else "absent"
                click.echo(f"{label}: {dataset_path} is {presence}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts where datasets are added or removed isn't supported yet"
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
            for label, pk in zip(labels3, pks3):
                click.echo(f"{label}: {dataset_path}:{pk}")
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
    empty_input = False
    if dry_run:
        click.echo("Printing conflicts but not resolving due to --dry-run")
    elif is_interactive_terminal():
        interactive_pause(
            "Press enter to begin resolving merge conflicts, or Ctrl+C to abort at any time..."
        )
    elif is_empty_stream(sys.stdin):
        click.echo(
            "Printing conflicts but not resolving - run from an interactive terminal to resolve"
        )
        empty_input = True

    # For each conflict, print and maybe resolve it.
    for dataset_path, pks in sorted(conflict_pks.items()):
        datasets3 = aot(rs[dataset_path] for rs in repo_structures3)
        ours_ds = datasets3.ours
        for pk in sorted(pks):
            feature_name = f"{dataset_path}:{ours_ds.primary_key}={pk}"
            features3 = aot(_safe_get_feature(d, pk) for d in datasets3)
            print_conflict(feature_name, features3, labels3)

            if not (dry_run or empty_input):
                index_path = f"{dataset_path}/{ours_ds.get_feature_path(pk)}"
                resolve_conflict_interactive(feature_name, merge_index, index_path)

    if dry_run:
        return None
    elif empty_input:
        raise InvalidOperation("Use an interactive terminal to resolve merge conflicts")

    # Conflicts are resolved, time to commit
    assert not merge_index.conflicts
    merge_tree_id = merge_index.write_tree(repo)
    L.debug(f"Merge tree: {merge_tree_id}")

    user = repo.default_signature
    merge_message = "Merge '{}'".format(
        refs3.theirs.shorthand if refs3.theirs else commits3.theirs.id.hex
    )
    commit_id = repo.create_commit(
        repo.head.name,
        user,
        user,
        merge_message,
        merge_tree_id,
        [commits3.ours.id, commits3.theirs.id],
    )
    click.echo(f"Merge committed as: {commit_id}")
    return commit_id


def print_conflict(feature_label, features3, labels3):
    """
    Prints 3 versions of a feature.
    feature_label - the name of the feature.
    features3 - AncestorOursTheirs tuple containing three versions of a feature.
    labels3 - AncestorOursTheirs tuple containing the label for each version.
    """
    click.secho(f"\n=========== {feature_label} ==========", bold=True)
    for name, label, feature in zip(AncestorOursTheirs.names, labels3, features3):
        prefix = "---" if name == "ancestor" else "+++"
        click.secho(f"{prefix} {name:>9}: {label}")
        if feature is not None:
            prefix = "- " if name == "ancestor" else "+ "
            fg = "red" if name == "ancestor" else "green"
            click.secho(repr_row(feature, prefix=prefix), fg=fg)


_aot_choice = click.Choice(choices=AncestorOursTheirs.chars)


def resolve_conflict_interactive(feature_name, merge_index, index_path):
    """
    Resolves the conflict at merge_index.conflicts[index_path] by asking
    the user version they prefer - ancestor, ours or theirs.
    merge_index - a pygit2.Index with conflicts.
    index_path - a path where merge_index has a conflict.
    feature_name - the name of the feature at index_path.
    """
    char = click.prompt(
        f"For {feature_name} accept which version - ancestor, ours or theirs",
        type=_aot_choice,
    )
    choice = AncestorOursTheirs.chars.index(char)
    index_entries3 = merge_index.conflicts[index_path]
    del merge_index.conflicts[index_path]
    merge_index.add(index_entries3[choice])
