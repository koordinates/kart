import os
import sys

import click

from .diff_output import repr_row
from .exceptions import InvalidOperation, NotYetImplemented
from .structure import RepositoryStructure


@click.command()
@click.option(
    "--ff/--no-ff",
    default=True,
    help=(
        "When the merge resolves as a fast-forward, only update the branch pointer, without creating a merge commit. "
        "With --no-ff create a merge commit even when the merge resolves as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date "
        "or the merge can be resolved as a fast-forward."
    ),
)
@click.option("--force", "-f", is_flag=True)
@click.argument("commit", required=True, metavar="COMMIT")
@click.pass_context
def merge(ctx, ff, ff_only, force, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """
    repo = ctx.obj.repo

    if ff_only and not ff:
        raise click.BadParameter(
            "Conflicting parameters: --no-ff & --ff-only", param_hint="--ff-only"
        )

    # accept ref-ish things (refspec, branch, commit)
    c_theirs, r_theirs = repo.resolve_refish(commit)
    c_ours, r_ours = repo.resolve_refish("HEAD")

    click.echo(f"Merging {c_theirs.id} to {c_ours.id} ...")
    merge_base_id = repo.merge_base(c_theirs.id, c_ours.id)
    click.echo(f"Found merge base: {merge_base_id}")

    if not merge_base_id:
        raise InvalidOperation(f"Commits {c_theirs.id} and {c_ours.id} aren't related.")

    # We're up-to-date if we're trying to merge our own common ancestor.
    if merge_base_id == c_theirs.id:
        click.echo("Already merged!")
        return

    # We're fastforwardable if we're our own common ancestor.
    can_ff = merge_base_id == c_ours.id

    if ff_only and not can_ff:
        click.echo("Can't resolve as a fast-forward merge and --ff-only specified")
        ctx.exit(1)

    if can_ff and ff:
        # do fast-forward merge
        repo.head.set_target(c_theirs.id, "merge: Fast-forward")
        commit_id = c_theirs.id
        click.echo("Fast-forward")
    else:
        c_ancestor = repo[merge_base_id]
        merge_index = repo.merge_trees(
            ancestor=c_ancestor.tree, ours=c_ours.tree, theirs=c_theirs.tree
        )
        if merge_index.conflicts:
            commit_id = resolve_merge_conflicts(
                repo,
                merge_index,
                ancestor=(c_ancestor, None),
                ours=(c_ours, r_ours),
                theirs=(c_theirs, r_theirs),
                force=force,
            )
        else:
            click.echo("No conflicts!")
            merge_tree_id = merge_index.write_tree(repo)
            click.echo(f"Merge tree: {merge_tree_id}")

            user = repo.default_signature
            merge_message = "Merge '{}'".format(
                r_theirs.shorthand if r_theirs else c_theirs.id
            )
            commit_id = repo.create_commit(
                repo.head.name,
                user,
                user,
                merge_message,
                merge_tree_id,
                [c_ours.id, c_theirs.id],
            )
            click.echo(f"Merge commit: {commit_id}")

    # update our working copy
    repo_structure = RepositoryStructure(repo)
    wc = repo_structure.working_copy
    click.echo(f"Updating {wc.path} ...")
    commit = repo[commit_id]
    return wc.reset(commit, repo_structure, force=force)


def _get_long_label(commit, ref):
    """Given a commit and a reference (can be None), returns a label"""
    if ref is not None:
        return f"\"{ref.shorthand}\" ({commit.id.hex})"
    else:
        return f"({commit.id.hex})"


def _safe_get_dataset_for_index_entry(repo_structure, index_entry):
    """Gets the dataset that a pygit2.IndexEntry refers to, or None"""
    try:
        return repo_structure.get_for_index_entry(index_entry)
    except KeyError:
        return None


def _get_pk_for_index_entry(dataset, index_entry):
    """Uses a dataset to decode the primary key that a pygit2.IndexEntry refers to"""
    return dataset.index_entry_to_pk(index_entry)


def _safe_get_feature(dataset, pk):
    """Gets the dataset's feature with a particular primary key, or None"""
    try:
        _, feature = dataset.get_feature(pk)
        return feature
    except KeyError:
        return None


def first_true(iterable):
    return next(filter(None, iterable))


def _interactive_pause(prompt):
    click.prompt(prompt, prompt_suffix="", default="", show_default=False)


def resolve_merge_conflicts(repo, merge_index, ancestor, ours, theirs, force=False):
    """
    Supports resolution of basic merge conflicts, fails in more complex unsupported cases.

    repo - a pygit2.Repository
    merge_index - a pygit2.Index containing the attempted merge and merge conflicts.
    ancestor, ours, theirs - each is a tuple (pygit2.Commit, pygit2.Reference), such
    as might be obtained from Repository.resolve_refish. The reference can be None.
    """

    ANCESTOR, OURS, THEIRS = 0, 1, 2
    # All of the following are 3-tuples of the form (ancestor, ours, theirs)
    commit_args3 = (ancestor, ours, theirs)
    commits3 = tuple(arg[0] for arg in commit_args3)
    refs3 = tuple(arg[1] for arg in commit_args3)
    labels3 = tuple(_get_long_label(*arg) for arg in commit_args3)
    repo_structures3 = tuple(RepositoryStructure(repo, commit=c) for c in commits3)

    conflict_pks = {}
    for index_entries3 in merge_index.conflicts:
        datasets3 = tuple(
            _safe_get_dataset_for_index_entry(*x)
            for x in zip(repo_structures3, index_entries3)
        )
        dataset = first_true(datasets3)
        dataset_path = dataset.path
        if None in datasets3:
            for i in range(3):
                presence = "present" if datasets3[i] is not None else "absent"
                click.echo(f"{labels3[i]}: {dataset_path} is {presence}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts where datasets are added or removed isn't supported yet"
            )

        pks3 = tuple(
            _get_pk_for_index_entry(*x) for x in zip(datasets3, index_entries3)
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
            for i in range(3):
                click.echo(f"{labels3[i]}: {dataset_path}:{pks3[i]}")
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
    ours_rs = repo_structures3[OURS]
    ours_rs.working_copy.reset(commits3[OURS], ours_rs, force=force)

    # At this point, the failure should be dealt with so we can start resolving conflicts interactively.
    # We don't want to fail during conflict resolution, since then we would lose all the user's work.
    # TODO: Support other way(s) of resolving conflicts.
    if sys.stdout.isatty():
        _interactive_pause(
            "Press enter to begin resolving merge conflicts, or Ctrl+C to abort at any time..."
        )
    else:
        click.echo(
            "Printing conflicts but not resolving - merge conflicts must be resolved in an interactive terminal"
        )

    # For each conflict, print and maybe resolve it.
    for dataset_path, pks in conflict_pks.items():
        datasets = tuple(rs[dataset_path] for rs in repo_structures3)
        ours_ds = datasets[OURS]
        for pk in sorted(pks):
            feature_name = f"{dataset_path}:{pk}"
            features3 = tuple(_safe_get_feature(d, pk) for d in datasets)
            print_conflict(feature_name, features3, labels3)

            if sys.stdout.isatty():
                index_path = os.path.join(dataset_path, ours_ds.get_feature_path(pk))
                resolve_conflict_interactive(feature_name, merge_index, index_path)

    if not sys.stdout.isatty():
        raise InvalidOperation(
            "Merge conflicts must be resolved in an interactive terminal"
        )

    # Conflicts are resolved, time to commit
    assert not merge_index.conflicts
    merge_tree_id = merge_index.write_tree(repo)
    click.echo(f"Merge tree: {merge_tree_id}")

    user = repo.default_signature
    merge_message = "Merge '{}'".format(
        refs3[THEIRS].shorthand if refs3[THEIRS] else refs3[THEIRS].id.hex
    )
    commit_id = repo.create_commit(
        repo.head.name,
        user,
        user,
        merge_message,
        merge_tree_id,
        [commits3[OURS].id, commits3[THEIRS].id],
    )
    click.echo(f"Merge commit: {commit_id}")
    return commit_id


_version_names3 = ("ancestor", "ours", "theirs")
_prefixes3 = ("- ", "+ ", "+ ")
_big_prefixes3 = ("---", "+++", "+++")
_cols3 = ("red", "green", "green")


def print_conflict(feature_label, features3, labels3):
    """
    Prints 3 versions of a feature.
    feature_label - the name of the feature.
    features3 - tuple of 3 versions of the feature (ancestor, ours, theirs)
    labels3 - labels for each version.
    """
    click.secho(f"\n=========== {feature_label} ==========", bold=True)
    for i in range(3):
        click.secho(f"{_big_prefixes3[i]} {_version_names3[i]:>9}: {labels3[i]}")
        if features3[i] is not None:
            click.secho(repr_row(features3[i], prefix=_prefixes3[i]), fg=_cols3[i])


_version_chars3 = ("a", "o", "t")
_version_chars_choice = click.Choice(choices=_version_chars3)


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
        type=_version_chars_choice,
    )
    choice = _version_chars3.index(char)
    index_entries3 = merge_index.conflicts[index_path]
    del merge_index.conflicts[index_path]
    merge_index.add(index_entries3[choice])
