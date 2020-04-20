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
@click.argument("commit", required=True, metavar="COMMIT")
@click.pass_context
def merge(ctx, ff, ff_only, commit):
    """ Incorporates changes from the named commits (usually other branch heads) into the current branch. """
    repo = ctx.obj.repo

    if ff_only and not ff:
        raise click.BadParameter(
            "Conflicting parameters: --no-ff & --ff-only", param_hint="--ff-only"
        )

    c_ours = repo[repo.head.target]

    # accept ref-ish things (refspec, branch, commit)
    c_theirs, r_theirs = repo.resolve_refish(commit)

    print(f"Merging {c_theirs.id} to {c_ours.id} ...")
    merge_base_id = repo.merge_base(c_theirs.id, c_ours.id)
    print(f"Found merge base: {merge_base_id}")

    if not merge_base_id:
        raise InvalidOperation(f"Commits {c_theirs.id} and {c_ours.id} aren't related.")

    # We're up-to-date if we're trying to merge our own common ancestor.
    if merge_base_id == c_theirs.id:
        print("Already merged!")
        return

    # We're fastforwardable if we're our own common ancestor.
    can_ff = merge_base_id == c_ours.id

    if ff_only and not can_ff:
        print("Can't resolve as a fast-forward merge and --ff-only specified")
        ctx.exit(1)

    if can_ff and ff:
        # do fast-forward merge
        repo.head.set_target(c_theirs.id, "merge: Fast-forward")
        commit_id = c_theirs.id
        print("Fast-forward")
    else:
        c_ancestor = repo[merge_base_id]
        merge_index = repo.merge_trees(
            ancestor=c_ancestor.tree, ours=c_ours.tree, theirs=c_theirs.tree
        )
        if merge_index.conflicts:
            handle_merge_conflicts(
                repo,
                merge_index,
                ancestor=(c_ancestor, "ancestor"),
                ours=(c_ours, "HEAD"),
                theirs=(c_theirs, commit),
            )
        else:
            print("No conflicts!")
            merge_tree_id = merge_index.write_tree(repo)
            print(f"Merge tree: {merge_tree_id}")

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
                [c_ours.oid, c_theirs.oid],
            )
            print(f"Merge commit: {commit_id}")

    # update our working copy
    repo_structure = RepositoryStructure(repo)
    wc = repo_structure.working_copy
    click.echo(f"Updating {wc.path} ...")
    commit = repo[commit_id]
    return wc.reset(commit, repo_structure)


def _get_commit(commit_or_tuple):
    """Given either a commit or a tuple (commit, label), returns the commit"""
    if type(commit_or_tuple) is tuple:
        return commit_or_tuple[0]
    return commit_or_tuple


def _get_commit_label(commit_or_tuple):
    """Given either a commit or a tuple (commit, label), returns a label for the commit"""
    if type(commit_or_tuple) is tuple:
        commit = commit_or_tuple[0]
        label = commit_or_tuple[1]
        if label != commit.id.hex:
            label = f"{label} ({commit.id.hex})"
        return label
    else:
        return commit_or_tuple.id.hex


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


def handle_merge_conflicts(
    repo, merge_index, ancestor, ours, theirs,
):
    """
    Prints merge conflicts and fails, since resolving merge conflicts is not yet supported.

    repo - a pygit2.Repository
    merge_index - a pygit2.Index containing the attempted merge and merge conflicts.
    ancestor, ours, theirs - each is either a pygit2.Commit or tuple (pygit2.Commit, label) -
        where label is a name of the commit that the user would recognize in this context.

    """
    print("Merge conflicts!")
    # All of the following are 3-tuples of the form (ancestor, ours, theirs)
    commit_args3 = (ancestor, ours, theirs)
    commits3 = tuple(_get_commit(arg) for arg in commit_args3)
    labels3 = tuple(_get_commit_label(arg) for arg in commit_args3)
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
                print(f"{labels3[i]}: {dataset_path} is {presence}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts where datasets are added or removed isn't supported yet"
            )

        pks3 = tuple(
            _get_pk_for_index_entry(*x) for x in zip(datasets3, index_entries3)
        )
        if "META" in pks3:
            raise NotYetImplemented(
                "Sorry, resolving conflicts in metadata isn't supported yet"
            )
        pk = first_true(pks3)
        if pks3.count(pk) != 3:
            for i in range(3):
                print(f"{labels3[i]}: {dataset_path}:{pks3[i]}")
            raise NotYetImplemented(
                "Sorry, resolving conflicts where primary keys have changed isn't supported yet"
            )
        conflict_pks.setdefault(dataset_path, [])
        conflict_pks[dataset_path].append(pk)

    prefixes3 = ("- ", "+ ", "+ ")
    big_prefixes3 = ("---", "+++", "+++")
    cols3 = ("red", "green", "green")

    for dataset_path, pks in conflict_pks.items():
        datasets = tuple(rs[dataset_path] for rs in repo_structures3)
        for pk in sorted(pks):
            features3 = tuple(_safe_get_feature(d, pk) for d in datasets)
            click.secho(f"=========== {dataset_path}:{pk} ==========", bold=True)
            for i in range(3):
                click.secho(f"{big_prefixes3[i]} {labels3[i]}")
                if features3[i] is not None:
                    click.secho(
                        repr_row(features3[i], prefix=prefixes3[i]), fg=cols3[i]
                    )
            click.secho()

    raise NotYetImplemented("Sorry, merging conflicts isn't supported yet")
