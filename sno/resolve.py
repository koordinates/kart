import click

from .cli_util import MutexOption
from .merge_util import MergedOursTheirs, MergeIndex, MergeContext, RichConflict
from .exceptions import InvalidOperation, NotFound, NO_CONFLICT
from .repo_files import RepoState


@click.command()
@click.pass_context
@click.option(
    "--ancestor",
    "resolve_to_version",
    flag_value="ancestor",
    help="Resolve the conflict by accepting the 'ancestor' version",
    cls=MutexOption,
    exclusive_with=["ours", "theirs", "delete"],
)
@click.option(
    "--ours",
    "resolve_to_version",
    flag_value="ours",
    help="Resolve the conflict by accepting the 'ours' version",
    cls=MutexOption,
    exclusive_with=["ancestor", "theirs", "delete"],
)
@click.option(
    "--theirs",
    "resolve_to_version",
    flag_value="theirs",
    help="Resolve the conflict by accepting the 'theirs' version",
    cls=MutexOption,
    exclusive_with=["ancestor", "ours", "delete"],
)
@click.option(
    "--delete",
    "resolve_to_version",
    flag_value="delete",
    help="Resolve the conflict by deleting it",
    cls=MutexOption,
    exclusive_with=["ancestor", "ours", "theirs"],
)
# TODO - add more options for accepting other, more interesting versions.
@click.argument("conflict_label", default=None, required=True)
def resolve(ctx, resolve_to_version, conflict_label):
    """Resolve a merge conflict. So far only supports resolving to any of the three existing versions."""

    repo = ctx.obj.get_repo(allowed_states=[RepoState.MERGING])
    if not resolve_to_version:
        raise click.UsageError("Resolve with either --ancestor, --ours or --theirs")

    merge_index = MergeIndex.read_from_repo(repo)
    merge_context = MergeContext.read_from_repo(repo)

    for key, conflict3 in merge_index.conflicts.items():
        rich_conflict = RichConflict(conflict3, merge_context)
        if rich_conflict.label == conflict_label:
            if key in merge_index.resolves:
                raise InvalidOperation(
                    f"Conflict at {conflict_label} is already resolved"
                )

            if resolve_to_version == "delete":
                res = MergedOursTheirs.EMPTY
            else:
                res = MergedOursTheirs.partial(
                    merged=getattr(conflict3, resolve_to_version)
                )
            merge_index.add_resolve(key, res)
            merge_index.write_to_repo(repo)
            click.echo(
                f"Resolved 1 conflict. {len(merge_index.unresolved_conflicts)} conflicts to go."
            )
            ctx.exit(0)

    raise NotFound(f"No conflict found at {conflict_label}", NO_CONFLICT)
