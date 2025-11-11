import sys

import click
import pygit2

from kart.cli_util import StringFromFile
from kart.commit import compose_draft_message, get_commit_message
from kart.output_util import dump_json_output
from kart.exceptions import NO_CHANGES, NotFound, InvalidOperation
from kart.object_builder import ObjectBuilder


@click.command()
@click.argument("old_name")
@click.argument("new_name")
@click.pass_context
@click.option(
    "--message",
    "-m",
    multiple=True,
    help=(
        "Use the given message as the commit message. If multiple `-m` options are given, their values are "
        "concatenated as separate paragraphs."
    ),
    type=StringFromFile(encoding="utf-8"),
)
@click.option(
    " /--no-editor",
    "launch_editor",
    is_flag=True,
    default=True,
    hidden=True,
    help="Whether to launch an editor to let the user choose the commit message.",
)
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
def mv(ctx, old_name, new_name, message, launch_editor, output_format):
    """
    Rename a dataset in the Kart repository.

    Moves/renames OLD_NAME to NEW_NAME. This is analogous to 'git mv'.
    """
    repo = ctx.obj.repo
    structure = repo.structure()
    tree = structure.tree

    # Check if working copy is dirty
    repo.working_copy.check_not_dirty()

    datasets = structure.datasets()
    try:
        old_dataset_tree = datasets[old_name].tree
    except KeyError:
        raise NotFound(f"Dataset '{old_name}' not found in repository")

    if new_name in datasets:
        raise InvalidOperation(f"Dataset '{new_name}' already exists in repository")

    # Create the commit message
    do_json = output_format == "json"
    commit_msg = None
    if message:
        commit_msg = "\n\n".join([m.strip() for m in message]).strip()
    elif launch_editor:
        initial_message = compose_draft_message(
            repo, f"Rename {old_name!r} to {new_name!r}"
        )
        commit_msg = get_commit_message(
            repo, initial_message=initial_message, quiet=do_json
        )

    if not commit_msg:
        raise click.UsageError("Aborting commit due to empty commit message.")

    object_builder = ObjectBuilder(repo, tree)

    # create a new tree and remove the old one
    object_builder.insert(new_name, old_dataset_tree)
    object_builder.remove(old_name)
    new_tree = object_builder.flush()

    # create the commit
    parent_commit = repo.head_commit

    new_commit_oid = repo.create_commit(
        structure.ref,
        repo.author_signature(),
        repo.committer_signature(),
        commit_msg,
        new_tree.id,
        [parent_commit.oid],
    )
    new_commit = repo[new_commit_oid]

    # update the working copy by resetting to the new commit
    if not do_json:
        click.echo(
            f"[{repo.head_branch or 'HEAD'} {str(new_commit_oid)[:7]}] {commit_msg}"
        )
        click.echo(f"Renamed {old_name} -> {new_name}")

    # Reset the working copy to the new commit
    # We need to do this to update the working copy with the renamed dataset
    if repo.working_copy.exists():
        repo.working_copy.reset(
            new_commit,
            quiet=do_json,
        )

    if do_json:
        branch = None
        if not repo.head_is_detached:
            branch = repo.branches[repo.head.shorthand].shorthand
        jdict = {
            "kart.commit/v1": {
                "commit": str(new_commit_oid),
                "abbrevCommit": str(new_commit_oid)[:7],
                "message": commit_msg,
                "branch": branch,
            }
        }
        dump_json_output(jdict, sys.stdout)

    repo.gc("--auto")
