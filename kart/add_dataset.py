import click
import sys
from .cli_util import StringFromFile
from .commit import (
    commit_obj_to_json,
    commit_json_to_text,
    get_commit_message,
    CommitDiffWriter,
)
from .output_util import dump_json_output
from .working_copy import WorkingCopyPart
from .exceptions import NO_CHANGES, NotFound, InvalidOperation
from .status import get_untracked_tables


@click.command()
@click.argument("table_name")
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
def add_dataset(ctx, table_name, message, launch_editor, output_format):
    """
    Add a new table to the Kart repository.

    To check the new untracked tables, run 'kart status --list-untracked-tables'
    """
    repo = ctx.obj.repo

    # Check that the table is in the list of untracked tables:
    untracked_tables = get_untracked_tables(repo)
    if table_name not in untracked_tables:
        # Check if the table is already tracked:
        ds_paths = list(repo.datasets().paths())
        for ds_path in ds_paths:
            if ds_path == table_name:
                raise InvalidOperation(
                    f"Table '{table_name}' is already tracked\n",
                    exit_code=NO_CHANGES,
                )

        raise NotFound(
            f"""Table '{table_name}' is not found\n\nTry running 'kart status --list-untracked-tables'\n""",
            exit_code=NO_CHANGES,
        )

    meta_items = repo.working_copy.tabular.meta_items(table_name)
    table_diff = repo.working_copy.tabular.get_diff_for_table_creation(
        table_name, meta_items
    )

    commit_table(
        repo, table_diff, message, launch_editor, output_format, allow_empty=False
    )


def commit_table(
    repo, table_diff, message, launch_editor, output_format, allow_empty=False
):
    if not table_diff and not allow_empty:
        raise NotFound("No changes to commit", exit_code=NO_CHANGES)

    do_json = output_format == "json"
    commit_msg = None
    if message:
        commit_msg = "\n\n".join([m.strip() for m in message]).strip()
    elif launch_editor:
        commit_msg = get_commit_message(repo, table_diff, quiet=do_json)

    if not commit_msg:
        raise click.UsageError("Aborting commit due to empty commit message.")

    commit_diff_writer = CommitDiffWriter(repo)
    new_commit = repo.structure().commit_diff(
        table_diff, commit_msg, allow_empty=allow_empty
    )

    repo.working_copy.soft_reset_after_commit(
        new_commit,
        quiet=do_json,
        mark_as_clean=commit_diff_writer.repo_key_filter,
        now_outside_spatial_filter=commit_diff_writer.now_outside_spatial_filter,
        committed_diff=table_diff,
    )

    jdict = commit_obj_to_json(new_commit, repo, table_diff)
    if do_json:
        dump_json_output(jdict, sys.stdout)
    else:
        click.echo(commit_json_to_text(jdict))

    repo.gc("--auto")
