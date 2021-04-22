from datetime import datetime, timedelta, timezone
import re
import os
import shlex
import shutil
import subprocess
import sys

import click
import pygit2

from . import is_windows
from .cli_util import StringFromFile, tool_environment
from .core import check_git_user
from .exceptions import (
    NotFound,
    SubprocessError,
    NO_CHANGES,
    NO_DATA,
    NO_WORKING_COPY,
)
from .filter_util import build_feature_filter
from .output_util import dump_json_output
from .status import (
    get_branch_status_message,
    get_diff_status_message,
    get_diff_status_json,
    diff_status_to_text,
)
from .timestamps import (
    datetime_to_iso8601_utc,
    timedelta_to_iso8601_tz,
    commit_time_to_text,
)
from .repo import SnoRepoFiles


@click.command()
@click.pass_context
@click.option(
    "--message",
    "-m",
    multiple=True,
    help="Use the given message as the commit message. If multiple `-m` options are given, their values are concatenated as separate paragraphs.",
    type=StringFromFile(encoding="utf-8"),
)
@click.option(
    "--allow-empty",
    is_flag=True,
    default=False,
    help=(
        "Usually recording a commit that has the exact same tree as its sole "
        "parent commit is a mistake, and the command prevents you from making "
        "such a commit. This option bypasses the safety"
    ),
)
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.argument(
    "filters",
    nargs=-1,
)
def commit(ctx, message, allow_empty, output_format, filters):
    """
    Record a snapshot of all of the changes to the repository.

    To commit only particular changes, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """
    repo = ctx.obj.repo

    if repo.is_empty:
        raise NotFound(
            'Empty repository.\n  (use "kart import" to add some data)',
            exit_code=NO_DATA,
        )

    check_git_user(repo)

    commit = repo.head_commit
    tree = commit.tree

    working_copy = repo.working_copy
    if not working_copy:
        raise NotFound("No working copy, use 'checkout'", exit_code=NO_WORKING_COPY)

    working_copy.assert_db_tree_match(tree)

    commit_filter = build_feature_filter(filters)
    wc_diff = working_copy.diff_to_tree(commit_filter)

    if not wc_diff and not allow_empty:
        raise NotFound("No changes to commit", exit_code=NO_CHANGES)

    do_json = output_format == "json"
    if message:
        commit_msg = "\n\n".join([m.strip() for m in message]).strip()
    else:
        commit_msg = get_commit_message(repo, wc_diff, quiet=do_json)

    if not commit_msg:
        raise click.UsageError("Aborting commit due to empty commit message.")

    new_commit = repo.structure().commit_diff(
        wc_diff, commit_msg, allow_empty=allow_empty
    )

    working_copy.reset_tracking_table(commit_filter)
    working_copy.update_state_table_tree(new_commit.peel(pygit2.Tree).id.hex)

    jdict = commit_obj_to_json(new_commit, repo, wc_diff)
    if do_json:
        dump_json_output(jdict, sys.stdout)
    else:
        click.echo(commit_json_to_text(jdict))

    repo.gc("--auto")


def get_commit_message(repo, diff, draft_message="", quiet=False):
    """ Launches the system editor to get a commit message """
    initial_message = [
        draft_message,
        "# Please enter the commit message for your changes. Lines starting",
        "# with '#' will be ignored, and an empty message aborts the commit.",
        "#",
        re.sub(r"^", "# ", get_branch_status_message(repo), flags=re.MULTILINE),
        "#",
        "# Changes to be committed:",
        "#",
        re.sub(
            r"^",
            "# ",
            (get_diff_status_message(diff) or "  No changes (empty commit)"),
            flags=re.MULTILINE,
        ),
        "#",
    ]

    commit_editmsg_file = repo.gitdir_file(SnoRepoFiles.COMMIT_EDITMSG)
    commit_editmsg_file.write_text("\n".join(initial_message) + "\n", encoding="utf-8")
    if not quiet:
        click.echo("hint: Waiting for your editor to close the file...")
    user_edit_file(commit_editmsg_file)
    message = commit_editmsg_file.read_text(encoding="utf-8")

    # strip:
    # - whitespace at start/end
    # - comment lines
    # - blank lines surrounding comment lines
    message = re.sub(r"^\n*#.*\n", "", message, flags=re.MULTILINE)
    return message.strip()


def fallback_editor():
    if is_windows:
        return "notepad.exe"
    else:
        return shutil.which("nano") and "nano" or "vi"


def user_edit_file(path):
    editor = os.environ.get("GIT_EDITOR")
    if not editor:
        editor = os.environ.get("VISUAL")
    if not editor:
        editor = os.environ.get("EDITOR")
    if not editor:
        editor = fallback_editor()

    path = str(path.resolve())
    if is_windows:
        # No shlex.quote() on windows
        # " isn't legal in filenames
        editor_cmd = f'{editor} "{path}"'
    else:
        editor_cmd = f"{editor} {shlex.quote(path)}"
    try:
        run_editor_cmd(editor_cmd)
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with the editor '{editor}': {e}",
            called_process_error=e,
        ) from e


def run_editor_cmd(editor_cmd):
    subprocess.check_call(editor_cmd, shell=True, env=tool_environment())


def commit_obj_to_json(commit, repo, wc_diff):
    branch = None
    if not repo.head_is_detached:
        branch = repo.branches[repo.head.shorthand].shorthand
    commit_time = datetime.fromtimestamp(commit.commit_time, timezone.utc)
    commit_time_offset = timedelta(minutes=commit.commit_time_offset)
    return {
        "kart.commit/v1": {
            "commit": commit.id.hex,
            "abbrevCommit": commit.short_id,
            "author": commit.author.email,
            "committer": commit.committer.email,
            "branch": branch,
            "message": commit.message,
            "changes": get_diff_status_json(wc_diff),
            "commitTime": datetime_to_iso8601_utc(commit_time),
            "commitTimeOffset": timedelta_to_iso8601_tz(commit_time_offset),
        }
    }


def commit_json_to_text(jdict):
    jdict = jdict["kart.commit/v1"]
    branch = jdict["branch"]
    commit = jdict["abbrevCommit"]
    message = jdict["message"].replace("\n", " ")
    diff = diff_status_to_text(jdict["changes"])
    datetime = commit_time_to_text(jdict["commitTime"], jdict["commitTimeOffset"])
    return f"[{branch} {commit}] {message}\n{diff}\n  Date: {datetime}"
