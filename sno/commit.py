import os
import re
import shlex
import subprocess
from pathlib import Path

import click
import pygit2

from . import is_windows
from .core import check_git_user
from .diff import Diff
from .status import get_branch_status_message, get_diff_status_message
from .working_copy import WorkingCopy
from .structure import RepositoryStructure
from .cli_util import MutexOption

if is_windows:
    FALLBACK_EDITOR = 'notepad.exe'
else:
    FALLBACK_EDITOR = 'nano'

@click.command()
@click.pass_context
@click.option(
    "--message",
    "-m",
    multiple=True,
    help="Use the given message as the commit message. If multiple `-m` options are given, their values are concatenated as separate paragraphs.",
    cls=MutexOption,
    exclusive_with=["message_file"],
)
@click.option(
    "message_file",
    "--file",
    "-F",
    type=click.File(encoding='utf-8'),
    help="Take the commit message from the given file. Use `-` to read the message from the standard input.",
    cls=MutexOption,
    exclusive_with=["message"],
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
def commit(ctx, message, message_file, allow_empty):
    """ Record changes to the repository """
    repo = ctx.obj.repo

    check_git_user(repo)

    commit = repo.head.peel(pygit2.Commit)
    tree = commit.tree

    working_copy = WorkingCopy.open(repo)
    if not working_copy:
        raise click.UsageError("No working copy, use 'checkout'")

    working_copy.assert_db_tree_match(tree)

    rs = RepositoryStructure(repo)
    wcdiff = Diff(None)
    wc_changes = {}
    for i, dataset in enumerate(rs):
        diff = working_copy.diff_db_to_tree(dataset)
        wcdiff += diff
        wc_changes[dataset.path] = diff.counts(dataset)

    if not wcdiff and not allow_empty:
        raise click.ClickException("No changes to commit")

    if message_file:
        commit_msg = message_file.read().strip()
    elif message:
        commit_msg = "\n\n".join([m.strip() for m in message]).strip()
    else:
        commit_msg = get_commit_message(repo, wc_changes)

    if not commit_msg:
        raise click.Abort()

    new_commit = rs.commit(wcdiff, commit_msg, allow_empty=allow_empty)


def get_commit_message(repo, wc_changes):
    """ Launches the system editor to get a commit message """
    editor = os.environ.get("GIT_EDITOR")
    if not editor:
        editor = os.environ.get("VISUAL")
    if not editor:
        editor = os.environ.get("EDITOR", FALLBACK_EDITOR)

    initial_message = [
        "",
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
            (get_diff_status_message(wc_changes) or "  No changes (empty commit)"),
            flags=re.MULTILINE,
        ),
        "#",
    ]

    commit_editmsg = str(Path(repo.path) / "COMMIT_EDITMSG")
    with open(commit_editmsg, "wt+", encoding='utf-8') as f:
        f.write("\n".join(initial_message) + "\n")
        f.flush()

    click.echo("hint: Waiting for your editor to close the file...")
    if is_windows:
        # No shlex.quote() on windows
        # " isn't legal in filenames
        editor_cmd = f'{editor} "{commit_editmsg}"'
    else:
        editor_cmd = f"{editor} {shlex.quote(commit_editmsg)}"
    try:
        subprocess.check_call(editor_cmd, shell=True)
    except subprocess.CalledProcessError as e:
        raise click.ClickException(
            f"There was a problem with the editor '{editor}': {e}"
        ) from e

    with open(commit_editmsg, "rt", encoding='utf-8') as f:
        f.seek(0)
        message = f.read()

    # strip:
    # - whitespace at start/end
    # - comment lines
    # - blank lines surrounding comment lines
    message = re.sub(r"^\n*#.*\n", "", message.strip(), flags=re.MULTILINE)
    return message.strip()
