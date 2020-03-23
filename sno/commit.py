import contextlib
import logging
import json
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path

import click
import pygit2

from .core import check_git_user
from .diff import Diff
from .status import get_branch_status_message, get_diff_status_message
from .working_copy import WorkingCopy
from .structure import RepositoryStructure
from .cli_util import MutexOption


@contextlib.contextmanager
def _info_to_stdout(logger):
    old_effective_level = logger.getEffectiveLevel()
    old_level = logger.level

    def log_at_root(r):
        return r.name != logger.name or r.levelno >= old_effective_level

    def log_to_stdout(r):
        return r.name == logger.name and r.levelno == logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    handler.addFilter(log_to_stdout)

    logger.root.handlers[0].addFilter(log_at_root)
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    yield logger
    logger.removeHandler(handler)
    logger.setLevel(old_level)
    logger.root.handlers[0].removeFilter(log_at_root)


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
    type=click.File(),
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
@click.option(
    "--text",
    "is_output_json",
    flag_value=False,
    default=True,
    help="Commit result shown in text format",
    cls=MutexOption,
    exclusive_with=["json"],
)
@click.option(
    "--json",
    "is_output_json",
    flag_value=True,
    help="Commit result shown in JSON format",
    cls=MutexOption,
    exclusive_with=["text"],
)
def commit(ctx, message, message_file, allow_empty, is_output_json):
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

    log_tweak = contextlib.nullcontext()
    if not is_output_json:
        log_tweak = _info_to_stdout(logging.getLogger("sno.structure"))

    with log_tweak:
        new_commit = rs.commit(wcdiff, commit_msg, allow_empty=allow_empty)

    if is_output_json:
        json.dump(
            {
                "sno.commit/v1": {
                    "commit": str(new_commit),
                    "tree": str(repo.head.peel(pygit2.Tree).id)
                }
            },
            sys.stdout,
            indent=2)


def get_commit_message(repo, wc_changes):
    """ Launches the system editor to get a commit message """
    editor = os.environ.get("GIT_EDITOR")
    if not editor:
        editor = os.environ.get("VISUAL")
    if not editor:
        editor = os.environ.get("EDITOR", "nano")

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

    with open(Path(repo.path) / "COMMIT_EDITMSG", "w+") as f:
        f.write("\n".join(initial_message) + "\n")
        f.flush()

        click.echo("hint: Waiting for your editor to close the file...")
        try:
            subprocess.check_call(f"{editor} {shlex.quote(f.name)}", shell=True)
        except subprocess.CalledProcessError as e:
            raise click.ClickException(
                f"There was a problem with the editor '{editor}': {e}"
            ) from e

        f.seek(0)
        message = f.read()

        # strip:
        # - whitespace at start/end
        # - comment lines
        # - blank lines surrounding comment lines
        message = re.sub(r"^\n*#.*\n", "", message.strip(), flags=re.MULTILINE)
        return message.strip()
