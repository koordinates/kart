import json
import os
import re
import shlex
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
import pygit2

from . import is_windows
from .core import check_git_user
from .diff import Diff
from .exceptions import NotFound, SubprocessError, NO_CHANGES, NO_DATA, NO_WORKING_COPY
from .status import (
    get_branch_status_message,
    get_diff_status_message,
    get_diff_status_json,
    diff_status_to_text,
)
from .working_copy import WorkingCopy
from .structure import RepositoryStructure
from .cli_util import MutexOption, do_json_option

if is_windows:
    FALLBACK_EDITOR = "notepad.exe"
else:
    FALLBACK_EDITOR = "nano"


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
    type=click.File(encoding="utf-8"),
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
@do_json_option
def commit(ctx, message, message_file, allow_empty, do_json):
    """ Record changes to the repository """
    repo = ctx.obj.repo

    if repo.is_empty:
        raise NotFound(
            'Empty repository.\n  (use "sno import" to add some data)',
            exit_code=NO_DATA,
        )

    check_git_user(repo)

    commit = repo.head.peel(pygit2.Commit)
    tree = commit.tree

    working_copy = WorkingCopy.open(repo)
    if not working_copy:
        raise NotFound("No working copy, use 'checkout'", exit_code=NO_WORKING_COPY)

    working_copy.assert_db_tree_match(tree)

    rs = RepositoryStructure(repo)
    wcdiff = Diff(None)
    wc_changes = {}
    for i, dataset in enumerate(rs):
        diff = working_copy.diff_db_to_tree(dataset)
        wcdiff += diff
        wc_changes[dataset.path] = diff.counts(dataset)

    if not wcdiff and not allow_empty:
        raise NotFound("No changes to commit", exit_code=NO_CHANGES)

    if message_file:
        commit_msg = message_file.read().strip()
    elif message:
        commit_msg = "\n\n".join([m.strip() for m in message]).strip()
    else:
        commit_msg = get_commit_message(repo, wc_changes, quiet=do_json)

    if not commit_msg:
        raise click.UsageError("No commit message")

    rs.commit(wcdiff, commit_msg, allow_empty=allow_empty)

    new_commit = repo.head.peel(pygit2.Commit)
    jdict = commit_obj_to_json(new_commit, repo, wc_changes)
    if do_json:
        json.dump(jdict, sys.stdout, indent=2)
    else:
        click.echo(commit_json_to_text(jdict))


def get_commit_message(repo, wc_changes, quiet=False):
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
    with open(commit_editmsg, "wt+", encoding="utf-8") as f:
        f.write("\n".join(initial_message) + "\n")
        f.flush()

    if not quiet:
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
        raise SubprocessError(
            f"There was a problem with the editor '{editor}': {e}",
            called_process_error=e,
        ) from e

    with open(commit_editmsg, "rt", encoding="utf-8") as f:
        f.seek(0)
        message = f.read()

    # strip:
    # - whitespace at start/end
    # - comment lines
    # - blank lines surrounding comment lines
    message = re.sub(r"^\n*#.*\n", "", message, flags=re.MULTILINE)
    return message.strip()


def commit_obj_to_json(commit, repo, wc_changes):
    branch = None
    if not repo.head_is_detached:
        branch = repo.branches[repo.head.shorthand].shorthand
    commit_time = datetime.fromtimestamp(commit.commit_time, timezone.utc)
    commit_time_offset = timedelta(minutes=commit.commit_time_offset)
    return {
        "sno.commit/v1": {
            "commit": commit.id.hex,
            "abbrevCommit": commit.short_id,
            "author": commit.author.email,
            "committer": commit.committer.email,
            "branch": branch,
            "message": commit.message,
            "changes": get_diff_status_json(wc_changes),
            "commitTime": to_iso8601_utc(commit_time),
            "commitTimeOffset": to_iso8601_tz(commit_time_offset),
        }
    }


def commit_json_to_text(jdict):
    jdict = jdict["sno.commit/v1"]
    branch = jdict["branch"]
    commit = jdict["abbrevCommit"]
    message = jdict["message"].replace("\n", " ")
    diff = diff_status_to_text(jdict["changes"])
    datetime = commit_time_to_text(jdict["commitTime"], jdict["commitTimeOffset"])
    return f"[{branch} {commit}] {message}\n{diff}\n  Date: {datetime}"


def to_iso8601_utc(datetime):
    """Returns a string like: 2020-03-26T09:10:11Z"""
    isoformat = datetime.astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    return f"{isoformat}Z"


def to_iso8601_tz(timedelta):
    """Returns a string like "+05:00" or "-05:00" (ie five hours ahead or behind)."""
    abs_delta = datetime.utcfromtimestamp(abs(timedelta).seconds).strftime('%H:%M')
    return f"+{abs_delta}" if abs(timedelta) == timedelta else f"-{abs_delta}"


def commit_time_to_text(iso8601z, iso_offset):
    """
    Given an isoformat time in UTC, and a isoformat timezone offset,
    returns the time in a human readable format, for that timezone.
    """
    right_time = datetime.fromisoformat(iso8601z.replace("Z", "+00:00"))
    right_tzinfo = datetime.fromisoformat(iso8601z.replace("Z", iso_offset))
    return right_time.astimezone(right_tzinfo.tzinfo).strftime("%c %z")
