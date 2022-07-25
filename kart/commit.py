import os
import re
import shlex
import shutil
import subprocess
import sys
from datetime import datetime, timedelta, timezone

import click

from . import is_windows
from .base_diff_writer import BaseDiffWriter
from .cli_util import StringFromFile, tool_environment, KartCommand
from .core import check_git_user
from .exceptions import (
    NO_CHANGES,
    SPATIAL_FILTER_PK_CONFLICT,
    InvalidOperation,
    NotFound,
    SubprocessError,
)
from .key_filters import RepoKeyFilter
from .output_util import dump_json_output
from .repo import KartRepoFiles
from .status import (
    diff_status_to_text,
    get_branch_status_message,
    get_diff_status_message,
)
from .timestamps import (
    commit_time_to_text,
    datetime_to_iso8601_utc,
    timedelta_to_iso8601_tz,
)


class CommitDiffWriter(BaseDiffWriter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.spatial_filter.match_all:
            self.record_spatial_filter_stats = True
            self.spatial_filter_pk_conflicts = RepoKeyFilter()
            self.now_outside_spatial_filter = RepoKeyFilter()
        else:
            self.record_spatial_filter_stats = False
            self.spatial_filter_pk_conflicts = None
            self.now_outside_spatial_filter = None

    def get_repo_diff(self):
        repo_diff = super().get_repo_diff()

        if self.record_spatial_filter_stats:
            for ds_path, ds_diff in repo_diff.items():
                self.record_spatial_filter_stats_for_dataset(ds_path, ds_diff)

        return repo_diff

    def record_spatial_filter_stat(
        self, ds_path, key, delta, old_match_result, new_match_result
    ):
        super().record_spatial_filter_stat(
            ds_path, key, delta, old_match_result, new_match_result
        )
        if delta.new is not None and not new_match_result:
            self.now_outside_spatial_filter.recursive_set(
                [ds_path, "feature", key], True
            )


@click.command(cls=KartCommand)
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
    "--allow-empty",
    is_flag=True,
    default=False,
    help=(
        "Usually it is a mistake to record a commit that has the exact same tree as its sole parent commit, "
        "so by default it is not allowed. This option bypasses the safety."
    ),
)
@click.option(
    "--allow-pk-conflicts",
    is_flag=True,
    default=False,
    help=(
        "Usually, it is a mistake to insert features into the working copy that have the same primary key "
        "as something that is outside the spatial filter. If such features were committed, it could "
        "accidentally overwrite existing features you were unaware of. So by default, this is not allowed. "
        "This option bypasses the safety."
    ),
)
@click.option(
    "--convert-to-dataset-format",
    is_flag=True,
    default=False,
    help=(
        "Converts any new files in-place, where needed, to ensure they match the existing dataset's format, immediately "
        "before they are committed."
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
def commit(
    ctx,
    message,
    allow_empty,
    allow_pk_conflicts,
    convert_to_dataset_format,
    output_format,
    filters,
):
    """
    Record a snapshot of all of the changes to the repository.

    To commit only particular changes, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """
    repo = ctx.obj.repo

    repo.working_copy.assert_exists()
    repo.working_copy.assert_matches_head_tree()

    check_git_user(repo)

    commit_diff_writer = CommitDiffWriter(repo, "HEAD", filters)
    commit_diff_writer.convert_to_dataset_format(convert_to_dataset_format)
    wc_diff = commit_diff_writer.get_repo_diff()

    if not wc_diff and not allow_empty:
        raise NotFound("No changes to commit", exit_code=NO_CHANGES)

    pk_conflicts = commit_diff_writer.spatial_filter_pk_conflicts
    if not allow_pk_conflicts and pk_conflicts and any(pk_conflicts.values()):
        commit_diff_writer.write_warnings_footer()
        raise InvalidOperation(
            "Aborting commit due to conflicting primary key values - use --allow-pk-conflicts to commit anyway "
            "(this will overwrite some existing features that are outside of the current spatial filter)",
            exit_code=SPATIAL_FILTER_PK_CONFLICT,
        )

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

    repo.working_copy.soft_reset_after_commit(
        new_commit,
        quiet=do_json,
        mark_as_clean=commit_diff_writer.repo_key_filter,
        now_outside_spatial_filter=commit_diff_writer.now_outside_spatial_filter,
        committed_diff=wc_diff,
    )

    jdict = commit_obj_to_json(new_commit, repo, wc_diff)
    if do_json:
        dump_json_output(jdict, sys.stdout)
    else:
        click.echo(commit_json_to_text(jdict))

    repo.gc("--auto")


def get_commit_message(repo, diff, draft_message="", quiet=False):
    """Launches the system editor to get a commit message"""
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

    commit_editmsg_file = repo.gitdir_file(KartRepoFiles.COMMIT_EDITMSG)
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
            "changes": wc_diff.type_counts(),
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
