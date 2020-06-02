import os
from pathlib import Path
import shlex
import shutil
import subprocess

import click

from . import is_windows
from .exceptions import SubprocessError, InvalidOperation, NotFound

# Standard git files:
HEAD = "HEAD"
COMMIT_EDITMSG = "COMMIT_EDITMSG"
ORIG_HEAD = "ORIG_HEAD"
MERGE_HEAD = "MERGE_HEAD"
MERGE_MSG = "MERGE_MSG"

# Sno-specific files:
MERGE_INDEX = "MERGE_INDEX"
MERGE_BRANCH = "MERGE_BRANCH"


def repo_file_path(repo, filename):
    return Path(repo.path) / filename


def repo_file_exists(repo, filename):
    return repo_file_path(repo, filename).exists()


def write_repo_file(repo, filename, contents):
    if not isinstance(contents, str):
        raise TypeError("File contents must be a string", type(contents))
    if not contents.endswith("\n"):
        contents += "\n"

    path = repo_file_path(repo, filename)
    path.write_text(contents, encoding="utf-8")


def fallback_editor():
    if is_windows:
        return "notepad.exe"
    else:
        return shutil.which("nano") and "nano" or "vi"


def user_edit_repo_file(repo, filename):
    editor = os.environ.get("GIT_EDITOR")
    if not editor:
        editor = os.environ.get("VISUAL")
    if not editor:
        editor = os.environ.get("EDITOR")
    if not editor:
        editor = fallback_editor()

    path = str(repo_file_path(repo, filename))
    if is_windows:
        # No shlex.quote() on windows
        # " isn't legal in filenames
        editor_cmd = f'{editor} "{path}"'
    else:
        editor_cmd = f"{editor} {shlex.quote(path)}"
    try:
        subprocess.check_call(editor_cmd, shell=True)
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with the editor '{editor}': {e}",
            called_process_error=e,
        ) from e


def read_repo_file(repo, filename, missing_ok=False):
    path = repo_file_path(repo, filename)
    if missing_ok and not path.exists():
        return None
    return path.read_text(encoding="utf-8")


def remove_repo_file(repo, filename, missing_ok=True):
    path = repo_file_path(repo, filename)
    if missing_ok and not path.exists():
        return  # TODO: use path.unlink(missing_ok=True) (python3.8)
    path.unlink()


def remove_all_merge_repo_files(repo):
    """Deletes the following files (if they exist) - MERGE_HEAD, MERGE_BRANCH, MERGE_MSG, MERGE_INDEX"""
    remove_repo_file(repo, MERGE_HEAD)
    remove_repo_file(repo, MERGE_BRANCH)
    remove_repo_file(repo, MERGE_MSG)
    remove_repo_file(repo, MERGE_INDEX)


class RepoState:
    """
    A sno repository is defined as being in a certain state depending on the
    presence or absence of certain repository files. Certain sno commands are
    only valid in certain repository states.
    """

    NORMAL = "normal"
    MERGING = "merging"

    ALL_STATES = [NORMAL, MERGING]

    @classmethod
    def bad_state_message(cls, allowed_states, bad_state, command_extra):
        """Generates a generic message about a disallowed_state if no specific message is provided."""
        # Only two states exist right now so logic is pretty simple:
        cmd = click.get_current_context().command_path
        if command_extra:
            cmd = f"{cmd} {command_extra}"
        if bad_state == cls.MERGING:
            return (
                f'`{cmd}` does not work while the sno repo is in "merging" state.\n'
                'Use `sno merge --abort` to abandon the merge and get back to the previous state.'
            )
        return f'`{cmd}` only works when the sno repo is in "merging" state, but it is in "normal" state.'

    @classmethod
    def get_state(cls, repo):
        if repo_file_exists(repo, MERGE_HEAD):
            if not repo_file_exists(repo, MERGE_INDEX):
                raise NotFound(
                    'sno repo is in "merging" state, but required file MERGE_INDEX is missing.\n'
                    'Try `sno merge --abort` to return to a good state.'
                )
            return RepoState.MERGING
        return RepoState.NORMAL

    @classmethod
    def ensure_state(
        cls, repo, allowed_states, bad_state_message=None, command_extra=None
    ):
        repo_state = cls.get_state(repo)
        if repo_state not in allowed_states:
            if not bad_state_message:
                bad_state_message = cls.bad_state_message(
                    allowed_states, repo_state, command_extra
                )
            raise InvalidOperation(bad_state_message)
