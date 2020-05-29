import os

from pathlib import Path
import shlex
import shutil
import subprocess


from . import is_windows
from .exceptions import SubprocessError

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


def is_ongoing_merge(repo):
    if repo_file_exists(repo, MERGE_HEAD):
        if not repo_file_exists(repo, MERGE_INDEX):
            raise RuntimeError(
                "Repository is in merging state but MERGE_INDEX is missing. Try `sno merge --abort`"
            )
        return True
    return False


def remove_all_merge_repo_files(repo):
    """Deletes the following files (if they exist) - MERGE_HEAD, MERGE_BRANCH, MERGE_MSG, MERGE_INDEX"""
    remove_repo_file(repo, MERGE_HEAD)
    remove_repo_file(repo, MERGE_BRANCH)
    remove_repo_file(repo, MERGE_MSG)
    remove_repo_file(repo, MERGE_INDEX)
