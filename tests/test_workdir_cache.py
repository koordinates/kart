"""
Unit tests for the FileSystemWorkingCopy workdir-index plumbing that
detect dirty files via git's mtime optimisation.

The higher-level classification (dirty_attachment_paths, tile-vs-tabular dataset handling) is
exercised end-to-end by the test_status_attached_files_* and test_checkout/test_diff attachment
tests, which run against real working copies.
"""
import os
import subprocess
from unittest.mock import MagicMock

import pygit2


def make_tidy_repo(workdir):
    """
    Set up a directory like a tidy-style kart repo:
    - workdir/.git -> workdir/.kart (pointer file)
    - workdir/.kart/ is a real git repo with bare=false (has a worktree)
    Returns a mock KartRepo suitable for a FileSystemWorkingCopy.
    """
    git_dir = workdir / ".kart"
    env = os.environ.copy()
    env.pop("GIT_INDEX_FILE", None)  # Don't let kart's global override interfere
    subprocess.check_call(
        ["git", "init", str(workdir)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    (workdir / ".git").rename(git_dir)
    (workdir / ".git").write_text("gitdir: .kart\n")

    repo = MagicMock()
    repo.workdir_path = workdir
    repo.gitdir_file = lambda name: git_dir / name
    return repo


def make_workdir_index(repo):
    """A FileSystemWorkingCopy bound to the mock repo, used to exercise its workdir-index
    plumbing in isolation."""
    from kart.workdir import FileSystemWorkingCopy

    return FileSystemWorkingCopy(repo)


def create_index(wi):
    if not wi.index_path.is_file():
        index = pygit2.Index(str(wi.index_path))
        index._repo = wi.repo
        index.write()


class TestWorkdirIndexPlumbing:
    def test_create_index(self, tmp_path):
        repo = make_tidy_repo(tmp_path)
        wi = make_workdir_index(repo)
        assert not wi.index_path.exists()
        create_index(wi)
        assert wi.index_path.exists()

    def test_add_paths_to_index(self, tmp_path):
        repo = make_tidy_repo(tmp_path)
        (tmp_path / "LICENSE.txt").write_text("MIT License\n")
        wi = make_workdir_index(repo)
        create_index(wi)
        wi.add_paths_to_index(["LICENSE.txt"])

        idx = pygit2.Index(str(wi.index_path))
        assert any(e.path == "LICENSE.txt" for e in idx)

    def test_git_diff_paths_empty_when_no_changes(self, tmp_path):
        repo = make_tidy_repo(tmp_path)
        (tmp_path / "LICENSE.txt").write_text("MIT License\n")
        wi = make_workdir_index(repo)
        create_index(wi)
        wi.add_paths_to_index(["LICENSE.txt"])
        assert wi._git_diff_paths() == []

    def test_git_diff_paths_detects_modification(self, tmp_path):
        repo = make_tidy_repo(tmp_path)
        test_file = tmp_path / "LICENSE.txt"
        test_file.write_text("MIT License\n")
        wi = make_workdir_index(repo)
        create_index(wi)
        wi.add_paths_to_index(["LICENSE.txt"])

        test_file.write_text("Modified!\n")  # change after indexing
        assert "LICENSE.txt" in wi._git_diff_paths()

    def test_git_ls_others_paths_detects_untracked(self, tmp_path):
        repo = make_tidy_repo(tmp_path)
        wi = make_workdir_index(repo)
        create_index(wi)
        (tmp_path / "NOTES.txt").write_text("Notes\n")
        assert "NOTES.txt" in wi._git_ls_others_paths()

    def test_dirty_paths_combines_modified_and_untracked(self, tmp_path):
        repo = make_tidy_repo(tmp_path)
        tracked = tmp_path / "LICENSE.txt"
        tracked.write_text("Original\n")
        wi = make_workdir_index(repo)
        create_index(wi)
        wi.add_paths_to_index(["LICENSE.txt"])

        tracked.write_text("Modified!\n")
        (tmp_path / "NOTES.txt").write_text("Notes\n")

        dirty = wi.dirty_paths()
        assert "LICENSE.txt" in dirty
        assert "NOTES.txt" in dirty
