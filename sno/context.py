from pathlib import Path

import pygit2

from .exceptions import NotFound, NO_REPOSITORY
from .repo_files import RepoState
from .structure import RepositoryStructure


class Context(object):
    """
    The context object accessible to all commands.

    Repo / repo_path is nuanced:
    Context.user_repo_path gives the path the user typed, if any - a str, defaults to None.
    Context.repo_path gives the implied repo path - a pathlib.Path, defaults to current directory.
    Context.repo.path - same as Context.repo_path but it raises a NotFound error if no repo is found.

    Context.repo ensures that the repo is in NORMAL state, since this is required by most
    commands. To allow other states, use the more configurable Context.get_repo(...)
    """

    CURRENT_DIRECTORY = Path()

    def __init__(self):
        # The current repo_path string as set by the user, or None.
        self.user_repo_path = None

    @property
    def repo_path(self):
        """The impled repo path as a Path - defaults to current directory."""
        if self.user_repo_path is None:
            return self.CURRENT_DIRECTORY
        else:
            return Path(self.user_repo_path)

    @property
    def repo(self):
        """
        Returns the sno repository at self.repo_path
        Raises an error if there isn't a sno repository there.
        Ensures that the repository is in state NORMAL, since this is generally required.

        Accessing Context.repo.path ensures you have the path to an existing sno repository.
        """
        return self.get_repo()

    def get_repo(
        self,
        allowed_states=(RepoState.NORMAL,),
        bad_state_message=None,
        command_extra=None,
    ):
        """
        Returns the sno repository at self.repo_path
        Raises an error if there isn't a sno repository there, or if the repository is
        not in one of the allowed states.
        """
        if not hasattr(self, "_repo"):
            try:
                self._repo = pygit2.Repository(str(self.repo_path))
            except pygit2.GitError:
                self._repo = None

        if not self._repo or not self._repo.is_bare:
            if self.user_repo_path:
                message = "Not an existing repository"
                param_hint = "--repo"
            else:
                message = "Current directory is not an existing repository"
                param_hint = None

            raise NotFound(message, exit_code=NO_REPOSITORY, param_hint=param_hint)

        RepoState.ensure_state(
            self._repo, allowed_states, bad_state_message, command_extra
        )
        return self._repo

    def check_not_dirty(self, help_message=None):
        repo = self.get_repo(allowed_states=RepoState.ALL_STATES)
        repo_structure = RepositoryStructure(repo)
        working_copy = repo_structure.working_copy
        if working_copy:
            working_copy.check_not_dirty(help_message)
