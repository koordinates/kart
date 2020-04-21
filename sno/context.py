from pathlib import Path

import pygit2

from .exceptions import NotFound, NO_REPOSITORY


class Context(object):
    CURRENT_DIRECTORY = Path()

    def __init__(self):
        """The current repo_path string as set by the user, or None."""
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
        Accessing Context.repo.path ensures you have the path to an existing sno repository.
        """
        if not hasattr(self, "_repo"):
            try:
                self._repo = pygit2.Repository(str(self.repo_path))
            except pygit2.GitError:
                self._repo = None

        if not self._repo or not self._repo.is_bare:
            if self.user_repo_path:
                message = "Not an existing repository"
                param_hint = "repo"
            else:
                message = "Current directory is not an existing repository"
                param_hint = None

            raise NotFound(message, exit_code=NO_REPOSITORY, param_hint=param_hint)

        return self._repo
