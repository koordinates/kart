from pathlib import Path

import pygit2

from .exceptions import NotFound, NO_REPOSITORY


class Context(object):
    DEFAULT_REPO_PATH = Path()

    def __init__(self):
        self._repo_path = None

    @property
    def repo_path(self):
        """The path of the repository. Defaults to the current directory."""
        return self._repo_path or self.DEFAULT_REPO_PATH

    @repo_path.setter
    def repo_path(self, repo_path):
        if isinstance(repo_path, str):
            repo_path = Path(repo_path)
        self._repo_path = repo_path
        if hasattr(self, "_repo"):
            del self._repo

    @property
    def has_repo_path(self):
        return self._repo_path is not None

    @property
    def repo(self):
        """
        Returns the sno repository at repo_path.
        Raises an error if there isn't a valid repo at repo_path.
        """
        if not hasattr(self, "_repo"):
            try:
                self._repo = pygit2.Repository(str(self.repo_path))
            except pygit2.GitError:
                self._repo = None

        if not self._repo or not self._repo.is_bare:
            if self.has_repo_path:
                message = "Not an existing repository"
                param_hint = "repo"
            else:
                message = "Current directory is not an existing repository"
                param_hint = None

            raise NotFound(message, exit_code=NO_REPOSITORY, param_hint=param_hint)

        return self._repo
