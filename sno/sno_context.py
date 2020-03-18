import click
import pygit2
import os


class SnoContext(object):
    def __init__(self):
        self._repo_dir = None

    @property
    def repo_dir(self):
        """The path of the repository. Defaults to the current directory."""
        return self._repo_dir or os.curdir

    @repo_dir.setter
    def repo_dir(self, repo_dir):
        self._repo_dir = repo_dir
        if hasattr(self, "_repo"):
            del self._repo

    @property
    def repo_dir_is_set(self):
        """True if repo_dir has been explicitly set."""
        return self._repo_dir is not None

    @property
    def repo(self):
        """
        Returns the sno repository at repo_dir.
        Raises an error if there isn't a valid repo at repo_dir.
        """
        if not hasattr(self, "_repo"):
            try:
                self._repo = pygit2.Repository(self.repo_dir)
            except pygit2.GitError:
                self._repo = None

        if not self._repo or not self._repo.is_bare:
            if self.repo_dir_is_set:
                raise click.BadParameter("Not an existing repository", param_hint="--repo")
            else:
                raise click.UsageError("Current directory is not an existing repository")
        return self._repo
