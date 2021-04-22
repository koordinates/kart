from collections.abc import Iterable
from pathlib import Path

from .repo import SnoRepo, SnoRepoState
from .exceptions import InvalidOperation, NotFound, NO_REPOSITORY


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
        Returns the Kart repository at self.repo_path
        Raises an error if there isn't a Kart repository there.
        Ensures that the repository is in state NORMAL, since this is generally required.

        Accessing Context.repo.path ensures you have the path to an existing Kart repository.
        """
        return self.get_repo()

    def get_repo(
        self,
        allow_unsupported_versions=False,
        allowed_states=SnoRepoState.NORMAL,
        bad_state_message=None,
        command_extra=None,
    ):
        """
        Returns the Kart repository at self.repo_path
        Raises an error if there isn't a Kart repository there, or if the repository is
        not in one of the allowed states.
        """
        if not hasattr(self, "_repo"):
            try:
                self._repo = SnoRepo(self.repo_path)
            except NotFound:
                if self.user_repo_path:
                    message = "Not an existing Kart repository"
                    param_hint = "--repo"
                else:
                    message = "Current directory is not an existing Kart repository"
                    param_hint = None

                raise NotFound(message, exit_code=NO_REPOSITORY, param_hint=param_hint)

        if not allow_unsupported_versions:
            self._repo.ensure_supported_version()

        state = self._repo.state
        state_is_allowed = (
            state in allowed_states
            if isinstance(allowed_states, Iterable)
            else state == allowed_states
        )
        if not state_is_allowed:
            if not bad_state_message:
                bad_state_message = SnoRepoState.bad_state_message(
                    state, allowed_states, command_extra
                )
            raise InvalidOperation(bad_state_message)

        return self._repo

    def check_not_dirty(self, help_message=None):
        repo = self.get_repo(allowed_states=SnoRepoState.ALL_STATES)
        working_copy = repo.working_copy
        if working_copy:
            working_copy.check_not_dirty(help_message)
