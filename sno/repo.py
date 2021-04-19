import logging
import os
import re
import struct
import subprocess
import sys
from enum import Enum
from pathlib import Path

import click
import pygit2

from . import is_windows
from .cli_util import tool_environment
from .exceptions import (
    translate_subprocess_exit_code,
    InvalidOperation,
    NotFound,
    NO_REPOSITORY,
    UNSUPPORTED_VERSION,
)
from .repo_version import (
    SUPPORTED_REPO_VERSION,
    SUPPORTED_DATASET_CLASS,
    get_repo_version,
)
from .structure import RepoStructure
from .timestamps import tz_offset_to_minutes

L = logging.getLogger("sno.repo")


class SnoRepoFiles:
    """Useful files that are found in `repo.gitdir_path`"""

    # Standard git files:
    HEAD = "HEAD"
    INDEX = "index"
    COMMIT_EDITMSG = "COMMIT_EDITMSG"
    ORIG_HEAD = "ORIG_HEAD"
    MERGE_HEAD = "MERGE_HEAD"
    MERGE_MSG = "MERGE_MSG"

    # Sno-specific files:
    MERGE_INDEX = "MERGE_INDEX"
    MERGE_BRANCH = "MERGE_BRANCH"


class SnoRepoState(Enum):
    NORMAL = "normal"
    MERGING = "merging"

    @classmethod
    def bad_state_message(cls, bad_state, allowed_states, command_extra):
        """Generates a generic message about a disallowed_state if no specific message is provided."""
        # Only two states exist right now so logic is pretty simple:
        cmd = click.get_current_context().command_path
        if command_extra:
            cmd = f"{cmd} {command_extra}"
        if bad_state == SnoRepoState.MERGING:
            return (
                f'`{cmd}` does not work while the sno repo is in "merging" state.\n'
                "Use `sno merge --abort` to abandon the merge and get back to the previous state."
            )
        return f'`{cmd}` only works when the sno repo is in "merging" state, but it is in "normal" state.'


SnoRepoState.ALL_STATES = (SnoRepoState.NORMAL, SnoRepoState.MERGING)


class KartConfigKeys:
    """
    Kart specifig config variables found in repo.config
    (which is read from the file at `repo.gitdir_path / "config"`)
    """

    # Whichever of these two variables is written, controls whether the repo is kart branded or not.
    KART_REPOSTRUCTURE_VERSION = "kart.repostructure.version"
    SNO_REPOSITORY_VERSION = "sno.repository.version"

    KART_WORKINGCOPY_LOCATION = "kart.workingcopy.location"
    SNO_WORKINGCOPY_PATH = "sno.workingcopy.path"

    # This variable was also renamed, but when tidy-style repos were added - not during rebranding.
    CORE_BARE = "core.bare"  # Newer sno repos use the standard "core.bare" variable.
    SNO_WORKINGCOPY_BARE = (
        "sno.workingcopy.bare"  # Older sno repos use this custom variable instead.
    )

    BRANDED_REPOSTRUCTURE_VERSION_KEYS = {
        "kart": KART_REPOSTRUCTURE_VERSION,
        "sno": SNO_REPOSITORY_VERSION,
    }

    BRANDED_WORKINGCOPY_LOCATION_KEYS = {
        "kart": KART_WORKINGCOPY_LOCATION,
        "sno": SNO_WORKINGCOPY_PATH,
    }


def locked_git_index(extension_name):
    """
    Returns an empty index file, but extended with a required extension in the extensions section of the index binary
    format. (Not the file extension - the filename is simply "index", it has no file extension.)
    Causes all git commands that would involve the index or working copy to fail with "unsupported extension: NAME" -
    where name is "kart" or ".sno", giving the user a clue as to which command they *should* be using.
    in that sense it is "locked" to git. Various techniques can be used to unlock it if certain git functionality is
    needed - eg marking the repository as bare so it is ignored, or removing the unsupported extension.
    """
    assert isinstance(extension_name, bytes)
    assert len(extension_name) == 4
    first_char = extension_name[0]
    assert not (first_char >= ord("A") and first_char <= ord("Z"))

    GIT_INDEX_VERSION = 2
    BASE_EMPTY_GIT_INDEX = struct.pack(">4sII", b"DIRC", GIT_INDEX_VERSION, 0)

    # Extension name must not start with A-Z, therefore is a required extension.
    # See https://git-scm.com/docs/index-format

    extension = struct.pack(">4sI", extension_name, 0)
    data = BASE_EMPTY_GIT_INDEX + extension
    # Append checksum to the end.
    return data + pygit2.hash(data).raw


LOCKED_GIT_INDEX_CONTENTS = {
    "kart": locked_git_index(b"kart"),  # These extension names must be 4 bytes long
    "sno": locked_git_index(b".sno"),  # and they must not start with a capital letter
}


class SnoRepo(pygit2.Repository):
    """
    A valid pygit2.Repository, since all sno repos are also git repos - but with some added functionality.
    Ensures the git directory structure is one of the two styles supported by sno - "bare-style" or "tidy-style".
    For tidy-style, prevents worktree-related git commands from working by using a "locked git index".
    Helps set up sno specific config, and adds support for pathlib Paths.

    The two styles of Sno repos:
    Originally, all Sno repos were implemented as bare git repositorys. Some had GPKG working copies, some did not.
    Since they were bare git repositories, all the git internals were immediately visible inside the root directory -
    right alongside the GPKG. For this reason, they were kind of "untidy".

    Eventually, this style of repo was named a "bare-style" Sno repo. "Bare-style" Sno repo's are always implemented
    as bare git repositories, but they may or may not have a working copy, so they may or may not be actually "bare".

    A new style of sno repo was added - a "tidy-style" Sno repo. This type of Sno repo is implemented as a non-bare
    git repository, so the git internals are hidden in a ".sno" subfolder, leaving the root folder mostly empty as
    a place to put a GPKG file or similar. If a "tidy-style" Sno repo were to be reconfigured, it *could* have its
    working copy emoved and so be made bare. But going forward, "bare-style" Sno repos are supposed to be used for
    actual bare Sno repos, and "tidy-style" are supposed to be used for Sno repos with a working copy.

    Note: this is not enforced, especially since all legacy "bare-style" sno repos violate this assumption.
    """

    # There is an ongoing rename from Sno -> Kart. Kart branding is supported, but new repos are still Sno branded.
    BRANDING_FOR_NEW_REPOS = "sno"
    DIRNAME_FOR_NEW_REPOS = f".{BRANDING_FOR_NEW_REPOS}"

    # Directory names that we look in for a Kart / Sno repo - both continue to be supported.
    KART_DIR_NAMES = (".kart", ".sno")

    def __init__(self, path, *, validate=True):
        path = Path(path).resolve()

        for d in self.KART_DIR_NAMES:
            if (path / d).exists():
                path = path / d
                break

        try:
            super().__init__(
                str(path),
                # Instructs pygit2 not to look at the working copy or the index.
                pygit2.GIT_REPOSITORY_OPEN_BARE | pygit2.GIT_REPOSITORY_OPEN_FROM_ENV,
            )
        except pygit2.GitError:
            raise NotFound("Not an existing sno repository", exit_code=NO_REPOSITORY)

        self.gitdir_path = Path(self.path).resolve()

        if self.is_tidy_style:
            self.workdir_path = self.gitdir_path.parent.resolve()
        else:
            self.workdir_path = self.gitdir_path

        if validate:
            self.validate_bare_or_tidy_style()

    @classmethod
    def init_repository(
        cls, repo_root_path, wc_location=None, bare=False, initial_branch=None
    ):
        """
        Initialise a new sno repo. A sno repo is basically a git repo, except -
        - git internals are stored in .sno instead of .git
          (.git is a file that contains a reference to .sno, this is allowed by git)
        - datasets are stored in /.sno-dataset/ trees according to a particular dataset format version -
          see DATASETS_v2.md. But, this only matters when there are commits. At this stage they are not yet present.
        - there is a blob called sno.repository.version that contains the dataset format version number - but, this
          written in the first commit. At this stage it is not yet present.
        - there is property in the repo config called sno.repository.version that contains the dataset format version
          number, which is used until the sno.repository.version blob is written.
        - there are extra properties in the repo config about where / how the working copy is written.
        - the .sno/index file has been extended to stop git messing things up - see LOCKED_EMPTY_GIT_INDEX.
        """
        repo_root_path = repo_root_path.resolve()
        cls._ensure_exists_and_empty(repo_root_path)
        if not bare:
            from sno.working_copy.base import BaseWorkingCopy

            BaseWorkingCopy.check_valid_creation_location(
                wc_location, PotentialRepo(repo_root_path)
            )

        extra_args = []
        if initial_branch is not None:
            extra_args += [f"--initial-branch={initial_branch}"]
        if bare:
            # Create bare-style repo:
            sno_repo = cls._create_with_git_command(
                [
                    "git",
                    "init",
                    "--bare",
                    *extra_args,
                    str(repo_root_path),
                ],
                gitdir_path=repo_root_path,
            )
        else:
            # Create tidy-style repo:
            dot_kart_path = repo_root_path / cls.DIRNAME_FOR_NEW_REPOS
            dot_init_path = repo_root_path / ".init"

            sno_repo = cls._create_with_git_command(
                [
                    "git",
                    "init",
                    f"--separate-git-dir={dot_kart_path}",
                    *extra_args,
                    str(dot_init_path),
                ],
                gitdir_path=dot_kart_path,
                temp_workdir_path=dot_init_path,
            )
            sno_repo.lock_git_index()

        sno_repo.write_config(wc_location, bare)
        sno_repo.write_readme()
        sno_repo.activate()
        return sno_repo

    @classmethod
    def clone_repository(
        cls, clone_url, repo_root_path, clone_args, wc_location=None, bare=False
    ):
        repo_root_path = repo_root_path.resolve()
        cls._ensure_exists_and_empty(repo_root_path)
        if not bare:
            from sno.working_copy.base import BaseWorkingCopy

            BaseWorkingCopy.check_valid_creation_location(
                wc_location, PotentialRepo(repo_root_path)
            )

        if bare:
            sno_repo = cls._create_with_git_command(
                [
                    "git",
                    "clone",
                    "--bare",
                    *clone_args,
                    clone_url,
                    str(repo_root_path),
                ],
                gitdir_path=repo_root_path,
            )

        else:
            dot_kart_path = (
                repo_root_path if bare else repo_root_path / cls.DIRNAME_FOR_NEW_REPOS
            )
            dot_clone_path = repo_root_path / ".clone"

            sno_repo = cls._create_with_git_command(
                [
                    "git",
                    "clone",
                    "--no-checkout",
                    f"--separate-git-dir={dot_kart_path}",
                    *clone_args,
                    clone_url,
                    str(dot_clone_path),
                ],
                gitdir_path=dot_kart_path,
                temp_workdir_path=dot_clone_path,
            )
            sno_repo.lock_git_index()

        sno_repo.write_config(wc_location, bare)
        sno_repo.write_readme()
        sno_repo.activate()
        return sno_repo

    @classmethod
    def _create_with_git_command(cls, cmd, gitdir_path, temp_workdir_path=None):
        try:
            subprocess.check_call(cmd, env=tool_environment())
        except subprocess.CalledProcessError as e:
            sys.exit(translate_subprocess_exit_code(e.returncode))

        result = SnoRepo(gitdir_path, validate=False)

        # Tidy up temp workdir - this is created as a side effect of the git command.
        if temp_workdir_path is not None and temp_workdir_path.exists():
            if (temp_workdir_path / ".git").exists():
                (temp_workdir_path / ".git").unlink()
            temp_workdir_path.rmdir()

        return result

    def write_config(
        self,
        wc_location=None,
        bare=False,
    ):
        # Whichever of these variable is written, controls whether this repo is kart branded or not.
        version_key = KartConfigKeys.BRANDED_REPOSTRUCTURE_VERSION_KEYS[
            self.BRANDING_FOR_NEW_REPOS
        ]
        self.config[version_key] = str(SUPPORTED_REPO_VERSION)

        # Bare-style sno repos are always implemented as bare git repos:
        if self.is_bare_style:
            self.config["core.bare"] = True
        # Force writing to reflogs:
        self.config["core.logAllRefUpdates"] = "always"

        # Write working copy config:
        from sno.working_copy.base import BaseWorkingCopy

        BaseWorkingCopy.write_config(self, wc_location, bare)

    def ensure_supported_version(self):
        from .cli import get_version

        if self.version != SUPPORTED_REPO_VERSION:
            message = (
                f"This Sno repo uses Datasets v{self.version}, "
                f"but Sno {get_version()} only supports Datasets v{SUPPORTED_REPO_VERSION}.\n"
            )
            if self.version < SUPPORTED_REPO_VERSION:
                message += "Use `sno upgrade SOURCE DEST` to upgrade this repo to the supported version."
            else:
                message += "Get the latest version of Sno to work with this repo."
            raise InvalidOperation(message, exit_code=UNSUPPORTED_VERSION)

    def write_readme(self):
        readme_filename = f"{self.branding.upper()}_README.txt"
        readme_text = self.get_readme_text(self.is_bare_style, self.branding)
        try:
            self.workdir_file(readme_filename).write_text(readme_text)
        except Exception as e:
            L.warn(e)

    @classmethod
    def get_readme_text(cls, is_bare_style, branding):
        text = (
            cls.KART_BARE_STYLE_README if is_bare_style else cls.KART_TIDY_STYLE_README
        )
        text = "\n".join(text)
        if branding == "sno":
            text = (
                text.replace(
                    KartConfigKeys.KART_WORKINGCOPY_LOCATION,
                    KartConfigKeys.SNO_WORKINGCOPY_PATH,
                )
                .replace("kart.global", "sno.earth")
                .replace("Kart", "Sno")
                .replace("kart", "sno")
            )
        return text

    def activate(self):
        """
        We create new+tidy repos in .kart/ but we don't write the .git file pointing to .kart/ until everything
        else is ready, and until that file is written, git or sno commands won't find the repo.
        So, if creation fails, the result will be something that doesn't work at all, not something that half
        works but is also half corrupted.
        """
        if self.is_bare_style:
            # Bare-style repos are always activated - since all the files are right there in the root directory,
            # we can't reveal them by writing the .git file. So, no action is required here.
            return

        dot_git_path = self.workdir_path / ".git"
        dot_kart_path = self.gitdir_path
        dot_kart_name = dot_kart_path.stem
        # .kart is linked from .git at this point, which means git (or kart) can find it
        # and so the repository is activated (ie, sno or git commands will work):
        dot_git_path.write_text(f"gitdir: {dot_kart_name}\n", encoding="utf-8")

        if is_windows:
            # Hide .git and .kart
            # Best effort: if it doesn't work for some reason, continue anyway.
            subprocess.call(["attrib", "+h", str(dot_git_path)], env=tool_environment())
            subprocess.call(
                ["attrib", "+h", str(dot_kart_path)], env=tool_environment()
            )

    @property
    def branding(self):
        if KartConfigKeys.KART_REPOSTRUCTURE_VERSION in self.config:
            return "kart"
        elif KartConfigKeys.SNO_REPOSITORY_VERSION in self.config:
            return "sno"
        # Pre V2 repos (no longer fully supported - need to be upgraded) are always Sno branded.
        if self.BRANDING_FOR_NEW_REPOS != "sno" and self.version < 2:
            return "sno"
        # New repo, config is not yet written. Refer to BRANDING_FOR_NEW_REPOS
        return self.BRANDING_FOR_NEW_REPOS

    @property
    def is_kart_branded(self):
        return self.branding == "kart"

    @property
    def is_bare_style(self):
        """Bare-style Kart repos are bare git repos. They are not "tidy": all of the git internals are visible."""
        return self.gitdir_path.stem not in self.KART_DIR_NAMES

    @property
    def is_tidy_style(self):
        """Tidy-style Kart repos are "tidy": they hide the git internals in a ".kart" directory."""
        return self.gitdir_path.stem in self.KART_DIR_NAMES

    def validate_bare_or_tidy_style(self):
        if self.is_bare_style and not super().is_bare:
            raise NotFound(
                "Selected repo isn't a bare-style or tidy-style sno repo. Perhaps a git repo?",
                exit_code=NO_REPOSITORY,
            )

    @property
    def REPOSTRUCTURE_VERSION_KEY(self):
        return KartConfigKeys.BRANDED_REPOSTRUCTURE_VERSION_KEYS[self.branding]

    @property
    def WORKINGCOPY_LOCATION_KEY(self):
        return KartConfigKeys.BRANDED_WORKINGCOPY_LOCATION_KEYS[self.branding]

    @property
    def BARE_CONFIG_KEY(self):
        """
        Return the config key we can check to see if the repo is actually bare,
        given that all bare-style Sno repos have core.bare = True regardless of whether they have a working copy.
        """
        return (
            KartConfigKeys.SNO_WORKINGCOPY_BARE
            if self.is_bare_style
            else KartConfigKeys.CORE_BARE
        )

    @property
    def version(self):
        """Returns the sno repository version - eg 2 for 'Datasets V2' See DATASETS_v2.md"""
        return get_repo_version(self)

    @property
    def workingcopy_location(self):
        """Return the path to the sno working copy, if one exists."""
        repo_cfg = self.config
        location_key = self.WORKINGCOPY_LOCATION_KEY
        return repo_cfg[location_key] if location_key in repo_cfg else None

    @property
    def is_bare(self):
        """
        True if this sno repo is genuinely bare - it has no sno working copy.
        The repo may or may not also be a bare git repository - this is an implementation detail.
        That information can be found super().is_bare
        """
        repo_cfg = self.config
        bare_key = self.BARE_CONFIG_KEY
        return repo_cfg.get_bool(bare_key) if bare_key in repo_cfg else False

    def lock_git_index(self):
        index_contents = LOCKED_GIT_INDEX_CONTENTS[self.branding]
        (self.gitdir_path / SnoRepoFiles.INDEX).write_bytes(index_contents)

    @property
    def state(self):
        merge_head = self.gitdir_file(SnoRepoFiles.MERGE_HEAD).exists()
        merge_index = self.gitdir_file(SnoRepoFiles.MERGE_INDEX).exists()
        if merge_head and not merge_index:
            raise NotFound(
                'sno repo is in "merging" state, but required file MERGE_INDEX is missing.\n'
                "Try `sno merge --abort` to return to a good state."
            )
        return SnoRepoState.MERGING if merge_head else SnoRepoState.NORMAL

    def structure(self, refish="HEAD"):
        """Get the structure of this Sno repository at a particular revision."""
        self.ensure_supported_version()
        return RepoStructure(self, refish, dataset_class=SUPPORTED_DATASET_CLASS)

    def datasets(self, refish="HEAD"):
        """
        Get the datasets of this Sno repository at a particular revision.
        Equivalent to: self.structure(refish).datasets
        """
        return self.structure(refish).datasets

    @property
    def working_copy(self):
        """Return the working copy of this Sno repository, or None if it it does not exist."""
        if not hasattr(self, "_working_copy"):
            self._working_copy = self.get_working_copy()
        return self._working_copy

    @working_copy.deleter
    def working_copy(self):
        wc = self.get_working_copy(allow_invalid_state=True)
        if wc:
            wc.delete()
        del self._working_copy

    def get_working_copy(
        self,
        allow_uncreated=False,
        allow_invalid_state=False,
        allow_unconnectable=False,
    ):
        from sno.working_copy.base import BaseWorkingCopy

        return BaseWorkingCopy.get(
            self,
            allow_uncreated=allow_uncreated,
            allow_invalid_state=allow_invalid_state,
            allow_unconnectable=allow_unconnectable,
        )

    def del_config(self, key):
        config = self.config
        if key in config:
            del config[key]

    def gc(self, *args):
        """Runs git-gc on the sno repository."""
        try:
            args = ["git", "-C", self.path, "gc", *args]
            subprocess.check_call(args, env=tool_environment())
        except subprocess.CalledProcessError as e:
            sys.exit(translate_subprocess_exit_code(e.returncode))

    def _ensure_exists_and_empty(dir_path):
        if dir_path.exists() and any(dir_path.iterdir()):
            raise InvalidOperation(f'"{dir_path}" isn\'t empty')
        elif not dir_path.exists():
            dir_path.mkdir(parents=True)

    @property
    def head_commit(self):
        """
        Returns the commit at the current repo HEAD. Returns None if there is no commit at HEAD - ie, head_is_unborn.
        """
        return None if self.head_is_unborn else self.head.peel(pygit2.Commit)

    @property
    def head_tree(self):
        """
        Returns the tree at the current repo HEAD. Returns None if there is no tree at HEAD - ie, head_is_unborn.
        """
        return None if self.head_is_unborn else self.head.peel(pygit2.Tree)

    @property
    def head_branch(repo):
        """
        Returns the branch that HEAD is currently on. Returns None if head is not on a branch - ie, head_is_detached.
        """
        return None if repo.head_is_detached else repo.references["HEAD"].target

    @property
    def head_branch_shorthand(repo):
        """
        Returns the shorthand for the branch that HEAD is currently on.
        Returns None if head is not on a branch - ie, head_is_detached.
        """
        if repo.head_is_detached:
            return None
        return repo.references["HEAD"].target.rsplit("/", 1)[-1]

    _GIT_VAR_OUTPUT_RE = re.compile(
        r"^(?P<name>.*) <(?P<email>[^>]*)> (?P<time>\d+) (?P<offset>[+-]?\d+)$"
    )

    def _signature(self, person_type, **overrides):
        # 'git var' lets us use the environment variables to
        # control the user info, e.g. GIT_AUTHOR_DATE.
        # libgit2/pygit2 doesn't handle those env vars at all :(
        env = os.environ.copy()

        name = overrides.pop("name", None)
        if name is not None:
            env[f"GIT_{person_type}_NAME"] = name

        email = overrides.pop("email", None)
        if email is not None:
            env[f"GIT_{person_type}_EMAIL"] = email

        output = subprocess.check_output(
            ["git", "var", f"GIT_{person_type}_IDENT"],
            cwd=self.path,
            encoding="utf8",
            env=tool_environment(env),
        )
        m = self._GIT_VAR_OUTPUT_RE.match(output)
        kwargs = m.groupdict()
        kwargs["time"] = int(kwargs["time"])
        kwargs["offset"] = tz_offset_to_minutes(kwargs["offset"])
        kwargs.update(overrides)
        return pygit2.Signature(**kwargs)

    def author_signature(self, **overrides):
        return self._signature("AUTHOR", **overrides)

    def committer_signature(self, **overrides):
        return self._signature("COMMITTER", **overrides)

    def gitdir_file(self, rel_path):
        return self.gitdir_path / rel_path

    def workdir_file(self, rel_path):
        return self.workdir_path / rel_path

    def write_gitdir_file(self, rel_path, text):
        assert isinstance(text, str)
        if not text.endswith("\n"):
            text += "\n"
        self.gitdir_file(rel_path).write_text(text, encoding="utf-8")

    def read_gitdir_file(self, rel_path, missing_ok=False, strip=False):
        path = self.gitdir_file(rel_path)
        if missing_ok and not path.exists():
            return None
        result = path.read_text(encoding="utf-8")
        if strip:
            result = result.strip()
        return result

    def remove_gitdir_file(self, rel_path, missing_ok=True):
        path = self.gitdir_file(rel_path)
        if missing_ok and not path.exists():
            return
        path.unlink()

    KART_COMMON_README = [
        "",
        "kart status",
        "",
        'It may simply output "Empty repository. Use kart import to add some data".',
        "Follow the tutorial at http://kart.global/ for help getting started with Kart.",
        "",
        "Some more helpful commands for getting a broad view of what a Kart repository",
        "contains are:",
        "",
        "kart log      - show the history of what has been committed to this repository.",
        "kart data ls  - show the names of every dataset in this repository.",
        "",
        "This directory is the default location where Kart puts the repository's working",
        "copy, which is created as soon as there is some data to put in it. However",
        "the working copy can also be configured to be somewhere else, and may not be",
        "a file at all. To see the working copy's location, run this command:",
        "",
        "kart config kart.workingcopy.location",
        "",
        "",
    ]

    KART_TIDY_STYLE_README = [
        "This directory is a Kart repository.",
        "",
        "It may look empty, but every version of every datasets that this repository",
        'contains is stored in Kart\'s internal format in the ".kart" hidden subdirectory.',
        "To check if a directory is a Kart repository and see what is stored, run:",
    ] + KART_COMMON_README

    KART_BARE_STYLE_README = [
        "This directory is a Kart repository.",
        "",
        "In this repository, the internals are visible - in files and in subdirectories",
        'like "HEAD", "objects" and "refs". These are best left untouched. Instead, use',
        "Kart commands to interact with the repository. To check if a directory is a Kart",
        "repository and see what is stored, run:",
    ] + KART_COMMON_README


class PotentialRepo:
    """
    A repo that doesn't yet exist, but which we are considering whether to create.
    Used for calling code that needs a repo as context, for example:
    >>> WorkingCopy.check_valid_creation_location(location, repo)
    """

    def __init__(self, workdir_path):
        self.workdir_path = workdir_path
        self.branding = SnoRepo.BRANDING_FOR_NEW_REPOS
        self.is_kart_branded = self.branding == "kart"
