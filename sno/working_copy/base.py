from sno.repository_version import get_repo_version

SNO_WORKINGCOPY_PATH = "sno.workingcopy.path"


class WorkingCopyDirty(Exception):
    pass


class WorkingCopy:
    VALID_VERSIONS = (1, 2)

    @classmethod
    def get(cls, repo, create_if_missing=False):
        from .gpkg import WorkingCopy_GPKG_1, WorkingCopy_GPKG_2

        if create_if_missing:
            cls.ensure_config_exists(repo)

        repo_cfg = repo.config
        path_key = SNO_WORKINGCOPY_PATH
        if path_key not in repo_cfg:
            return None

        path = repo_cfg[path_key]
        full_path = repo.workdir_path / path
        if not full_path.is_file() and not create_if_missing:
            return None

        version = get_repo_version(repo)
        if version not in cls.VALID_VERSIONS:
            raise NotImplementedError(f"Working copy version: {version}")
        if version < 2:
            return WorkingCopy_GPKG_1(repo, path)
        else:
            return WorkingCopy_GPKG_2(repo, path)

    @classmethod
    def ensure_config_exists(cls, repo):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        is_bare = bare_key in repo_cfg and repo_cfg.get_bool(bare_key)
        if is_bare:
            return

        path_key = SNO_WORKINGCOPY_PATH
        path = repo_cfg[path_key] if path_key in repo_cfg else None
        if path is None:
            cls.write_config(repo, None, False)

    @classmethod
    def write_config(cls, repo, path=None, bare=False):
        repo_cfg = repo.config
        bare_key = repo.BARE_CONFIG_KEY
        path_key = SNO_WORKINGCOPY_PATH

        if bare:
            repo_cfg[bare_key] = True
            repo.del_config(path_key)
        else:
            path = path or cls.default_path(repo)
            repo_cfg[bare_key] = False
            repo_cfg[path_key] = str(path)

    @classmethod
    def default_path(cls, repo):
        """Returns `example.gpkg` for a sno repo in a directory named `example`."""
        stem = repo.workdir_path.stem
        return f"{stem}.gpkg"

    class Mismatch(ValueError):
        def __init__(self, working_copy_tree_id, match_tree_id):
            self.working_copy_tree_id = working_copy_tree_id
            self.match_tree_id = match_tree_id

        def __str__(self):
            return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"
