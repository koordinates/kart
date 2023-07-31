import contextlib
import os
import click
import logging
from enum import Enum, auto
import functools
from pathlib import Path
import shutil
import sys
from kart.structure import RepoStructure

import pygit2
import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import CreateTable


from kart import diff_util
from kart.diff_util import get_file_diff
from kart.diff_structs import Delta, DatasetDiff
from kart.exceptions import (
    NotFound,
    SubprocessError,
    NO_WORKING_COPY,
    translate_subprocess_exit_code,
)
from kart.lfs_util import get_local_path_from_lfs_hash, dict_to_pointer_file_bytes
from kart.lfs_commands import fetch_lfs_blobs_for_pointer_files
from kart.key_filters import RepoKeyFilter
from kart.output_util import InputMode, get_input_mode
from kart.reflink_util import try_reflink
from kart.sqlalchemy import TableSet
from kart.sqlalchemy.sqlite import sqlite_engine
from kart import subprocess_util as subprocess
from kart.tile import ALL_TILE_DATASET_TYPES
from kart.tile.tile_dataset import TileDataset
from kart.tile.tilename_util import (
    remove_any_tile_extension,
    case_insensitive,
    PAM_SUFFIX,
)
from kart.working_copy import WorkingCopyPart

L = logging.getLogger("kart.workdir")


class FileSystemWorkingCopyStatus(Enum):
    """Different status that a file-system working copy can have."""

    UNCREATED = auto()
    PARTIALLY_CREATED = auto()
    CREATED = auto()


class WorkdirKartTables(TableSet):
    """Tables for Kart-specific metadata as it is stored in the workdir, using a sqlite DB."""

    def __init__(self):
        super().__init__()

        self.kart_state = sa.Table(
            "kart_state",
            self.sqlalchemy_metadata,
            sa.Column("table_name", sa.Text, nullable=False, primary_key=True),
            sa.Column("key", sa.Text, nullable=False, primary_key=True),
            sa.Column("value", sa.Text, nullable=False),
        )


# Makes it so WorkdirKartTables table definitions are also accessible at the WorkdirKartTables class itself:
WorkdirKartTables.copy_tables_to_class()


class FileSystemWorkingCopy(WorkingCopyPart):
    """
    A working copy on the filesystem - also referred to as the "workdir" for brevity.
    Much like Git's working copy but with some key differences:
    - doesn't have an index for staging
    - but does have an index just for tracking which files are dirty, at .kart/workdir-index
    - the files in the workdir aren't necessarily in the exact same place or same format as the
      files in the ODB, so the easiest way to check which ones are dirty is to compare them to the index:
      comparing them to the ODB would involve re-adapting the ODB format to the workdir format.
    - also has a sqlite DB for tracking kart_state, just as the tabular working copies do.
      This is at .kart/workdir-state.db.
    """

    @property
    def WORKING_COPY_TYPE_NAME(self):
        """Human readable name of this part of the working copy, eg "PostGIS"."""
        return "file-system"

    def __str__(self):
        return "file-system working copy"

    def __init__(self, repo):
        super().__init__()

        self.repo = repo
        self.path = repo.workdir_path

        self.index_path = repo.gitdir_file("workdir-index")
        self.state_path = repo.gitdir_file("workdir-state.db")

        self._required_paths = [self.index_path, self.state_path]

        self.kart_tables = WorkdirKartTables()

    @classmethod
    def get(
        self,
        repo,
        allow_uncreated=False,
        allow_invalid_state=False,
    ):
        wc = FileSystemWorkingCopy(repo)

        if allow_uncreated and allow_invalid_state:
            return wc

        status = wc.status()
        if not allow_invalid_state:
            wc.check_valid_state(status)

        if not allow_uncreated and status == FileSystemWorkingCopyStatus.UNCREATED:
            wc = None

        return wc

    def status(self):
        existing_files = [f for f in self._required_paths if f.is_file()]
        if not existing_files:
            return FileSystemWorkingCopyStatus.UNCREATED
        if existing_files == self._required_paths:
            return FileSystemWorkingCopyStatus.CREATED
        return FileSystemWorkingCopyStatus.PARTIALLY_CREATED

    def check_valid_state(self, status=None):
        if status is None:
            status = self.status()

        if status == FileSystemWorkingCopyStatus.PARTIALLY_CREATED:
            missing_file = next(
                iter([f for f in self._required_paths if not f.is_file()])
            )
            raise NotFound(
                f"File system working copy is corrupt - {missing_file} is missing",
                exit_code=NO_WORKING_COPY,
            )

    COPY_ON_WRITE_WARNING = [
        "Copy-on-write is not supported on this filesystem.",
        "Currently Kart must create two copies of tiles to support full distributed version control features.",
        "For more info, see https://docs.kartproject.org/en/latest/pages/git_lfs.html#disk-usage",
    ]

    def check_if_reflink_okay(self):
        """Makes sure that reflink is working, or failing that, that the user has been made aware that reflink is not working."""
        if self.repo.get_config_str("kart.reflink.warningShown"):
            # User has been warned that reflink is not supported and they have okayed it.
            return True

        import reflink

        if reflink.supported_at(self.path):
            # Reflink is supported - no need to warn user about anything.
            return True

        click.echo("\n".join(self.COPY_ON_WRITE_WARNING), err=True)

        if get_input_mode() is not InputMode.INTERACTIVE:
            # Can't ask the user what they think - we've logged a warning, carry on regardless.
            return True

        if not click.confirm("Do you wish to continue?"):
            click.echo("Aborting file-system working copy checkout.", err=True)
            return False

        try:
            self.repo.config.get_global_config()["kart.reflink.warningShown"] = True
        except Exception:
            self.repo.config["kart.reflink.warningShown"] = True
        return True

    def create_and_initialise(self):
        index = pygit2.Index(str(self.index_path))
        index._repo = self.repo
        index.write()
        engine = sqlite_engine(self.state_path)
        sm = sessionmaker(bind=engine)
        with sm() as s:
            s.execute(CreateTable(self.kart_tables.kart_state, if_not_exists=True))

    def delete(self):
        """Deletes the index file and state table, and attempts to clean up any datasets in the workdir itself."""
        datasets = self.repo.datasets(
            self.get_tree_id(), filter_dataset_type=ALL_TILE_DATASET_TYPES
        )
        self.delete_datasets_from_workdir(
            datasets, workdir_index=None, track_changes_as_dirty=True
        )

        if self.index_path.is_file():
            self.index_path.unlink()
        if self.state_path.is_file():
            self.state_path.unlink()

    @contextlib.contextmanager
    def state_session(self):
        """
        Context manager for database sessions, yields a connection object inside a transaction

        Calling again yields the _same_ session, the transaction/etc only happen in the outer one.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.state_session")

        if hasattr(self, "_session"):
            # Inner call - reuse existing session.
            L.debug("session: existing...")
            yield self._session
            L.debug("session: existing/done")
            return

        L.debug("session: new...")
        engine = sqlite_engine(self.state_path)
        sm = sessionmaker(bind=engine)
        self._session = sm()
        try:
            # TODO - use tidier syntax for opening transactions from sqlalchemy.
            yield self._session
            self._session.commit()
        except Exception:
            self._session.rollback()
            raise
        finally:
            self._session.close()
            del self._session
            L.debug("session: new/done")

    @contextlib.contextmanager
    def workdir_index_session(self):
        """
        Context manager for opening and updating the workdir-index file.
        Automatically writes it on close, as long as no exception is thrown.

        Calling again yields the _same_ session, writing only happens when the outer one is closed.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.workdir_index_session")

        if hasattr(self, "_workdir_index"):
            # Inner call - reuse existing session.
            L.debug("workdir_index_session: existing...")
            yield self._workdir_index
            L.debug("workdir_index_session: existing/done")
            return

        L.debug("workdir_index_session: new...")
        self._workdir_index = pygit2.Index(self.index_path)

        try:
            yield self._workdir_index
            self._workdir_index.write()
        finally:
            del self._workdir_index
            L.debug("workdir_index_session: new/done")

    def is_dirty(self):
        """
        Returns True if there are uncommitted changes in the working copy,
        or False otherwise.
        """
        if self.get_tree_id() is None:
            return False

        datasets = self.repo.datasets(
            self.get_tree_id(), filter_dataset_type=ALL_TILE_DATASET_TYPES
        )
        workdir_diff_cache = self.workdir_diff_cache()
        for dataset in datasets:
            ds_tiles_path_pattern = dataset.get_tile_path_pattern(
                parent_path=dataset.path
            )
            for tile_path in workdir_diff_cache.dirty_paths_for_dataset(dataset):
                if ds_tiles_path_pattern.fullmatch(tile_path):
                    return True
        return False

    def _is_head(self, commit_or_tree):
        return commit_or_tree.peel(pygit2.Tree) == self.repo.head_tree

    def reset(
        self,
        commit_or_tree,
        *,
        repo_key_filter=RepoKeyFilter.MATCH_ALL,
        track_changes_as_dirty=False,
        rewrite_full=False,
        quiet=False,
    ):
        """
        Resets the working copy to the given target-tree (or the tree pointed to by the given target-commit).

        Any existing changes which match the repo_key_filter will be discarded. Existing changes which do not
        match the repo_key_filter will be kept.

        If track_changes_as_dirty=False (the default) the tree ID in the kart_state table gets set to the
        new tree ID and the tracking table is left empty. If it is True, the old tree ID is kept and the
        tracking table is used to record all the changes, so that they can be committed later.

        If rewrite_full is True, then every dataset currently being tracked will be dropped, and all datasets
        present at commit_or_tree will be written from scratch using write_full.
        Since write_full honours the current repo spatial filter, this also ensures that the working copy spatial
        filter is up to date.
        """

        if rewrite_full:
            # These aren't supported when we're doing a full rewrite.
            assert repo_key_filter.match_all and not track_changes_as_dirty

        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        if commit_or_tree is not None:
            target_tree = commit_or_tree.peel(pygit2.Tree)
            target_tree_id = target_tree.id.hex
        else:
            target_tree_id = target_tree = None

        # base_tree is the tree the working copy is based on.
        # If the working copy exactly matches base_tree, then it is clean,
        # and the workdir-index will also exactly match the workdir contents.

        base_tree_id = self.get_tree_id()
        base_tree = self.repo[base_tree_id] if base_tree_id else None
        repo_tree_id = self.repo.head_tree.hex if self.repo.head_tree else None

        L.debug(
            "reset(): WorkingCopy base_tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree: %s",
            base_tree_id,
            repo_tree_id,
            target_tree_id,
        )
        L.debug("reset(): track_changes_as_dirty=%s", track_changes_as_dirty)

        base_datasets = self.repo.datasets(
            base_tree,
            repo_key_filter=repo_key_filter,
            filter_dataset_type=ALL_TILE_DATASET_TYPES,
        ).datasets_by_path()
        if base_tree == target_tree:
            target_datasets = base_datasets
        else:
            target_datasets = self.repo.datasets(
                target_tree,
                repo_key_filter=repo_key_filter,
                filter_dataset_type=ALL_TILE_DATASET_TYPES,
            ).datasets_by_path()

        ds_inserts = target_datasets.keys() - base_datasets.keys()
        ds_deletes = base_datasets.keys() - target_datasets.keys()
        ds_updates = base_datasets.keys() & target_datasets.keys()

        if rewrite_full:
            for ds_path in ds_updates:
                ds_inserts.add(ds_path)
                ds_deletes.add(ds_path)
            ds_updates.clear()

        structural_changes = ds_inserts | ds_deletes
        is_new_target_tree = base_tree != target_tree
        self._check_for_unsupported_structural_changes(
            structural_changes,
            is_new_target_tree,
            track_changes_as_dirty,
            repo_key_filter,
        )

        pointer_files_to_fetch = set()
        workdir_diff_cache = self.workdir_diff_cache()
        update_diffs = {}

        # First pass - make sure the LFS blobs are present in the local LFS cache:
        # - For the datasets that will be inserted (written from scratch):
        for ds_path in ds_inserts:
            pointer_files_to_fetch.update(
                blob.hex
                for blob in target_datasets[ds_path].tile_pointer_blobs(
                    self.repo.spatial_filter
                )
            )

        # - For the datasets that will be updated:
        for ds_path in ds_updates:
            update_diffs[ds_path] = self._diff_to_reset(
                ds_path,
                base_datasets,
                target_datasets,
                workdir_diff_cache,
                repo_key_filter[ds_path],
            )
            pointer_files_to_fetch.update(
                blob.hex
                for blob in self._list_new_pointer_blobs_for_diff(
                    update_diffs[ds_path], target_datasets[ds_path]
                )
            )

        # We fetch the LFS tiles immediately before writing them to the working copy -
        # unlike ODB objects that are already fetched.
        fetch_lfs_blobs_for_pointer_files(
            self.repo, pointer_files_to_fetch, quiet=quiet
        )

        # Second pass - actually update the working copy:

        with self.workdir_index_session() as workdir_index:
            if ds_deletes:
                self.delete_datasets_from_workdir(
                    [base_datasets[d] for d in ds_deletes], workdir_index
                )
            if ds_inserts:
                self.write_full_datasets_to_workdir(
                    [target_datasets[d] for d in ds_inserts], workdir_index
                )
            # Update the working copy with files that have changed:
            kart_attachments = os.environ.get("X_KART_ATTACHMENTS")
            if kart_attachments:
                if base_tree and target_tree:
                    self.update_file_diffs(base_tree, target_tree, workdir_index)

            for ds_path in ds_updates:
                self._update_dataset_in_workdir(
                    target_datasets[ds_path],
                    update_diffs[ds_path],
                    workdir_index,
                    ds_filter=repo_key_filter[ds_path],
                    track_changes_as_dirty=track_changes_as_dirty,
                )

        with self.state_session() as sess:
            if not track_changes_as_dirty:
                self._update_state_table_tree(sess, target_tree_id)
            self._update_state_table_spatial_filter_hash(
                sess, self.repo.spatial_filter.hexhash
            )

    def update_file_diffs(self, base_tree, target_tree, workdir_index):
        """Get the deltas for attachment files and write them to the working copy."""
        repo = self.repo
        base_rs = RepoStructure(repo, base_tree)
        target_rs = RepoStructure(repo, target_tree)
        attachment_deltas = get_file_diff(base_rs, target_rs)

        for filename, file_delta in attachment_deltas.items():
            new_path = self.path / filename

            # Delete the old file
            if file_delta.old and new_path.is_file():
                new_path.unlink()

            # Create a new file
            if file_delta.new:
                blob_data = repo[file_delta.new.value].data
                new_path.write_bytes(blob_data)

            # TODO: update workdir_index.

    def _diff_to_reset(
        self, ds_path, base_datasets, target_datasets, workdir_diff_cache, ds_filter
    ):
        """
        Get the diff-to-apply needed to reset a particular dataset - currently based on base_datasets[ds_path] -
        to the target state at target_datasets[ds_path]."""
        ds_diff = ~base_datasets[ds_path].diff_to_working_copy(
            workdir_diff_cache, ds_filter=ds_filter, extract_metadata=False
        )
        if base_datasets != target_datasets:
            ds_diff = DatasetDiff.concatenated(
                ds_diff,
                diff_util.get_dataset_diff(
                    ds_path,
                    base_datasets,
                    target_datasets,
                    ds_filter=ds_filter,
                ),
                overwrite_original=True,
            )

        tile_diff = ds_diff.get("tile")
        # Remove new values that don't match the spatial filter - we don't want them in the working copy.
        if tile_diff and not self.repo.spatial_filter.match_all:
            spatial_filter = self.repo.spatial_filter.transform_for_dataset(
                target_datasets[ds_path]
            )
            tiles_to_remove = set()
            for tilename, delta in tile_diff.items():
                if delta.new_value and not spatial_filter.matches(delta.new_value):
                    tiles_to_remove.add(tilename)
            for tilename in tiles_to_remove:
                delta = tile_diff[tilename]
                if delta.old is not None:
                    tile_diff[tilename] = Delta.delete(delta.old)
                else:
                    del tile_diff[tilename]
        return ds_diff

    def _list_new_pointer_blobs_for_diff(self, dataset_diff, tile_dataset):
        inner_tree = tile_dataset.inner_tree
        if not inner_tree:
            return

        tile_diff = dataset_diff.get("tile")
        if not tile_diff:
            return
        for tilename in tile_diff.keys():
            path = tile_dataset.tilename_to_blob_path(tilename, relative=True)
            pointer_blob = tile_dataset.get_blob_at(path, missing_ok=True)
            if pointer_blob:
                yield pointer_blob
            pam_path = path + PAM_SUFFIX
            pam_pointer_blob = tile_dataset.get_blob_at(pam_path, missing_ok=True)
            if pam_pointer_blob:
                yield pam_pointer_blob

    def write_full_datasets_to_workdir(
        self, datasets, workdir_index, track_changes_as_dirty=False
    ):
        write_to_index = not track_changes_as_dirty

        dataset_count = len(datasets)
        for i, dataset in enumerate(datasets):
            assert isinstance(dataset, TileDataset)

            click.echo(
                f"Writing tiles for dataset {i+1} of {dataset_count}: {dataset.path}",
                err=True,
            )

            if write_to_index:
                workdir_index.remove_all([f"{dataset.path}/**"])

            wc_tiles_dir = self.path / dataset.path
            (wc_tiles_dir).mkdir(parents=True, exist_ok=True)

            for pointer_blob, pointer_dict in dataset.tile_pointer_blobs_and_dicts(
                self.repo.spatial_filter,
                show_progress=True,
            ):
                pointer_dict["name"] = dataset.set_tile_extension(
                    pointer_blob.name, tile_format=pointer_dict.get("format")
                )
                self._write_tile_or_pam_file_to_workdir(
                    dataset,
                    pointer_dict,
                    workdir_index,
                    write_to_index=write_to_index,
                )

        self.write_mosaic_for_dataset(dataset)

    def delete_datasets_from_workdir(
        self, datasets, workdir_index, track_changes_as_dirty=False
    ):
        write_to_index = not track_changes_as_dirty
        for dataset in datasets:
            assert isinstance(dataset, TileDataset)

            ds_tiles_dir = (self.path / dataset.path).resolve()
            # Sanity check to make sure we're not deleting something we shouldn't.
            assert self.path in ds_tiles_dir.parents
            assert self.repo.workdir_path in ds_tiles_dir.parents
            if ds_tiles_dir.is_dir():
                shutil.rmtree(ds_tiles_dir)
            if write_to_index:
                workdir_index.remove_all([f"{dataset.path}/**"])

    def delete_tiles(
        self,
        repo_key_filter,
        datasets,
        *,
        track_changes_as_dirty=True,
        including_conflict_versions=False,
    ):
        """
        Delete the tiles that match the repo_key_filter.
        If including_conflict_versions is True, then variants of the tile name that include conflict version infixes
        - .ancestor. or .ours. or .theirs. - will also be deleted.
        """
        if not repo_key_filter:
            return

        if repo_key_filter.match_all:
            raise NotImplementedError(
                "delete_tiles currently only supports deleting specific tiles, not match_all"
            )

        with self.workdir_index_session() as workdir_index:
            for ds_path, ds_filter in repo_key_filter.items():
                dataset = datasets[ds_path]
                self.delete_tiles_for_dataset(
                    dataset,
                    ds_filter,
                    workdir_index,
                    track_changes_as_dirty=track_changes_as_dirty,
                    including_conflict_versions=including_conflict_versions,
                )

    def delete_tiles_for_dataset(
        self,
        dataset,
        ds_filter,
        *,
        track_changes_as_dirty=True,
        including_conflict_versions=False,
    ):
        tile_filter = ds_filter.get("tile")
        if not tile_filter:
            return
        ds_tiles_dir = (self.path / dataset.path).resolve()
        if not ds_tiles_dir.is_dir():
            return

        # Sanity check to make sure we're not messing with files we shouldn't:
        assert self.path in ds_tiles_dir.parents
        assert self.repo.workdir_path in ds_tiles_dir.parents
        if tile_filter.match_all:
            raise NotImplementedError(
                "delete_tiles currently only supports deleting specific tiles, not match_all"
            )

        write_to_index = not track_changes_as_dirty
        with self.workdir_index_session() as workdir_index:
            for tilename in tile_filter:
                name_pattern = dataset.get_tile_path_pattern(
                    tilename, include_conflict_versions=including_conflict_versions
                )
                for child in ds_tiles_dir.glob(tilename + ".*"):
                    if name_pattern.fullmatch(child.name) and child.is_file():
                        child.unlink()
                    if write_to_index:
                        workdir_index.remove_all([f"{dataset.path}/{child.name}"])

    def _update_dataset_in_workdir(
        self,
        dataset,
        diff_to_apply,
        workdir_index,
        *,
        ds_filter,
        track_changes_as_dirty,
    ):
        ds_path = dataset.path
        ds_tiles_dir = (self.path / ds_path).resolve()
        # Sanity check to make sure we're not messing with files we shouldn't:
        assert self.path in ds_tiles_dir.parents
        assert self.repo.workdir_path in ds_tiles_dir.parents
        assert ds_tiles_dir.is_dir()

        tile_diff = diff_to_apply.get("tile")
        if not tile_diff:
            return

        write_to_index = not track_changes_as_dirty

        def _all_names_in_tile_delta(tile_delta):
            for tile_summary in tile_delta.old_value, tile_delta.new_value:
                if tile_summary is None:
                    continue
                for key in ("name", "sourceName", "pamName", "sourcePamName"):
                    if key in tile_summary:
                        yield tile_summary[key]

        for tile_delta in tile_diff.values():
            for tile_name in set(_all_names_in_tile_delta(tile_delta)):
                tile_path = ds_tiles_dir / tile_name
                if tile_path.is_file():
                    tile_path.unlink()
                if write_to_index:
                    workdir_index.remove_all([f"{ds_path}/{tile_name}"])

            if tile_delta.type in ("update", "insert"):
                new_val = tile_delta.new_value
                self._write_tile_or_pam_file_to_workdir(
                    dataset,
                    new_val,
                    workdir_index,
                    write_to_index=write_to_index,
                )

                if new_val.get("pamOid"):
                    self._write_tile_or_pam_file_to_workdir(
                        dataset,
                        new_val,
                        workdir_index,
                        use_pam_prefix=True,
                        write_to_index=write_to_index,
                    )

        self.write_mosaic_for_dataset(dataset)

    def _write_tile_or_pam_file_to_workdir(
        self,
        dataset,
        tile_summary,
        workdir_index,
        *,
        use_pam_prefix=False,
        write_to_index=True,
        skip_write_tile=False,
    ):
        """
        Given a tile summary eg {"name": "foo.laz", "oid": "sha256:...", "size": 1234},
        copies the tile from the LFS cache to the workdir, using reflink where possible.

        dataset - the dataset the tile belongs to, controls the path where the tile is written.
        tile_summary - the dict to read the tile's name, OID, and size from.
        workdir_index - index file for the workdir.
        use_pam_prefix - if True, "pamName", "pamOid" and "pamSize" will be read instead.
        write_to_index - if True, the workdir_index will be updated to record that this file is in the workdir.
        skip_write_tile - if True, don't actually write the tile - only update the index.
            This is useful if we know the file is already there (eg, the user put it there themselves).
        """

        tilename = tile_summary["pamName" if use_pam_prefix else "name"]
        oid = tile_summary["pamOid" if use_pam_prefix else "oid"]
        size = tile_summary["pamSize" if use_pam_prefix else "size"]
        lfs_path = get_local_path_from_lfs_hash(self.repo, oid)
        if not lfs_path.is_file():
            click.echo(f"Couldn't find {tilename} locally - skipping...", err=True)
            return

        workdir_path = (self.path / dataset.path / tilename).resolve()
        # Sanity check to make sure we're not messing with files we shouldn't:
        assert self.path in workdir_path.parents
        assert self.repo.workdir_path in workdir_path.parents
        assert workdir_path.parents[0].is_dir()

        if not skip_write_tile:
            try_reflink(lfs_path, workdir_path)

        if write_to_index:
            # In general, after writing a dataset, we ask Git to build an index of the workdir,
            # which we can then use (via some Git commands) to detect changes to the workdir.
            # Git builds an index by getting the hash and the stat info for each file it finds.
            # However, the LFS tiles are potentially large and numerous, costly to hash - and
            # we already know their hashes. So, in this step, we pre-emptively update the index
            # just for the LFS tiles. When Git builds the index, it will find the entries for
            # those tiles already written, and detect they are unchanged by checking the stat info,
            # so it won't need to update those entries.

            # Git would do something similarly efficient if we used Git to do the checkout
            # operation in the first place. However, Git would need some changes to be
            # able to do this well - understanding Kart datasets, rewriting paths, and
            # using reflink where available.

            rel_path = f"{dataset.path}/{tilename}"
            pointer_file = dict_to_pointer_file_bytes({"oid": oid, "size": size})
            pointer_file_oid = self.repo.write(pygit2.GIT_OBJ_BLOB, pointer_file)
            workdir_index.add_entry_with_custom_stat(
                pygit2.IndexEntry(rel_path, pointer_file_oid, pygit2.GIT_FILEMODE_BLOB),
                workdir_path,
            )

    def soft_reset_after_commit(
        self,
        commit_or_tree,
        *,
        mark_as_clean=None,
        now_outside_spatial_filter=None,
        committed_diff=None,
    ):
        datasets = self.repo.datasets(
            commit_or_tree,
            repo_key_filter=mark_as_clean,
            filter_dataset_type=ALL_TILE_DATASET_TYPES,
        )

        # Handle tiles that were, eg, converted to COPC during the commit - the non-COPC
        # tiles in the workdir now need to be replaced with the COPC ones:
        with self.workdir_index_session() as workdir_index:
            self._hard_reset_after_commit_for_converted_and_renamed_tiles(
                datasets, committed_diff, workdir_index
            )
            self._reset_workdir_index_after_commit(
                datasets, committed_diff, workdir_index
            )
            self.delete_tiles(
                now_outside_spatial_filter,
                datasets,
                track_changes_as_dirty=False,
            )

        self.update_state_table_tree(commit_or_tree.peel(pygit2.Tree))

    def _hard_reset_after_commit_for_converted_and_renamed_tiles(
        self, datasets, committed_diff, workdir_index
    ):
        """
        Look for tiles that were automatically modified as part of the commit operation
        - these will have extra properties like a "sourceName" that differs from "name"
        or a "sourceFormat" that differs from "format". The source one is what the user
        supplied, and the other is what was actually committed.
        These need to be updated in the workdir so that the workdir reflects what was committed.
        """
        for dataset in datasets:
            tile_diff = committed_diff.recursive_get([dataset.path, "tile"])
            if not tile_diff:
                continue
            for tile_delta in tile_diff.values():
                new_value = tile_delta.new_value
                if new_value is None:
                    continue
                if "sourceName" in new_value or "sourceFormat" in new_value:
                    self._hard_reset_converted_tile(dataset, tile_delta, workdir_index)
                if "pamSourceName" in new_value:
                    self._hard_reset_renamed_pam_file(
                        dataset, tile_delta, workdir_index
                    )

            self.write_mosaic_for_dataset(dataset)

    def _hard_reset_converted_tile(self, dataset, tile_delta, workdir_index):
        """
        Update an individual tile in the workdir so that it reflects what was actually committed.
        """
        tilename = dataset.remove_tile_extension(tile_delta.new_value["name"])

        ds_tiles_dir = (self.path / dataset.path).resolve()
        # Sanity check to make sure we're not messing with files we shouldn't:
        assert self.path in ds_tiles_dir.parents
        assert self.repo.workdir_path in ds_tiles_dir.parents
        assert ds_tiles_dir.is_dir()

        name_pattern = dataset.get_tile_path_pattern(tilename)
        for child in ds_tiles_dir.glob(tilename + ".*"):
            if name_pattern.fullmatch(child.name) and child.is_file():
                child.unlink()

        self._write_tile_or_pam_file_to_workdir(
            dataset, tile_delta.new_value, workdir_index, write_to_index=True
        )

    def _hard_reset_renamed_pam_file(self, dataset, tile_delta, workdir_index):
        """
        Update an individual PAM file in the workdir so that it reflects what was actually committed.
        """
        tilename = dataset.remove_tile_extension(tile_delta.new_value["name"])

        ds_tiles_dir = (self.path / dataset.path).resolve()
        # Sanity check to make sure we're not messing with files we shouldn't:
        assert self.path in ds_tiles_dir.parents
        assert self.repo.workdir_path in ds_tiles_dir.parents
        assert ds_tiles_dir.is_dir()

        pam_name_pattern = dataset.get_tile_path_pattern(
            tilename, is_pam=True, ignore_tile_case=True
        )

        for child in ds_tiles_dir.glob(case_insensitive(tilename) + ".*"):
            if pam_name_pattern.fullmatch(child.name) and child.is_file():
                child.unlink()

        self._write_tile_or_pam_file_to_workdir(
            dataset,
            tile_delta.new_value,
            workdir_index,
            use_pam_prefix=True,
            write_to_index=True,
        )

    def _reset_workdir_index_after_commit(
        self, datasets, committed_diff, workdir_index
    ):
        for dataset in datasets:
            tile_diff = committed_diff.recursive_get([dataset.path, "tile"])
            if not tile_diff:
                continue
            for tile_delta in tile_diff.values():
                if tile_delta.type in ("update", "delete"):
                    old_val = tile_delta.old_value
                    for key in ("name", "sourceName", "pamName", "sourcePamName"):
                        if key in old_val:
                            workdir_index.remove_all([f"{dataset.path}/{old_val[key]}"])

                if tile_delta.type in ("update", "insert"):
                    new_val = tile_delta.new_value
                    self._write_tile_or_pam_file_to_workdir(
                        dataset,
                        new_val,
                        workdir_index,
                        skip_write_tile=True,
                        write_to_index=True,
                    )

                    if new_val.get("pamOid"):
                        self._write_tile_or_pam_file_to_workdir(
                            dataset,
                            new_val,
                            workdir_index,
                            use_pam_prefix=True,
                            skip_write_tile=True,
                            write_to_index=True,
                        )

    def write_mosaic_for_dataset(self, dataset):
        assert isinstance(dataset, TileDataset)
        dataset.write_mosaic_for_directory((self.path / dataset.path).resolve())

    def dirty_paths(self):
        env_overrides = {"GIT_INDEX_FILE": str(self.index_path)}

        try:
            # This finds all files in the index that have been modified - and updates any mtimes in the index
            # if the mtimes are stale but the files are actually unchanged (as in GIT_DIFF_UPDATE_INDEX).
            cmd = ["git", "diff", "--name-only"]
            output_lines = (
                subprocess.check_output(
                    cmd, env_overrides=env_overrides, encoding="utf-8", cwd=self.path
                )
                .strip()
                .splitlines()
            )
            # This finds all untracked files that are not in the index.
            cmd = ["git", "ls-files", "--others", "--exclude-standard"]
            output_lines += (
                subprocess.check_output(
                    cmd, env_overrides=env_overrides, encoding="utf-8", cwd=self.path
                )
                .strip()
                .splitlines()
            )
        except subprocess.CalledProcessError as e:
            sys.exit(translate_subprocess_exit_code(e.returncode))

        return [p.replace("\\", "/") for p in output_lines]

    def dirty_paths_by_dataset_path(self, dirty_paths=None):
        """Returns all the deltas from self.raw_diff_from_index() but grouped by dataset path."""
        if dirty_paths is None:
            dirty_paths = self.dirty_paths()

        all_ds_paths = list(self.repo.datasets(self.get_tree_id()).paths())

        def find_ds_path(file_path):
            for ds_path in all_ds_paths:
                if (
                    len(file_path) > len(ds_path)
                    and file_path.startswith(ds_path)
                    and file_path[len(ds_path)] == "/"
                ):
                    return ds_path
            return None

        dirty_paths_by_dataset_path = {}
        for p in dirty_paths:
            ds_path = find_ds_path(p)
            dirty_paths_by_dataset_path.setdefault(ds_path, []).append(p)

        return dirty_paths_by_dataset_path

    def workdir_diff_cache(self):
        """
        Returns a WorkdirDiffCache that acts as a caching layer for this working copy -
        the results of certain operations such as raw_diff_from_index can be cached for the
        duration of a diff.
        """
        return WorkdirDiffCache(self)


class WorkdirDiffCache:
    """
    When we do use the index to diff the workdir, we get a diff for the entire workdir.
    The diffing code otherwise performs diffs per-dataset, so we use this class to cache
    the result of that diff so we can reuse it for the next dataset diff.

    - We don't want to run it up front, in case there are no datasets that need this info
    - We want to run it as soon a the first dataset needs this info, then cache the result
    - We want the result to stay cached for the duration of the diff operation, but no longer
      (in eg a long-running test, there might be several diffs run and the workdir might change)
    """

    def __init__(self, delegate):
        self.delegate = delegate

    @functools.lru_cache(maxsize=1)
    def dirty_paths(self):
        return self.delegate.dirty_paths()

    @functools.lru_cache(maxsize=1)
    def dirty_paths_by_dataset_path(self):
        # Make sure self.dirty_paths gets cached too:
        dirty_paths = self.dirty_paths()
        return self.delegate.dirty_paths_by_dataset_path(dirty_paths)

    def dirty_paths_for_dataset(self, dataset):
        if isinstance(dataset, str):
            path = dataset
        else:
            path = dataset.path
        return self.dirty_paths_by_dataset_path().get(path, ())
