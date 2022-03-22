import functools
import logging

from kart.core import find_blobs_in_tree
from kart.lfs_util import get_hash_from_pointer_file, get_local_path_from_lfs_hash


class PointCloudV1:
    """A V1 point-cloud (LIDAR) dataset."""

    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    META_PATH = "meta"
    TILES_PATH = "tiles"

    def __init__(self, tree, path, dirname=None, repo=None):
        # TODO - move functionality common to all datasets into a common base class.
        if dirname is None:
            dirname = self.DATASET_DIRNAME

        assert path is not None
        assert dirname is not None
        assert repo is not None

        if tree is not None:
            self.tree = tree
            self.inner_tree = tree / dirname
        else:
            self.inner_tree = self.tree = None

        self.path = path.strip("/")
        self.inner_path = f"{path}/{dirname}"
        self.repo = repo

        self.L = logging.getLogger(self.__class__.__qualname__)

    @property
    @functools.lru_cache(maxsize=1)
    def tiles_tree(self):
        """Returns the root of the tiles tree, or the empty tree if no tiles tree exists."""
        if self.inner_tree:
            try:
                return self.inner_tree / self.TILES_PATH
            except KeyError:
                pass
        return self.repo.empty_tree if self.repo else None

    def tile_pointer_blobs(self):
        """Returns a generator that yields every tile pointer blob in turn."""
        tiles_tree = self.tiles_tree
        if tiles_tree:
            yield from find_blobs_in_tree(tiles_tree)

    def tilenames_with_lfs_hashes(self):
        """Returns a generator that yields every tilename along with its LFS hash."""
        for blob in self.tile_pointer_blobs():
            yield blob.name, get_hash_from_pointer_file(blob)

    def tilenames_with_lfs_paths(self):
        """Returns a generator that yields every tilename along with the path where the tile content is stored locally."""
        for blob_name, lfs_hash in self.tilenames_with_lfs_hashes():
            yield blob_name, get_local_path_from_lfs_hash(self.repo, lfs_hash)
