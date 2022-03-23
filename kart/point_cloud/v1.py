import functools

from kart.core import find_blobs_in_tree
from kart.base_dataset import BaseDataset
from kart.lfs_util import get_hash_from_pointer_file, get_local_path_from_lfs_hash


class PointCloudV1(BaseDataset):
    """A V1 point-cloud (LIDAR) dataset."""

    VERSION = 1
    DATASET_TYPE = "point-cloud"
    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    # All relative paths should be relative to self.inner_tree - that is, to the tree named DATASET_DIRNAME.
    TILES_PATH = "tiles/"

    def get_meta_item(self, name, missing_ok=True):
        # TODO - move a common implementation of get_meta_item into BaseDataset
        return None

    @property
    @functools.lru_cache(maxsize=1)
    def tiles_tree(self):
        """Returns the root of the tiles tree, or the empty tree if no tiles tree exists."""
        if self.inner_tree:
            try:
                return self.inner_tree / self.TILES_PATH
            except KeyError:
                pass
        return self.repo.empty_tree

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
