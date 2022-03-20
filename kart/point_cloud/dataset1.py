import logging


class PointCloudV1:
    """A V1 point-cloud (LIDAR) dataset."""

    DATASET_DIRNAME = ".point-cloud-dataset.v1"

    def __init__(self, tree, path, dirname=None, repo=None):
        # TODO - move functionality common to all datasets into a common base class.
        if dirname is None:
            dirname = self.DATASET_DIRNAME

        if tree is not None:
            self.tree = tree
            self.inner_tree = tree / dirname if dirname else self.tree
        else:
            self.inner_tree = self.tree = None

        self.path = path.strip("/")
        self.inner_path = f"{path}/{dirname}" if dirname else self.path

        self.repo = repo

        self.L = logging.getLogger(self.__class__.__qualname__)

    # TODO - lots more functionality.
