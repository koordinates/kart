from .dataset3 import Dataset3
from .dataset3_paths import PathEncoder


class Dataset2(Dataset3):
    VERSION = 2

    DATASET_DIRNAME = ".sno-dataset"  # Default for V2 datasets.

    def feature_path_encoder(self, schema=None):
        return PathEncoder.LEGACY_ENCODER
