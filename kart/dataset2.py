from .dataset3 import Dataset3
from .dataset3_paths import PathEncoder


class Dataset2(Dataset3):
    VERSION = 2

    DATASET_DIRNAME = ".sno-dataset"  # Old name for V2 datasets.

    META_ITEM_NAMES = Dataset3.META_ITEM_NAMES + ("metadata/dataset.json",)

    @property
    def feature_path_encoder(self):
        return PathEncoder.LEGACY_ENCODER
