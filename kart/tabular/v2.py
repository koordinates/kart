from kart.base_dataset import MetaItemDefinition, MetaItemFileType
from .v3 import TableV3
from .v3_paths import PathEncoder


class TableV2(TableV3):
    VERSION = 2

    DATASET_DIRNAME = ".sno-dataset"  # Old name for V2 datasets.

    META_ITEMS = TableV3.META_ITEMS + (
        MetaItemDefinition("metadata/dataset.json", MetaItemFileType.JSON),
    )

    @property
    def feature_path_encoder(self):
        return PathEncoder.LEGACY_ENCODER
