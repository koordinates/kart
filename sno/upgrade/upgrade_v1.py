from sno.geometry import normalise_gpkg_geom
from sno.dataset1 import Dataset1


class UpgradeDataset1(Dataset1):
    """Variation on Dataset1 specifically for upgrading to V2 and beyond."""

    def features(self):
        # Geometries weren't normalised in V1, but they are in V2.
        # Normalise them here.
        geom_column = self.geom_column_name
        for feature in super().features():
            if geom_column:
                # add bboxes to geometries.
                feature[geom_column] = normalise_gpkg_geom(feature[geom_column])
            yield feature
