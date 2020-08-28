from sno import gpkg_adapter
from sno.geometry import normalise_gpkg_geom
from sno.import_source import ImportSource
from sno.structure import RepositoryStructure


def get_upgrade_sources(source_repo, source_commit):
    """Return upgrade sources for all V1 datasets at the given commit."""
    source_repo_structure = RepositoryStructure(source_repo, commit=source_commit)
    return [ImportV1Dataset(dataset) for dataset in source_repo_structure]


class ImportV1Dataset(ImportSource):
    # TODO: make ImportV1Dataset the same class as Dataset1 - they are almost the same already.

    def __init__(self, dataset):
        assert dataset.version == 1
        self.dataset = dataset

    def default_dest_path(self):
        return self.dataset.path

    def get_meta_item(self, key):
        return self.dataset.get_meta_item(key)

    def get_gpkg_meta_item(self, key):
        return self.dataset.get_gpkg_meta_item(key)

    def crs_definitions(self):
        return gpkg_adapter.all_v2_crs_definitions(self)

    def features(self):
        geom_column = self.dataset.geom_column_name
        for _, feature in self.dataset.features():
            if geom_column:
                # add bboxes to geometries.
                feature[geom_column] = normalise_gpkg_geom(feature[geom_column])
            yield feature

    @property
    def feature_count(self):
        return self.dataset.feature_count()
