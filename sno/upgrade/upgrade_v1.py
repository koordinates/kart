import functools

from sno.geometry import normalise_gpkg_geom
from sno.structure import RepositoryStructure
from sno.gpkg_adapter import gpkg_to_v2_schema, wkt_to_crs_str


def get_upgrade_sources(source_repo, source_commit):
    """Return upgrade sources for all V1 datasets at the given commit."""
    source_repo_structure = RepositoryStructure(source_repo, commit=source_commit)
    return {dataset.path: ImportV1Dataset(dataset) for dataset in source_repo_structure}


class ImportV1Dataset:
    # TODO: make ImportV1Dataset the same class as Dataset1 - they are almost the same already.

    def __init__(self, dataset):
        assert dataset.version == 1
        self.dataset = dataset
        self.path = self.dataset.path
        self.table = self.path
        self.source = "v1-sno-repo"

    @property
    @functools.lru_cache(maxsize=1)
    def schema(self):
        sqlite_table_info = self.dataset.get_meta_item("sqlite_table_info")
        gpkg_geometry_columns = self.dataset.get_meta_item("gpkg_geometry_columns")
        gpkg_spatial_ref_sys = self.dataset.get_meta_item("gpkg_spatial_ref_sys")
        return gpkg_to_v2_schema(
            sqlite_table_info,
            gpkg_geometry_columns,
            gpkg_spatial_ref_sys,
            id_salt=self.path,
        )

    def get_meta_item(self, key):
        if key == "title":
            return self.dataset.get_meta_item("gpkg_contents")["identifier"]
        elif key == "description":
            return self.dataset.get_meta_item("gpkg_contents")["description"]
        else:
            return self.dataset.get_meta_item(key)

    def crs_definitions(self):
        gsrs = self.dataset.get_meta_item("gpkg_spatial_ref_sys")
        if gsrs and gsrs[0]["definition"]:
            definition = gsrs[0]["definition"]
            yield wkt_to_crs_str(definition), definition

    def iter_features(self):
        geom_column = self.dataset.geom_column_name
        for _, feature in self.dataset.features():
            if geom_column:
                # add bboxes to geometries.
                feature[geom_column] = normalise_gpkg_geom(feature[geom_column])
            yield feature

    @property
    def row_count(self):
        return self.dataset.feature_count()

    def __str__(self):
        return self.path

    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass
