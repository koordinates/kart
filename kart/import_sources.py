from enum import Enum, auto
import os

import click

from kart.exceptions import NotYetImplemented


class ImportType(Enum):
    """
    Different types of dataset import currently supported by Kart.
    These are broad categories of import eg - Kart supports importing tables using sqlalchemy.
    More specific types are found below in ImportSourceType.
    """

    SQLALCHEMY_TABLE = auto()
    OGR_TABLE = auto()
    POINT_CLOUD = auto()
    RASTER = auto()

    @property
    def import_cmd(self):
        if self in (self.SQLALCHEMY_TABLE, self.OGR_TABLE):
            from kart.tabular.import_ import table_import

            return table_import
        elif self is self.POINT_CLOUD:
            from kart.point_cloud.import_ import point_cloud_import

            return point_cloud_import
        elif self is self.RASTER:
            from kart.raster.import_ import raster_import

            return raster_import

    @property
    def import_source_class(self):
        if self is self.SQLALCHEMY_TABLE:
            from kart.tabular import SqlAlchemyTableImportSource

            return SqlAlchemyTableImportSource
        elif self is self.OGR_TABLE:
            from kart.tabular import OgrTableImportSource

            return OgrTableImportSource


class ImportSourceType:
    """
    Different types of import source currently supported by Kart.
    These are more specific than the ImportType enum above - multiple ImportSourceTypes are of the same ImportType.
    """

    def __init__(
        self,
        name,
        spec,
        import_type,
        *,
        uri_scheme=None,
        file_ext=None,
        optional_prefix=None,
        hidden=False,
    ):
        self.name = name
        self.spec = spec
        self.import_type = import_type

        self.uri_scheme = uri_scheme
        if file_ext is None:
            self.file_ext = None
        elif isinstance(file_ext, str):
            self.file_ext = (file_ext,)
        elif isinstance(file_ext, tuple):
            self.file_ext = file_ext
        self.optional_prefix = optional_prefix
        self.hidden = hidden

    @property
    def import_cmd(self):
        return self.import_type.import_cmd

    @property
    def import_source_class(self):
        return self.import_type.import_source_class


ALL_IMPORT_SOURCE_TYPES = [
    # Sqlalchemy tabular imports
    ImportSourceType(
        "GeoPackage",
        "PATH.gpkg",
        ImportType.SQLALCHEMY_TABLE,
        file_ext=".gpkg",
        optional_prefix="GPKG:",
    ),
    ImportSourceType(
        "PostgreSQL",
        "postgresql://HOST/DBNAME[/DBSCHEMA[/TABLE]]",
        ImportType.SQLALCHEMY_TABLE,
        uri_scheme="postgresql",
    ),
    ImportSourceType(
        "Microsoft SQL Server",
        "mssql://HOST/DBNAME[/DBSCHEMA[/TABLE]]",
        ImportType.SQLALCHEMY_TABLE,
        uri_scheme="mssql",
    ),
    ImportSourceType(
        "MySQL",
        "mysql://HOST[/DBNAME[/TABLE]]",
        ImportType.SQLALCHEMY_TABLE,
        uri_scheme="mysql",
    ),
    # OGR tabular imports
    ImportSourceType(
        "ESRI Shapefile",
        "PATH.shp",
        ImportType.OGR_TABLE,
        file_ext=(".shp", ".shx", ".dbf"),
    ),
    ImportSourceType(
        "OGR", "OGR:...", ImportType.OGR_TABLE, uri_scheme="OGR", hidden=True
    ),
    # Point cloud imports:
    ImportSourceType(
        "LAS (LASer)",
        "PATH.las or PATH.laz",
        ImportType.POINT_CLOUD,
        file_ext=(".las", ".laz"),
    ),
    # Raster imports:
    ImportSourceType(
        "GeoTIFF",
        "PATH.tif or PATH.tiff",
        ImportType.RASTER,
        file_ext=(".tif", ".tiff"),
    ),
]

URI_SCHEME_TO_IMPORT_SOURCE_TYPE = {
    t.uri_scheme: t for t in ALL_IMPORT_SOURCE_TYPES if t.uri_scheme
}

FILE_EXT_TO_IMPORT_SOURCE_TYPE = {
    ext: t
    for t in ALL_IMPORT_SOURCE_TYPES
    if t.file_ext is not None
    for ext in t.file_ext
}


def from_spec(spec, allow_unrecognised=False):
    """
    Given a spec from the user that is supposed to define an import source,
    return the ImportSourceType it describes (or attempts to describe).
    """

    spec = str(spec)
    parts = spec.split(":", maxsplit=2)

    result = URI_SCHEME_TO_IMPORT_SOURCE_TYPE.get(parts[0])
    if result:
        return result

    ext = os.path.splitext(spec)[1]
    result = FILE_EXT_TO_IMPORT_SOURCE_TYPE.get(ext.lower())
    if result:
        return result

    if len(parts) == 2:
        ext = os.path.splitext(parts[0])[1]
        result = FILE_EXT_TO_IMPORT_SOURCE_TYPE.get(ext.lower())
        if result:
            raise NotYetImplemented(
                'Sorry, using the "SOURCE:AS_NAME" syntax to rename an import source is not yet supported.'
            )

    if allow_unrecognised:
        return None
    else:
        raise bad_spec_error(spec)


def suggest_specs(suggestions=None, import_types=None, indent="    "):
    """Returns a string for explaining to the user how to specify the various import sources."""
    if suggestions is None:
        suggestions = all_supported_import_source_types()

    if import_types is not None:
        suggestions = [s for s in suggestions if s.import_type in import_types]

    if len(suggestions) <= 1:
        return f"{indent}{suggestions[0].spec}"

    suggestion_strs = []
    for suggestion in suggestions:
        suggestion_strs.append(f"{indent}{suggestion.name}: {suggestion.spec}")
    return "\n".join(suggestion_strs)


def bad_spec_error(spec, suggestions=None):
    """If the user provided a bad format specification, returns a UsageError that tries to help them fix it."""
    if suggestions is None:
        suggestion = from_spec(spec, allow_unrecognised=True)
        suggestions = (
            [suggestion] if suggestion else all_supported_import_source_types()
        )

    try_the_following = (
        "Try one of the following" if len(suggestions) >= 2 else "Try the following"
    )
    return click.UsageError(
        f"Unrecognised import-source specification: {spec}\n"
        f"{try_the_following}:\n{suggest_specs(suggestions)}"
    )


def all_supported_import_source_types():
    result = ALL_IMPORT_SOURCE_TYPES
    result = [r for r in ALL_IMPORT_SOURCE_TYPES if not r.hidden]
    if not is_shp_supported():
        result = [r for r in result if "Shapefile" not in r.name]
    return result


def is_shp_supported():
    from osgeo import gdal

    d = gdal.GetDriverByName("ESRI Shapefile")
    if d:
        m = d.GetMetadata()
        if m.get("DCAP_VECTOR") == "YES" and m.get("DCAP_OPEN") == "YES":
            return True

    return False
