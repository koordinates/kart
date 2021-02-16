from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
)

from sqlalchemy.types import NVARCHAR


class TinyInt(Integer):
    __visit_name__ = "TINYINT"


class Double(Float):
    __visit_name__ = "DOUBLE"


class DateTime(Text):
    __visit_name__ = "DATETIME"


class TableSet:
    @classmethod
    def create_all(cls, session):
        return cls._SQLALCHEMY_METADATA.create_all(session.connection())


def _copy_columns(columns):
    return [c.copy() for c in columns]


class SnoTables(TableSet):
    """Tables for sno-specific metadata."""

    _SQLALCHEMY_METADATA = MetaData()

    sno_state = Table(
        "_sno_state",
        _SQLALCHEMY_METADATA,
        Column("table_name", Text, nullable=False, primary_key=True),
        Column("key", Text, nullable=False, primary_key=True),
        Column("value", Text, nullable=False),
    )

    sno_track = Table(
        "_sno_track",
        _SQLALCHEMY_METADATA,
        Column("table_name", Text, nullable=False, primary_key=True),
        Column("pk", Text, nullable=True, primary_key=True),
    )


class GpkgSnoTables(TableSet):
    """
    Tables for sno-specific metadata - GPKG variant.
    Prefixing the table names with "gpkg" means they are hidden.
    """

    _SQLALCHEMY_METADATA = MetaData()

    sno_state = Table(
        "gpkg_sno_state",
        _SQLALCHEMY_METADATA,
        *_copy_columns(SnoTables.sno_state.columns),
    )
    sno_track = Table(
        "gpkg_sno_track",
        _SQLALCHEMY_METADATA,
        *_copy_columns(SnoTables.sno_track.columns),
    )


class PostgisSnoTables(TableSet):
    """
    Tables for sno-specific metadata - PostGIS variant.
    Table names have a user-defined schema, and so unlike other table sets,
    we need to construct an instance with the appropriate schema.
    """

    def __init__(self, schema=None):
        self._SQLALCHEMY_METADATA = MetaData()

        self.sno_state = Table(
            "_sno_state",
            self._SQLALCHEMY_METADATA,
            *_copy_columns(SnoTables.sno_state.columns),
            schema=schema,
        )
        self.sno_track = Table(
            "_sno_track",
            self._SQLALCHEMY_METADATA,
            *_copy_columns(SnoTables.sno_track.columns),
            schema=schema,
        )

    def create_all(self, session):
        return self._SQLALCHEMY_METADATA.create_all(session.connection())


class SqlServerSnoTables(TableSet):
    """
    Tables for sno-specific metadata - PostGIS variant.
    Table names have a user-defined schema, and so unlike other table sets,
    we need to construct an instance with the appropriate schema.
    """

    def __init__(self, schema=None):
        self._SQLALCHEMY_METADATA = MetaData()

        self.sno_state = Table(
            "_sno_state",
            self._SQLALCHEMY_METADATA,
            Column("table_name", NVARCHAR(400), nullable=False, primary_key=True),
            Column("key", NVARCHAR(400), nullable=False, primary_key=True),
            Column("value", Text, nullable=False),
            schema=schema,
        )
        self.sno_track = Table(
            "_sno_track",
            self._SQLALCHEMY_METADATA,
            Column("table_name", NVARCHAR(400), nullable=False, primary_key=True),
            Column("pk", NVARCHAR(400), nullable=True, primary_key=True),
            schema=schema,
        )

    def create_all(self, session):
        return self._SQLALCHEMY_METADATA.create_all(session.connection())


class GpkgTables(TableSet):
    """GPKG spec tables - see http://www.geopackage.org/spec/#table_definition_sql"""

    _SQLALCHEMY_METADATA = MetaData()

    gpkg_spatial_ref_sys = Table(
        "gpkg_spatial_ref_sys",
        _SQLALCHEMY_METADATA,
        Column("srs_name", Text, nullable=False),
        Column("srs_id", Integer, primary_key=True),
        Column("organization", Text, nullable=False),
        Column("organization_coordsys_id", Integer, nullable=False),
        Column("definition", Text, nullable=False),
        Column("description", Text),
    )

    gpkg_contents = Table(
        "gpkg_contents",
        _SQLALCHEMY_METADATA,
        Column("table_name", Text, nullable=False, primary_key=True),
        Column("data_type", Text, nullable=False),
        Column("identifier", Text, unique=True),
        Column("description", Text, default=""),
        Column(
            "last_change",
            DateTime,
            nullable=False,
            server_default="strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        ),
        Column("min_x", Double),
        Column("min_y", Double),
        Column("max_x", Double),
        Column("max_y", Double),
        Column(
            "srs_id",
            Integer,
            ForeignKey("gpkg_spatial_ref_sys.srd_id", name="fk_gc_r_srs_id"),
        ),
    )

    gpkg_geometry_columns = Table(
        "gpkg_geometry_columns",
        _SQLALCHEMY_METADATA,
        Column(
            "table_name",
            Text,
            ForeignKey("gpkg_contents.table_name", name="fk_gc_tn"),
            nullable=False,
            primary_key=True,
            unique=True,
        ),
        Column("column_name", Text, nullable=False, primary_key=True),
        Column("geometry_type_name", Text, nullable=False),
        Column(
            "srs_id",
            Integer,
            ForeignKey("gpkg_spatial_ref_sys.srs_id", name="fk_gc_srs"),
            nullable=False,
        ),
        Column("z", TinyInt, nullable=False),
        Column("m", TinyInt, nullable=False),
    )

    gpkg_metadata = Table(
        "gpkg_metadata",
        _SQLALCHEMY_METADATA,
        Column("id", Integer, primary_key=True, nullable=False, autoincrement=True),
        Column("md_scope", Text, nullable=False, default="dataset"),
        Column("md_standard_uri", Text, nullable=False),
        Column("mime_type", Text, nullable=False, default="text/xml"),
        Column("metadata", Text, nullable=False, default=""),
    )

    gpkg_metadata_reference = Table(
        "gpkg_metadata_reference",
        _SQLALCHEMY_METADATA,
        Column("reference_scope", Text, nullable=False),
        Column("table_name", Text),
        Column("column_name", Text),
        Column("row_id_value", Integer),
        Column(
            "timestamp",
            DateTime,
            nullable=False,
            server_default="strftime('%Y-%m-%dT%H:%M:%fZ','now')",
        ),
        Column(
            "md_file_id",
            Integer,
            ForeignKey("gpkg_metadata.id", name="crmr_mfi_fk"),
            nullable=False,
        ),
        Column(
            "md_parent_id", Integer, ForeignKey("gpkg_metadata.id", name="crmr_mpi_fk")
        ),
    )

    gpkg_extensions = Table(
        "gpkg_extensions",
        _SQLALCHEMY_METADATA,
        Column("table_name", Text),
        Column("column_name", Text),
        Column("extension_name", Text, nullable=False),
        Column("definition", Text, nullable=False),
        Column("scope", Text, nullable=False),
        UniqueConstraint("table_name", "column_name", "extension_name", name="ge_tce"),
    )
