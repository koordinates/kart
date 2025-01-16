from kart.sqlalchemy import TableSet
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
from sqlalchemy.types import NVARCHAR, VARCHAR


class TinyInt(Integer):
    def visit(self):
        return "TINYINT"


class Double(Float):
    def visit(self):
        return "DOUBLE"


class DateTime(Text):
    def visit(self):
        return "DATETIME"


STATE = "state"
TRACK = "track"


class AbstractKartTables(TableSet):
    """
    Tables for Kart-specific metadata.
    This is an abstract definition of the tables we require, not designed to work with any database in particular.
    There are concrete implementations for all working copy types that we support.
    """

    def __init__(self, db_schema=None, is_kart_branding=False):
        super().__init__()

        self.db_schema = db_schema
        self.is_kart_branding = is_kart_branding

        self.kart_state = Table(
            self.kart_table_name(STATE),
            self.sqlalchemy_metadata,
            Column("table_name", Text, nullable=False, primary_key=True),
            Column("key", Text, nullable=False, primary_key=True),
            Column("value", Text, nullable=False),
            schema=self.db_schema,
        )

        self.kart_track = Table(
            self.kart_table_name(TRACK),
            self.sqlalchemy_metadata,
            Column("table_name", Text, nullable=False, primary_key=True),
            Column("pk", Text, nullable=True, primary_key=True),
            schema=self.db_schema,
        )

    def kart_table_name(self, short_name):
        return f"_kart_{short_name}" if self.is_kart_branding else f"_sno_{short_name}"


class GpkgKartTables(AbstractKartTables):
    """
    Tables for Kart-specific metadata - GPKG variant.
    No schema is needed - the GPKG file already provides the namespace that we need.
    Prefixing the table names with "gpkg" means they are hidden.
    """

    def __init__(self, is_kart_branding=False):
        super().__init__(is_kart_branding=is_kart_branding)

    def kart_table_name(self, short_name):
        return (
            f"gpkg_kart_{short_name}"
            if self.is_kart_branding
            else f"gpkg_sno_{short_name}"
        )


class PostgisKartTables(AbstractKartTables):
    """Tables for Kart-specific metadata - PostGIS variant. Nothing special required."""


class MySqlKartTables(AbstractKartTables):
    """
    Tables for Kart-specific metadata - MySQL variant.
    Primary keys have to be VARCHAR of a fixed maximum length -
    if the total maximum length is too long, MySQL cannot generate an index.
    """

    def __init__(self, db_schema=None, is_kart_branding=False):
        # Don't call super since we are redefining self.kart_state and self.kart_track.
        self.sqlalchemy_metadata = MetaData()
        self.db_schema = db_schema
        self.is_kart_branding = is_kart_branding

        self.kart_state = Table(
            self.kart_table_name(STATE),
            self.sqlalchemy_metadata,
            Column("table_name", VARCHAR(256), nullable=False, primary_key=True),
            Column("key", VARCHAR(256), nullable=False, primary_key=True),
            Column("value", Text, nullable=False),
            schema=self.db_schema,
        )

        self.kart_track = Table(
            self.kart_table_name(TRACK),
            self.sqlalchemy_metadata,
            Column("table_name", VARCHAR(256), nullable=False, primary_key=True),
            Column("pk", VARCHAR(256), nullable=True, primary_key=True),
            schema=self.db_schema,
        )


class SqlServerKartTables(AbstractKartTables):
    """
    Tables for kart-specific metadata - SQL Server variant.
    Primary keys have to be NVARCHAR of a fixed maximum length -
    if the total maximum length is too long, SQL Server cannot generate an index.
    """

    def __init__(self, db_schema=None, is_kart_branding=False):
        # Don't call super since we are redefining self.kart_state and self.kart_track.
        self.sqlalchemy_metadata = MetaData()
        self.db_schema = db_schema
        self.is_kart_branding = is_kart_branding

        self.kart_state = Table(
            self.kart_table_name(STATE),
            self.sqlalchemy_metadata,
            Column("table_name", NVARCHAR(400), nullable=False, primary_key=True),
            Column("key", NVARCHAR(400), nullable=False, primary_key=True),
            Column("value", Text, nullable=False),
            schema=self.db_schema,
        )

        self.kart_track = Table(
            self.kart_table_name(TRACK),
            self.sqlalchemy_metadata,
            Column("table_name", NVARCHAR(400), nullable=False, primary_key=True),
            Column("pk", NVARCHAR(400), nullable=True, primary_key=True),
            schema=self.db_schema,
        )


class GpkgTables(TableSet):
    """GPKG spec tables - see http://www.geopackage.org/spec/#table_definition_sql"""

    def __init__(self):
        super().__init__()

        self.gpkg_spatial_ref_sys = Table(
            "gpkg_spatial_ref_sys",
            self.sqlalchemy_metadata,
            Column("srs_name", Text, nullable=False),
            Column("srs_id", Integer, primary_key=True),
            Column("organization", Text, nullable=False),
            Column("organization_coordsys_id", Integer, nullable=False),
            Column("definition", Text, nullable=False),
            Column("description", Text),
        )

        self.gpkg_contents = Table(
            "gpkg_contents",
            self.sqlalchemy_metadata,
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
                ForeignKey("gpkg_spatial_ref_sys.srs_id", name="fk_gc_r_srs_id"),
            ),
        )

        self.gpkg_geometry_columns = Table(
            "gpkg_geometry_columns",
            self.sqlalchemy_metadata,
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

        self.gpkg_metadata = Table(
            "gpkg_metadata",
            self.sqlalchemy_metadata,
            Column("id", Integer, primary_key=True, nullable=False, autoincrement=True),
            Column("md_scope", Text, nullable=False, default="dataset"),
            Column("md_standard_uri", Text, nullable=False),
            Column("mime_type", Text, nullable=False, default="text/xml"),
            Column("metadata", Text, nullable=False, default=""),
        )

        self.gpkg_metadata_reference = Table(
            "gpkg_metadata_reference",
            self.sqlalchemy_metadata,
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
                "md_parent_id",
                Integer,
                ForeignKey("gpkg_metadata.id", name="crmr_mpi_fk"),
            ),
        )

        self.gpkg_extensions = Table(
            "gpkg_extensions",
            self.sqlalchemy_metadata,
            Column("table_name", Text),
            Column("column_name", Text),
            Column("extension_name", Text, nullable=False),
            Column("definition", Text, nullable=False),
            Column("scope", Text, nullable=False),
            UniqueConstraint(
                "table_name", "column_name", "extension_name", name="ge_tce"
            ),
        )

    EPSG_4326_WKT = (
        'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],'
        'PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
    )

    GPKG_SPATIAL_REF_SYS_INITIAL_CONTENTS = [
        {
            "srs_name": "Undefined cartesian SRS",
            "srs_id": -1,
            "organization": "NONE",
            "organization_coordsys_id": -1,
            "definition": "undefined",
            "description": "undefined cartesian coordinate reference system",
        },
        {
            "srs_name": "Undefined geographic SRS",
            "srs_id": 0,
            "organization": "NONE",
            "organization_coordsys_id": 0,
            "definition": "undefined",
            "description": "undefined geographic coordinate reference system",
        },
        {
            "srs_name": "WGS 84 geodetic",
            "srs_id": 4326,
            "organization": "EPSG",
            "organization_coordsys_id": 4326,
            "definition": EPSG_4326_WKT,
            "description": "longitude/latitude coordinates in decimal degrees on the WGS 84 spheroid",
        },
    ]

    @classmethod
    def init_table_contents(cls, sess):
        from kart.sqlalchemy.upsert import Upsert as upsert

        sess.execute(
            upsert(cls.gpkg_spatial_ref_sys),
            cls.GPKG_SPATIAL_REF_SYS_INITIAL_CONTENTS,
        )


# Makes it so GPKG table definitions are also accessible at the GpkgTables class itself:
GpkgTables.copy_tables_to_class()


class SqlServerOgrTables(TableSet):
    """
    OGR tables for SQL Server - we follow the OGR convention when looking for custom CRSs,
    and when writing custom CRSs (meaning, CRSs not found in sys.spatial_reference_systems).
    See https://github.com/OSGeo/gdal/blob/master/ogr/ogrsf_frmts/mssqlspatial/ogrmssqlspatialdatasource.cpp#L1349-L1352
    """

    def __init__(self, db_schema=None):
        self.sqlalchemy_metadata = MetaData()
        self.db_schema = db_schema

        self.geometry_columns = Table(
            "geometry_columns",
            self.sqlalchemy_metadata,
            Column("f_table_catalog", VARCHAR(128), nullable=False, primary_key=True),
            Column("f_table_schema", VARCHAR(128), nullable=False, primary_key=True),
            Column("f_table_name", VARCHAR(256), nullable=False, primary_key=True),
            Column("f_geometry_column", VARCHAR(256), nullable=False, primary_key=True),
            Column("coord_dimension", Integer, nullable=False),
            Column("srid", Integer, nullable=False),
            Column("geometry_type", VARCHAR(30), nullable=False),
            schema=self.db_schema,
        )

        self.spatial_ref_sys = Table(
            "spatial_ref_sys",
            self.sqlalchemy_metadata,
            Column(
                "srid", Integer, nullable=False, primary_key=True, autoincrement=False
            ),
            Column("auth_name", VARCHAR(256)),
            Column("auth_srid", Integer),
            Column("srtext", VARCHAR(2048)),
            Column("proj4text", VARCHAR(2048)),
            schema=self.db_schema,
        )
