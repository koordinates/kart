import contextlib
import logging
import time

from sqlalchemy import literal_column
from sqlalchemy.dialects.mssql.base import MSIdentifierPreparer
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.functions import Function
from sqlalchemy.types import UserDefinedType

from . import sqlserver_adapter
from .db_server import DatabaseServer_WorkingCopy
from .table_defs import SqlServerKartTables
from sno import crs_util
from sno.geometry import Geometry
from sno.sqlalchemy import text_with_inlined_params
from sno.sqlalchemy.create_engine import sqlserver_engine


class WorkingCopy_SqlServer(DatabaseServer_WorkingCopy):
    """
    SQL Server working copy implementation.

    Requirements:
    1. A SQL server driver must be installed on your system.
       See https://docs.microsoft.com/sql/connect/odbc/microsoft-odbc-driver-for-sql-server
    2. The database needs to exist
    3. The database user needs to be able to:
        - Create the specified schema (unless it already exists).
        - Create, delete and alter tables and triggers in the specified schema.
    """

    WORKING_COPY_TYPE_NAME = "SQL Server"
    URI_SCHEME = "mssql"

    def __init__(self, repo, location):
        """
        uri: connection string of the form mssql://[user[:password]@][netloc][:port][/dbname/schema][?param1=value1&...]
        """
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo
        self.uri = self.location = location

        self.check_valid_db_uri(self.uri, repo)
        self.db_uri, self.db_schema = self._separate_db_schema(self.uri)

        self.engine = sqlserver_engine(self.db_uri)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.preparer = MSIdentifierPreparer(self.engine.dialect)

        self.kart_tables = SqlServerKartTables(self.db_schema, repo.is_kart_branded)

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = sqlserver_adapter.v2_schema_to_sqlserver_spec(
            dataset.schema, dataset
        )
        sess.execute(
            f"""CREATE TABLE {self.table_identifier(dataset)} ({table_spec});"""
        )

    def _type_def_for_column_schema(self, col, dataset):
        if col.data_type == "geometry":
            crs_name = col.extra_type_info.get("geometryCRS")
            crs_id = crs_util.get_identifier_int_from_dataset(dataset, crs_name) or 0
            # This user-defined GeometryType adapts Kart's GPKG geometry to SQL Server's native geometry type.
            return GeometryType(crs_id)
        elif col.data_type in ("date", "time", "timestamp"):
            return BaseDateOrTimeType
        else:
            # Don't need to specify type information for other columns at present, since we just pass through the values.
            return None

    def _write_meta(self, sess, dataset):
        """Write the title. Other metadata is not stored in a SQL Server WC."""
        self._write_meta_title(sess, dataset)

    def _write_meta_title(self, sess, dataset):
        """Write the dataset title as a comment on the table."""
        # TODO - dataset title is not stored anywhere in SQL server working copy right now.
        # We can probably store it using function sp_addextendedproperty to add property 'MS_Description'
        pass

    def delete_meta(self, dataset):
        """Delete any metadata that is only needed by this dataset."""
        # There is no metadata stored anywhere except the table itself.
        pass

    def _get_geom_extent(self, sess, dataset, default=None):
        """Returns the envelope around the entire dataset as (min_x, min_y, max_x, max_y)."""
        geom_col = dataset.geom_column_name
        r = sess.execute(
            f"""
            WITH _E AS (
                SELECT geometry::EnvelopeAggregate({self.quote(geom_col)}) AS envelope
                FROM {self.table_identifier(dataset)}
            )
            SELECT
                envelope.STPointN(1).STX AS min_x,
                envelope.STPointN(1).STY AS min_y,
                envelope.STPointN(3).STX AS max_x,
                envelope.STPointN(3).STY AS max_y
            FROM _E;
            """
        )
        result = r.fetchone()
        return default if result == (None, None, None, None) else result

    def _grow_rectangle(self, rectangle, scale_factor):
        # scale_factor = 1 -> no change, >1 -> grow, <1 -> shrink.
        min_x, min_y, max_x, max_y = rectangle
        centre_x, centre_y = (min_x + max_x) / 2, (min_y + max_y) / 2
        min_x = (min_x - centre_x) * scale_factor + centre_x
        min_y = (min_y - centre_y) * scale_factor + centre_y
        max_x = (max_x - centre_x) * scale_factor + centre_x
        max_y = (max_y - centre_y) * scale_factor + centre_y
        return min_x, min_y, max_x, max_y

    def _create_spatial_index_post(self, sess, dataset):
        # Only implementing _create_spatial_index_post:
        # We need to know the rough extent of the data to create an index in that area,
        # so we create the spatial index once the bulk of the features have been written.

        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")

        extent = self._get_geom_extent(sess, dataset)
        if not extent:
            # Can't create a spatial index if we don't know the rough bounding box we need to index.
            return

        # Add 20% room to grow.
        GROW_FACTOR = 1.2
        min_x, min_y, max_x, max_y = self._grow_rectangle(extent, GROW_FACTOR)

        geom_col = dataset.geom_column_name
        index_name = f"{dataset.table_name}_idx_{geom_col}"

        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)
        t0 = time.monotonic()

        # Placeholders not allowed in CREATE SPATIAL INDEX - have to use text_with_inlined_params.
        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE SPATIAL INDEX {self.quote(index_name)}
                ON {self.table_identifier(dataset)} ({self.quote(geom_col)})
                WITH (BOUNDING_BOX = (:min_x, :min_y, :max_x, :max_y));
                """,
                {"min_x": min_x, "min_y": min_y, "max_x": max_x, "max_y": max_y},
            )
        )

        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _drop_spatial_index(self, sess, dataset):
        # SQL server deletes the spatial index automatically when the table is deleted.
        pass

    def _sno_tracking_name(self, trigger_type, dataset):
        assert trigger_type == "trigger"
        assert dataset is not None
        # This is how the trigger is named in Sno 0.8.0 and earlier.
        # Newer repos that use kart branding use _kart_tracking_name.
        return f"{dataset.table_name}_sno_track"

    def _create_triggers(self, sess, dataset):
        pk_name = dataset.primary_key
        # Placeholders not allowed in CREATE TRIGGER - have to use text_with_inlined_params.
        sess.execute(
            text_with_inlined_params(
                f"""
                CREATE TRIGGER {self._quoted_tracking_name("trigger", dataset)}
                ON {self.table_identifier(dataset)}
                AFTER INSERT, UPDATE, DELETE AS
                BEGIN
                    MERGE {self.KART_TRACK} TRA
                    USING
                        (SELECT :table_name1, {self.quote(pk_name)} FROM inserted
                        UNION SELECT :table_name2, {self.quote(pk_name)} FROM deleted)
                        AS SRC (table_name, pk)
                    ON SRC.table_name = TRA.table_name AND SRC.pk = TRA.pk
                    WHEN NOT MATCHED THEN INSERT (table_name, pk) VALUES (SRC.table_name, SRC.pk);
                END;
                """,
                {"table_name1": dataset.table_name, "table_name2": dataset.table_name},
            )
        )

    @contextlib.contextmanager
    def _suspend_triggers(self, sess, dataset):
        trigger_name = self._quoted_tracking_name("trigger", dataset)
        sess.execute(
            f"""DISABLE TRIGGER {trigger_name} ON {self.table_identifier(dataset)};"""
        )
        yield
        sess.execute(
            f"""ENABLE TRIGGER {trigger_name} ON {self.table_identifier(dataset)};"""
        )

    def meta_items(self, dataset):
        with self.session() as sess:
            table_info_sql = """
                SELECT
                    C.column_name, C.ordinal_position, C.data_type,
                    C.character_maximum_length, C.numeric_precision, C.numeric_scale,
                    KCU.ordinal_position AS pk_ordinal_position
                FROM information_schema.columns C
                LEFT OUTER JOIN information_schema.key_column_usage KCU
                ON (KCU.table_schema = C.table_schema)
                AND (KCU.table_name = C.table_name)
                AND (KCU.column_name = C.column_name)
                WHERE C.table_schema=:table_schema AND C.table_name=:table_name
                ORDER BY C.ordinal_position;
            """
            r = sess.execute(
                table_info_sql,
                {"table_schema": self.db_schema, "table_name": dataset.table_name},
            )
            ms_table_info = list(r)

            id_salt = f"{self.db_schema} {dataset.table_name} {self.get_db_tree()}"
            schema = sqlserver_adapter.sqlserver_to_v2_schema(ms_table_info, id_salt)
            yield "schema.json", schema.to_column_dicts()

    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        old_type = old_col_dict["dataType"]
        new_type = new_col_dict["dataType"]

        # Some types have to be approximated as other types in SQL Server, and they also lose any extra type info.
        if sqlserver_adapter.APPROXIMATED_TYPES.get(old_type) == new_type:
            new_col_dict["dataType"] = new_type = old_type
            for key in sqlserver_adapter.APPROXIMATED_TYPES_EXTRA_TYPE_INFO:
                new_col_dict[key] = old_col_dict.get(key)

        # Geometry type loses its extra type info when roundtripped through SQL Server.
        if new_type == "geometry":
            new_col_dict["geometryType"] = old_col_dict.get("geometryType")
            new_col_dict["geometryCRS"] = old_col_dict.get("geometryCRS")

        return new_type == old_type

    _UNSUPPORTED_META_ITEMS = (
        "title",
        "description",
        "metadata/dataset.json",
        "metadata.xml",
    )

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        super()._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)

        # Nowhere to put these in SQL Server WC
        for key in self._UNSUPPORTED_META_ITEMS:
            if key in ds_meta_items:
                del ds_meta_items[key]

        # Diffing CRS is not yet supported.
        for key in list(ds_meta_items.keys()):
            if key.startswith("crs/"):
                del ds_meta_items[key]

    def _is_meta_update_supported(self, dataset_version, meta_diff):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported if we drop and rewrite the table, but of course it is less efficient).
        meta_diff - DeltaDiff object containing the meta changes.
        """
        # For now, just always drop and rewrite.
        return not meta_diff


class InstanceFunction(Function):
    """
    An instance function that compiles like this when applied to an element:
    >>> element.function()
    Unlike a normal sqlalchemy function which would compile as follows:
    >>> function(element)
    """


@compiles(InstanceFunction)
def compile_instance_function(element, compiler, **kw):
    return "(%s).%s()" % (element.clauses, element.name)


class GeometryType(UserDefinedType):
    """UserDefinedType so that V2 geometry is adapted to MS binary format."""

    def __init__(self, crs_id):
        self.crs_id = crs_id

    def bind_processor(self, dialect):
        # 1. Writing - Python layer - convert Kart geometry to WKB
        return lambda geom: geom.to_wkb()

    def bind_expression(self, bindvalue):
        # 2. Writing - SQL layer - wrap in call to STGeomFromWKB to convert WKB to MS binary.
        return Function(
            quoted_name("geometry::STGeomFromWKB", False),
            bindvalue,
            self.crs_id,
            type_=self,
        )

    def column_expression(self, col):
        # 3. Reading - SQL layer - append with call to .STAsBinary() to convert MS binary to WKB.
        return InstanceFunction("STAsBinary", col, type_=self)

    def result_processor(self, dialect, coltype):
        # 4. Reading - Python layer - convert WKB to Kart geometry.
        return lambda wkb: Geometry.from_wkb(wkb)


class BaseDateOrTimeType(UserDefinedType):
    """
    UserDefinedType so we read dates, times, and datetimes as text.
    They are stored as date / time / datetime in SQL Server, but read back out as text.
    """

    def column_expression(self, col):
        # When reading, convert dates and times to strings using style 127: ISO8601 with time zone Z.
        # https://docs.microsoft.com/en-us/sql/t-sql/functions/cast-and-convert-transact-sql
        return Function(
            "CONVERT",
            literal_column("NVARCHAR"),
            col,
            literal_column("127"),
            type_=self,
        )
