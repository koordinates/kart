import contextlib
import logging
import time

from sqlalchemy.dialects.mssql.base import MSIdentifierPreparer
from sqlalchemy.orm import sessionmaker

from .db_server import DatabaseServer_WorkingCopy
from .table_defs import SqlServerKartTables
from kart import crs_util
from kart.schema import Schema
from kart.sqlalchemy import separate_last_path_part, text_with_inlined_params
from kart.sqlalchemy.adapter.sqlserver import KartAdapter_SqlServer


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

        self.check_valid_location(self.uri, repo)
        self.connect_uri, self.db_schema = separate_last_path_part(self.uri)

        self.adapter = KartAdapter_SqlServer
        self.engine = self.adapter.create_engine(self.connect_uri)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.preparer = MSIdentifierPreparer(self.engine.dialect)

        self.kart_tables = SqlServerKartTables(self.db_schema, repo.is_kart_branded)

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = self.adapter.v2_schema_to_sql_spec(dataset.schema, dataset)
        sess.execute(
            f"""CREATE TABLE {self.table_identifier(dataset)} ({table_spec});"""
        )
        sess.execute(
            "EXECUTE sys.sp_addextendedproperty 'MS_Description', :title, 'schema', :schema, 'table', :table",
            {
                "title": dataset.get_meta_item("title"),
                "schema": self.db_schema,
                "table": dataset.table_name,
            },
        )

    def _write_meta(self, sess, dataset):
        # There is no metadata stored anywhere except the table itself, so nothing to write.
        pass

    def _delete_meta(self, sess, dataset):
        # There is no metadata stored anywhere except the table itself, so nothing to delete.
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

    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        old_type = old_col_dict["dataType"]
        new_type = new_col_dict["dataType"]

        # Some types have to be approximated as other types in SQL Server, and they also lose any extra type info.
        if KartAdapter_SqlServer.APPROXIMATED_TYPES.get(old_type) == new_type:
            new_col_dict["dataType"] = new_type = old_type
            for key in KartAdapter_SqlServer.APPROXIMATED_TYPES_EXTRA_TYPE_INFO:
                new_col_dict[key] = old_col_dict.get(key)

        # Geometry type loses various extra type info when roundtripped through SQL Server.
        if new_type == "geometry":
            new_col_dict["geometryType"] = old_col_dict.get("geometryType")
            new_geometry_crs = new_col_dict.get("geometryCRS", "")
            # Custom CRS can't be stored in SQL Server - even the CRS authority can't be roundtripped:
            if new_geometry_crs.startswith("CUSTOM:"):
                suffix = new_geometry_crs[new_geometry_crs.index(":") :]
                if old_col_dict.get("geometryCRS", "").endswith(suffix):
                    new_col_dict["geometryCRS"] = old_col_dict["geometryCRS"]

        return new_type == old_type

    _UNSUPPORTED_META_ITEMS = (
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

        # Nowhere to put custom CRS in SQL Server, so remove custom CRS diffs.
        # The working copy doesn't know the true authority name, so refers to them all as CUSTOM.
        # Their original authority name could be anything.
        for wc_key in list(wc_meta_items.keys()):
            if not wc_key.startswith("crs/CUSTOM:"):
                continue
            del wc_meta_items[wc_key]
            suffix = wc_key[wc_key.index(':') :]
            matching_ds_keys = [
                d
                for d in ds_meta_items.keys()
                if d.startswith("crs/") and d.endswith(suffix)
            ]
            if len(matching_ds_keys) == 1:
                [ds_key] = matching_ds_keys
                del ds_meta_items[ds_key]

    def _is_builtin_crs(self, crs):
        auth_name, auth_code = crs_util.parse_authority(crs)
        return auth_name == "EPSG"

    def _is_schema_update_supported(self, schema_delta):
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_schema = Schema.from_column_dicts(schema_delta.old_value)
        new_schema = Schema.from_column_dicts(schema_delta.new_value)
        dt = old_schema.diff_type_counts(new_schema)

        # We support deletes, name_updates, and type_updates -
        # but we don't support any other type of schema update except by rewriting the entire table.
        dt.pop("deletes")
        dt.pop("name_updates")
        dt.pop("type_updates")
        return sum(dt.values()) == 0

    def _apply_meta_title(self, sess, dataset, src_value, dest_value):
        sess.execute(
            "EXECUTE sys.sp_addextendedproperty 'MS_Description', :title, 'schema', :schema, 'table', :table",
            {
                "title": dest_value,
                "schema": self.db_schema,
                "table": dataset.table_name,
            },
        )

    def _apply_meta_schema_json(self, sess, dataset, src_value, dest_value):
        src_schema = Schema.from_column_dicts(src_value)
        dest_schema = Schema.from_column_dicts(dest_value)

        diff_types = src_schema.diff_types(dest_schema)

        deletes = diff_types.pop("deletes")
        name_updates = diff_types.pop("name_updates")
        type_updates = diff_types.pop("type_updates")

        if any(dt for dt in diff_types.values()):
            raise RuntimeError(
                f"This schema change not supported by update - should be drop + re-write_full: {diff_types}"
            )

        table = dataset.table_name
        for col_id in deletes:
            src_name = src_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self.table_identifier(table)}
                DROP COLUMN {self.quote(src_name)};
                """
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            sess.execute(
                """sp_rename :qualifified_src_name, :dest_name, 'COLUMN';""",
                {
                    "qualifified_src_name": f"{self.db_schema}.{table}.{src_name}",
                    "dest_name": dest_name,
                },
            )

        for col_id in type_updates:
            col = dest_schema[col_id]
            dest_spec = KartAdapter_SqlServer.v2_column_schema_to_sqlserver_spec(
                col, dataset
            )
            sess.execute(
                f"""ALTER TABLE {self.table_identifier(table)} ALTER COLUMN {dest_spec};"""
            )
