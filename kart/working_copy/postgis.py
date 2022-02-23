import contextlib
import hashlib
import logging
import time


from sqlalchemy import Index
from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer
from sqlalchemy.orm import sessionmaker


from .db_server import DatabaseServer_WorkingCopy
from .table_defs import PostgisKartTables
from kart import crs_util
from kart.tabular.schema import Schema
from kart.sqlalchemy import separate_last_path_part
from kart.sqlalchemy.adapter.postgis import KartAdapter_Postgis


POSTGRES_MAX_IDENTIFIER_LENGTH = 63


class WorkingCopy_Postgis(DatabaseServer_WorkingCopy):
    """
    PosttGIS working copy implementation.

    Requirements:
    1. The database needs to exist
    2. If the dataset has geometry, then PostGIS (https://postgis.net/) v2.4 or newer needs
       to be installed into the database and available in the database user's search path
    3. The database user needs to be able to:
        - Create the specified schema (unless it already exists).
        - Create, delete and alter tables and triggers in the specified schema.
    """

    WORKING_COPY_TYPE_NAME = "PostGIS"
    URI_SCHEME = "postgresql"

    def __init__(self, repo, location):
        """
        uri: connection string of the form postgresql://[user[:password]@][netloc][:port][/dbname/schema][?param1=value1&...]
        """
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo
        self.uri = self.location = location

        self.check_valid_location(self.uri, repo)
        self.connect_uri, self.db_schema = separate_last_path_part(self.uri)

        self.adapter = KartAdapter_Postgis
        self.engine = self.adapter.create_engine(self.connect_uri)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.preparer = PGIdentifierPreparer(self.engine.dialect)

        self.kart_tables = PostgisKartTables(self.db_schema, repo.is_kart_branded)

    def create_common_functions(self, sess):
        sess.execute(
            f"""
            CREATE OR REPLACE FUNCTION {self._quoted_tracking_name("proc")}()
                RETURNS TRIGGER AS $body$
            DECLARE
                pk_field text := quote_ident(TG_ARGV[0]);
                pk_old text;
                pk_new text;
            BEGIN
                IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
                    EXECUTE 'SELECT $1.' || pk_field USING NEW INTO pk_new;

                    INSERT INTO {self.KART_TRACK} (table_name,pk) VALUES
                    (TG_TABLE_NAME::TEXT, pk_new)
                    ON CONFLICT DO NOTHING;
                END IF;
                IF (TG_OP = 'UPDATE' OR TG_OP = 'DELETE') THEN
                    EXECUTE 'SELECT $1.' || pk_field USING OLD INTO pk_old;

                    INSERT INTO {self.KART_TRACK} (table_name,pk) VALUES
                    (TG_TABLE_NAME::TEXT, pk_old)
                    ON CONFLICT DO NOTHING;

                    IF (TG_OP = 'DELETE') THEN
                        RETURN OLD;
                    END IF;
                END IF;
                RETURN NEW;
            END;
            $body$
            LANGUAGE plpgsql
            SECURITY DEFINER
            """
        )

    def delete(self, keep_db_schema_if_possible=False):
        # We don't use DROP SCHEMA CASCADE since that could possibly delete things outside the schema
        # if they've been linked to it using foreign keys, and we only want to delete the schema that we manage.
        with self.session() as sess:
            # Don't worry about constraints when dropping everything.
            sess.execute("SET CONSTRAINTS ALL DEFERRED;")
            self.adapter.drop_all_in_schema(sess, self.db_schema)

        if not keep_db_schema_if_possible:
            with self.session() as sess:
                self._drop_schema(sess, treat_error_as_warning=True)

    def _drop_all_functions(self, sess):
        r = sess.execute(
            "SELECT proname from pg_proc WHERE pronamespace = (:schema)::regnamespace;",
            {"schema": self.db_schema},
        )
        function_identifiers = ", ".join((self.table_identifier(row[0]) for row in r))
        sess.execute(f"DROP FUNCTION IF EXISTS {function_identifiers};")

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = self.adapter.v2_schema_to_sql_spec(dataset.schema, dataset)
        sess.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_identifier(dataset)} ({table_spec});"""
        )
        sess.execute(
            f"""COMMENT ON TABLE {self.table_identifier(dataset)} IS :comment""",
            {"comment": dataset.get_meta_item("title")},
        )

    def _write_meta(self, sess, dataset):
        # The only metadata to write that is stored outside the table is custom CRS.
        for crs in KartAdapter_Postgis.generate_postgis_spatial_ref_sys(dataset):
            existing_crs = sess.execute(
                "SELECT auth_name, srtext FROM spatial_ref_sys WHERE srid = :srid;",
                crs,
            ).fetchone()

            if existing_crs:
                # Don't overwrite existing CRS definitions if they are built-ins.
                if existing_crs['auth_name'] in ("EPSG", "ESRI"):
                    continue
                # Don't try to replace a CRS if a matching one already exists - overwriting a CRS with an identical
                # CRS is a no-op, but one which requires certain permissions, so we avoid it if we can.
                if existing_crs['srtext'] == crs['srtext']:
                    continue
                # Don't replace a CRS definition if it is currently being referenced.
                if sess.scalar(
                    "SELECT COUNT(*) FROM geometry_columns WHERE srid = :srid;", crs
                ):
                    continue

            sess.execute(
                """
                INSERT INTO spatial_ref_sys AS SRS (srid, auth_name, auth_srid, srtext, proj4text)
                VALUES (:srid, :auth_name, :auth_srid, :srtext, :proj4text)
                ON CONFLICT (srid) DO UPDATE
                    SET (auth_name, auth_srid, srtext, proj4text)
                    = (EXCLUDED.auth_name, EXCLUDED.auth_srid, EXCLUDED.srtext, EXCLUDED.proj4text)
                """,
                crs,
            )

    def _delete_meta(self, sess, dataset):
        # The only metadata outside the table itself is CRS definitions. We don't delete them however, for 2 reasons:
        # 1. CRS definitions have global scope and we can't tell if we created them. Even if they're not being used
        # right now, somebody else might have created them and expect them to stay where they are until they are needed.
        # 2. We might need that CRS definition again in a minute (eg next time we switch branch) and we might lack
        # permissions to create or delete CRS definitions. Better to just leave things as-is.
        pass

    def _create_spatial_index_post(self, sess, dataset):
        # Only implemented as _create_spatial_index_post:
        # It is more efficient to write the features first, then index them all in bulk.
        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")

        geom_col = dataset.geom_column_name
        index_name = f"{dataset.table_name}_idx_{geom_col}"
        if len(index_name) > POSTGRES_MAX_IDENTIFIER_LENGTH:
            # postgres can't handle identifiers this long.
            # so make it shorter, but keep it unique
            sha1 = hashlib.sha1(index_name.encode("utf-8")).hexdigest()
            keep_chars = POSTGRES_MAX_IDENTIFIER_LENGTH - 40 - 1
            index_name = f"{dataset.table_name[:keep_chars]}_{sha1}"

        table = self._table_def_for_dataset(dataset)

        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)
        t0 = time.monotonic()

        spatial_index = Index(
            index_name, table.columns[geom_col], postgresql_using="GIST"
        )
        spatial_index.create(sess.connection())
        sess.execute(f"""ANALYZE {self.table_identifier(dataset)};""")

        L.info("Created spatial index in %.1fs", time.monotonic() - t0)

    def _drop_spatial_index(self, sess, dataset):
        # PostGIS deletes the spatial index automatically when the table is deleted.
        pass

    def _initialise_sequence(self, sess, dataset):
        start = dataset.feature_path_encoder.find_start_of_unassigned_range(dataset)
        if start:
            quoted_table_name = ".".join(
                [self.quote(self.db_schema), self.quote(dataset.table_name)]
            )
            quoted_seq_identifier = sess.scalar(
                "SELECT pg_get_serial_sequence(:table_name, :col_name);",
                {
                    "table_name": quoted_table_name,
                    "col_name": dataset.primary_key,
                },
            )
            sess.execute(
                f"ALTER SEQUENCE {quoted_seq_identifier} RESTART WITH :start;",
                {"start": start},
            )

    def _sno_tracking_name(self, trigger_type, dataset):
        assert trigger_type in ("trigger", "proc")
        assert dataset is None
        # This is how the triggers are named in Sno 0.8.0 and earlier.
        # Newer repos that use kart branding use _kart_tracking_name.
        # The existing names are kind of backwards:
        if trigger_type == "trigger":
            return "sno_track"
        elif trigger_type == "proc":
            return "_sno_track_trigger"

    def _trigger_type_requires_db_schema(self, trigger_type):
        # Actual triggers (so, anything that's not "proc", a procedure that a trigger calls) -
        # are namespaced within the table they are attached to, so they don't require a db_schema.
        return trigger_type == "proc"

    def _create_triggers(self, sess, dataset):
        sess.execute(
            f"""
            CREATE TRIGGER {self._quoted_tracking_name("trigger")}
                AFTER INSERT OR UPDATE OR DELETE ON {self.table_identifier(dataset)}
            FOR EACH ROW EXECUTE PROCEDURE {self._quoted_tracking_name("proc")}(:pk_field)
            """,
            {"pk_field": dataset.primary_key},
        )

    @contextlib.contextmanager
    def _suspend_triggers(self, sess, dataset):
        sess.execute(
            f"""
            ALTER TABLE {self.table_identifier(dataset)}
            DISABLE TRIGGER {self._quoted_tracking_name("trigger")};
            """
        )
        yield
        sess.execute(
            f"""
            ALTER TABLE {self.table_identifier(dataset)}
            ENABLE TRIGGER {self._quoted_tracking_name("trigger")};
            """
        )

    # Postgis has nowhere obvious to put this metadata.
    _UNSUPPORTED_META_ITEMS = ("description", "metadata/dataset.json", "metadata.xml")

    # PostGIS approximates an int8 as an int16 - see super()._remove_hidden_meta_diffs
    @classmethod
    def try_align_schema_col(cls, old_col_dict, new_col_dict):
        old_type = old_col_dict["dataType"]
        new_type = new_col_dict["dataType"]

        # PostGIS can't store a certain integer size.
        if old_type == "integer" and new_type == "integer":
            old_size = old_col_dict.get("size")
            new_size = new_col_dict.get("size")
            if KartAdapter_Postgis.APPROXIMATED_TYPES.get((old_type, old_size)) == (
                new_type,
                new_size,
            ):
                new_col_dict["size"] = old_size

        # PostGIS can't limit a blob's size to a certain maximum length.
        if old_type == new_type == "blob":
            new_col_dict["length"] = old_col_dict.get("length")

        return new_type == old_type

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        super()._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)

        # Nowhere to put these in postgis WC
        for key in self._UNSUPPORTED_META_ITEMS:
            if key in ds_meta_items:
                del ds_meta_items[key]

    def _is_builtin_crs(self, crs):
        auth_name, auth_code = crs_util.parse_authority(crs)
        return auth_name in ("EPSG", "ESRI") or auth_code == "900913"  # GOOGLE

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
            f"COMMENT ON TABLE {self.table_identifier(dataset.table_name)} IS :comment",
            {"comment": dest_value},
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
                DROP COLUMN IF EXISTS {self.quote(src_name)};"""
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self.table_identifier(table)}
                RENAME COLUMN {self.quote(src_name)} TO {self.quote(dest_name)};
                """
            )

        for col_id in type_updates:
            col = dest_schema[col_id]
            dest_type = KartAdapter_Postgis.v2_type_to_sql_type(col, dataset)
            sess.execute(
                f"""ALTER TABLE {self.table_identifier(table)} ALTER COLUMN {self.quote(col.name)} TYPE {dest_type};"""
            )
