import contextlib
import logging
import time


from sqlalchemy import Index
from sqlalchemy.dialects.postgresql.base import PGIdentifierPreparer
from sqlalchemy.orm import sessionmaker


from . import postgis_adapter
from .db_server import DatabaseServer_WorkingCopy
from .table_defs import PostgisKartTables
from sno import crs_util
from sno.schema import Schema
from sno.sqlalchemy.create_engine import postgis_engine


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

        self.check_valid_db_uri(self.uri, repo)
        self.db_uri, self.db_schema = self._separate_db_schema(self.uri)

        self.engine = postgis_engine(self.db_uri)
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
            self._drop_all_tables(sess)
            self._drop_all_functions(sess)
            if not keep_db_schema_if_possible:
                self._drop_schema(sess)

    def _drop_all_functions(self, sess):
        r = sess.execute(
            "SELECT proname from pg_proc WHERE pronamespace = (:schema)::regnamespace;",
            {"schema": self.db_schema},
        )
        function_identifiers = ", ".join((self.table_identifier(row[0]) for row in r))
        sess.execute(f"DROP FUNCTION IF EXISTS {function_identifiers};")

    def _create_table_for_dataset(self, sess, dataset):
        table_spec = postgis_adapter.v2_schema_to_postgis_spec(dataset.schema, dataset)
        sess.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_identifier(dataset)} ({table_spec});"""
        )

    def _write_meta(self, sess, dataset):
        """Write the title (as a comment) and the CRS. Other metadata is not stored in a PostGIS WC."""
        self._write_meta_title(sess, dataset)
        self._write_meta_crs(sess, dataset)

    def _write_meta_title(self, sess, dataset):
        """Write the dataset title as a comment on the table."""
        sess.execute(
            f"""COMMENT ON TABLE {self.table_identifier(dataset)} IS :comment""",
            {"comment": dataset.get_meta_item("title")},
        )

    def _write_meta_crs(self, sess, dataset):
        """Populate the spatial_ref_sys table with data from this dataset."""
        spatial_refs = postgis_adapter.generate_postgis_spatial_ref_sys(dataset)
        if not spatial_refs:
            return

        for sr in spatial_refs:
            # We do not automatically overwrite a CRS if it seems likely to be one
            # of the Postgis builtin definitions - Postgis has lots of EPSG and ESRI
            # definitions built-in, plus the 900913 (GOOGLE) definition.
            # See POSTGIS_WC.md for help on working with CRS definitions in a Postgis WC.
            is_postgis_builtin = sess.scalar(
                """
                SELECT 1 FROM spatial_ref_sys
                WHERE srid = :srid AND (auth_name IN ('EPSG', 'ESRI') OR srid = 900913)
                LIMIT 1
                """,
                sr,
            )
            if not is_postgis_builtin:
                sess.execute(
                    """
                    INSERT INTO spatial_ref_sys AS SRS (srid, auth_name, auth_srid, srtext, proj4text)
                    VALUES (:srid, :auth_name, :auth_srid, :srtext, :proj4text)
                    ON CONFLICT (srid) DO UPDATE
                        SET (auth_name, auth_srid, srtext, proj4text)
                        = (EXCLUDED.auth_name, EXCLUDED.auth_srid, EXCLUDED.srtext, EXCLUDED.proj4text)
                    """,
                    sr,
                )

    def delete_meta(self, dataset):
        """Delete any metadata that is only needed by this dataset."""
        pass  # There is no metadata except for the spatial_ref_sys table.

    def _create_spatial_index_post(self, sess, dataset):
        # Only implemented as _create_spatial_index_post:
        # It is more efficient to write the features first, then index them all in bulk.
        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")

        geom_col = dataset.geom_column_name
        index_name = f"{dataset.table_name}_idx_{geom_col}"
        table = self._table_def_for_dataset(dataset)

        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)
        t0 = time.monotonic()

        spatial_index = Index(
            index_name, table.columns[geom_col], postgresql_using="GIST"
        )
        spatial_index.create(sess.connection())
        sess.execute(f"""ANALYZE {self.table_identifier(dataset)};""")

        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _drop_spatial_index(self, sess, dataset):
        # PostGIS deletes the spatial index automatically when the table is deleted.
        pass

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

    def meta_items(self, dataset):
        with self.session() as sess:
            title = sess.scalar(
                "SELECT obj_description((:table_identifier)::regclass, 'pg_class');",
                {"table_identifier": f"{self.db_schema}.{dataset.table_name}"},
            )
            yield "title", title

            table_info_sql = """
                SELECT
                    C.column_name, C.ordinal_position, C.data_type, C.udt_name,
                    C.character_maximum_length, C.numeric_precision, C.numeric_scale,
                    KCU.ordinal_position AS pk_ordinal_position,
                    upper(postgis_typmod_type(A.atttypmod)) AS geometry_type,
                    postgis_typmod_srid(A.atttypmod) AS geometry_srid
                FROM information_schema.columns C
                LEFT OUTER JOIN information_schema.key_column_usage KCU
                ON (KCU.table_schema = C.table_schema)
                AND (KCU.table_name = C.table_name)
                AND (KCU.column_name = C.column_name)
                LEFT OUTER JOIN pg_attribute A
                ON (A.attname = C.column_name)
                AND (A.attrelid = (C.table_schema || '.' || C.table_name)::regclass::oid)
                WHERE C.table_schema=:table_schema AND C.table_name=:table_name
                ORDER BY C.ordinal_position;
            """
            r = sess.execute(
                table_info_sql,
                {"table_schema": self.db_schema, "table_name": dataset.table_name},
            )
            pg_table_info = list(r)

            spatial_ref_sys_sql = """
                SELECT SRS.* FROM spatial_ref_sys SRS
                LEFT OUTER JOIN geometry_columns GC ON (GC.srid = SRS.srid)
                WHERE GC.f_table_schema=:table_schema AND GC.f_table_name=:table_name;
            """
            r = sess.execute(
                spatial_ref_sys_sql,
                {"table_schema": self.db_schema, "table_name": dataset.table_name},
            )
            pg_spatial_ref_sys = list(r)

            id_salt = f"{self.db_schema} {dataset.table_name} {self.get_db_tree()}"
            schema = postgis_adapter.postgis_to_v2_schema(
                pg_table_info, pg_spatial_ref_sys, id_salt
            )
            yield "schema.json", schema.to_column_dicts()

            for crs_info in pg_spatial_ref_sys:
                wkt = crs_info["srtext"]
                id_str = crs_util.get_identifier_str(wkt)
                yield f"crs/{id_str}.wkt", crs_util.normalise_wkt(wkt)

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
            if postgis_adapter.APPROXIMATED_TYPES.get((old_type, old_size)) == (
                new_type,
                new_size,
            ):
                new_col_dict["size"] = old_size

        # PostGIS can't limit a blob's size to a certain maximum length.
        if old_type == "blob" and new_type == "blob":
            new_col_dict["length"] = old_col_dict.get("length")

        return new_type == old_type

    def _remove_hidden_meta_diffs(self, dataset, ds_meta_items, wc_meta_items):
        super()._remove_hidden_meta_diffs(dataset, ds_meta_items, wc_meta_items)

        # Nowhere to put these in postgis WC
        for key in self._UNSUPPORTED_META_ITEMS:
            if key in ds_meta_items:
                del ds_meta_items[key]

        for key in ds_meta_items.keys() & wc_meta_items.keys():
            if not key.startswith("crs/"):
                continue
            old_is_standard = crs_util.has_standard_authority(ds_meta_items[key])
            new_is_standard = crs_util.has_standard_authority(wc_meta_items[key])
            if old_is_standard and new_is_standard:
                # The WC and the dataset have different definitions of a standard eg EPSG:2193.
                # We hide this diff because - hopefully - they are both EPSG:2193 (which never changes)
                # but have unimportant minor differences, and we don't want to update the Postgis builtin version
                # with the dataset version, or update the dataset version from the Postgis builtin.
                del ds_meta_items[key]
                del wc_meta_items[key]
            # If either definition is custom, we keep the diff, since it could be important.

    def _is_meta_update_supported(self, dataset_version, meta_diff):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported if we drop and rewrite the table, but of course it is less efficient).
        meta_diff - DeltaDiff object containing the meta changes.
        """
        if not meta_diff:
            return True

        if "schema.json" not in meta_diff:
            return True

        schema_delta = meta_diff["schema.json"]
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
            f"COMMENT ON TABLE {self._table_identifier(dataset.table_name)} IS :comment",
            {"comment": dest_value},
        )

    def _apply_meta_description(self, sess, dataset, src_value, dest_value):
        pass  # This is a no-op for postgis

    def _apply_meta_metadata_dataset_json(self, sess, dataset, src_value, dest_value):
        pass  # This is a no-op for postgis

    def _apply_meta_metadata_xml(self, sess, dataset, src_value, dest_value):
        pass  # This is a no-op for postgis

    def _apply_meta_schema_json(self, sess, dataset, src_value, dest_value):
        src_schema = Schema.from_column_dicts(src_value)
        dest_schema = Schema.from_column_dicts(dest_value)

        diff_types = src_schema.diff_types(dest_schema)

        deletes = diff_types.pop("deletes")
        name_updates = diff_types.pop("name_updates")
        type_updates = diff_types.pop("type_updates")

        if any(dt for dt in diff_types.values()):
            raise RuntimeError(
                f"This schema change not supported by update - should be drop + rewrite_full: {diff_types}"
            )

        table = dataset.table_name
        for col_id in deletes:
            src_name = src_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self._table_identifier(table)}
                DROP COLUMN IF EXISTS {self.quote(src_name)};"""
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            sess.execute(
                f"""
                ALTER TABLE {self._table_identifier(table)}
                RENAME COLUMN {self.quote(src_name)} TO {self.quote(dest_name)};
                """
            )

        do_write_crs = False
        for col_id in type_updates:
            col = dest_schema[col_id]
            dest_type = postgis_adapter.v2_type_to_pg_type(col, dataset)

            if col.data_type == "geometry":
                crs_name = col.extra_type_info.get("geometryCRS")
                if crs_name is not None:
                    crs_id = crs_util.get_identifier_int_from_dataset(dataset, crs_name)
                    if crs_id is not None:
                        dest_type += f""" USING ST_SetSRID({self.quote(col.name)}"::GEOMETRY, {crs_id})"""
                        do_write_crs = True

            sess.execute(
                f"""ALTER TABLE {self._table_identifier(table)} ALTER COLUMN {self.quote(col.name)} TYPE {dest_type};"""
            )

        if do_write_crs:
            self._write_meta_crs(sess, dataset)
