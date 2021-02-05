import contextlib
import logging
import re
import time
from urllib.parse import urlsplit, urlunsplit

import click
from sqlalchemy.sql.compiler import IdentifierPreparer
from sqlalchemy.orm import sessionmaker


from .base import WorkingCopy
from . import postgis_adapter
from .table_defs import PostgisSnoTables
from sno import crs_util
from sno.schema import Schema
from sno.sqlalchemy import postgis_engine


"""
* database needs to exist
* database needs to have postgis enabled
* database user needs to be able to:
    1. create 'sno' schema & tables
    2. create & alter tables in the default (or specified) schema
    3. create triggers
"""

L = logging.getLogger("sno.working_copy.postgis")


class WorkingCopy_Postgis(WorkingCopy):
    def __init__(self, repo, uri):
        """
        uri: connection string of the form postgresql://[user[:password]@][netloc][:port][/dbname/schema][?param1=value1&...]
        """
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo
        self.uri = uri
        self.path = uri

        url = urlsplit(uri)

        if url.scheme != "postgresql":
            raise ValueError("Expecting postgresql://")

        url_path = url.path
        path_parts = url_path[1:].split("/", 3) if url_path else []
        if len(path_parts) != 2:
            raise ValueError("Expecting postgresql://[HOST]/DBNAME/SCHEMA")
        url_path = f"/{path_parts[0]}"
        self.db_schema = path_parts[1]

        url_query = url.query
        if "fallback_application_name" not in url_query:
            url_query = "&".join(
                filter(None, [url_query, "fallback_application_name=sno"])
            )

        # Rebuild DB URL suitable for postgres
        self.dburl = urlunsplit([url.scheme, url.netloc, url_path, url_query, ""])
        self.engine = postgis_engine(self.dburl)
        self.sessionmaker = sessionmaker(bind=self.engine)
        self.preparer = IdentifierPreparer(self.engine.dialect)

        self.sno_tables = PostgisSnoTables(self.db_schema)

    @classmethod
    def check_valid_uri(cls, uri, workdir_path):
        url = urlsplit(uri)

        if url.scheme != "postgresql":
            raise click.UsageError(
                "Invalid postgres URI - Expecting URI in form: postgresql://[HOST]/DBNAME/SCHEMA"
            )

        url_path = url.path
        path_parts = url_path[1:].split("/", 3) if url_path else []

        suggestion_message = ""
        if len(path_parts) == 1 and workdir_path is not None:
            suggested_path = f"/{path_parts[0]}/{cls.default_schema(workdir_path)}"
            suggested_uri = urlunsplit(
                [url.scheme, url.netloc, suggested_path, url.query, ""]
            )
            suggestion_message = f"\nFor example: {suggested_uri}"

        if len(path_parts) != 2:
            raise click.UsageError(
                "Invalid postgres URI - postgis working copy requires both dbname and schema:\n"
                "Expecting URI in form: postgresql://[HOST]/DBNAME/SCHEMA"
                + suggestion_message
            )

    @classmethod
    def default_schema(cls, workdir_path):
        stem = workdir_path.stem
        schema = re.sub("[^a-z0-9]+", "_", stem.lower()) + "_sno"
        if schema[0].isdigit():
            schema = "_" + schema
        return schema

    def __str__(self):
        p = urlsplit(self.uri)
        if p.password is not None:
            nl = p.hostname
            if p.username is not None:
                nl = f"{p.username}@{nl}"
            if p.port is not None:
                nl += f":{p.port}"

            p._replace(netloc=nl)
        return p.geturl()

    @contextlib.contextmanager
    def session(self, bulk=0):
        """
        Context manager for GeoPackage DB sessions, yields a connection object inside a transaction

        Calling again yields the _same_ connection, the transaction/etc only happen in the outer one.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, "_session"):
            # inner - reuse
            L.debug("session: existing...")
            yield self._session
            L.debug("session: existing/done")

        else:
            L.debug("session: new...")

            try:
                self._session = self.sessionmaker()

                # TODO - use tidier syntax for opening transactions from sqlalchemy.
                self._session.execute("BEGIN TRANSACTION;")
                yield self._session
                self._session.commit()
            except Exception:
                self._session.rollback()
                raise
            finally:
                self._session.close()
                del self._session
                L.debug("session: new/done")

    def is_created(self):
        """
        Returns true if the postgres schema referred to by this working copy exists and
        contains at least one table. If it exists but is empty, it is treated as uncreated.
        This is so the postgres schema can be created ahead of time before a repo is created
        or configured, without it triggering code that checks for corrupted working copies.
        Note that it might not be initialised as a working copy - see self.is_initialised.
        """
        with self.session() as db:
            count = db.scalar(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema;
                """,
                {"table_schema": self.db_schema},
            )
            return count > 0

    def is_initialised(self):
        """
        Returns true if the postgis working copy is initialised -
        the schema exists and has the necessary sno tables, _sno_state and _sno_track.
        """
        with self.session() as db:
            count = db.scalar(
                f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema AND table_name IN ('{self.SNO_STATE_NAME}', '{self.SNO_TRACK_NAME}');
                """,
                {"table_schema": self.db_schema},
            )
            return count == 2

    def has_data(self):
        """
        Returns true if the postgis working copy seems to have user-created content already.
        """
        with self.session() as db:
            count = db.scalar(
                f"""
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema=:table_schema AND table_name NOT IN ('{self.SNO_STATE_NAME}', '{self.SNO_TRACK_NAME}');
                """,
                {"table_schema": self.db_schema},
            )
            return count > 0

    def create_and_initialise(self):
        with self.session() as db:
            db.execute(f"CREATE SCHEMA IF NOT EXISTS {self.DB_SCHEMA};")
            self.sno_tables.create_all(db)

            db.execute(
                f"""
                CREATE OR REPLACE FUNCTION {self.DB_SCHEMA}._sno_track_trigger() RETURNS TRIGGER AS $body$
                DECLARE
                    pk_field text := quote_ident(TG_ARGV[0]);
                    pk_old text;
                    pk_new text;
                BEGIN
                    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
                        EXECUTE 'SELECT $1.' || pk_field USING NEW INTO pk_new;

                        INSERT INTO {self.SNO_TRACK} (table_name,pk) VALUES
                        (TG_TABLE_NAME::TEXT, pk_new)
                        ON CONFLICT DO NOTHING;
                    END IF;
                    IF (TG_OP = 'UPDATE' OR TG_OP = 'DELETE') THEN
                        EXECUTE 'SELECT $1.' || pk_field USING OLD INTO pk_old;

                        INSERT INTO {self.SNO_TRACK} (table_name,pk) VALUES
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
        """Delete all tables in the schema."""
        # We don't use drop ... cascade since that could also delete things outside the schema.
        # Better to fail to delete the schema, than to delete things the user didn't want to delete.
        with self.session() as db:
            # Don't worry about constraints when dropping everything.
            db.execute("SET CONSTRAINTS ALL DEFERRED;")
            # Drop tables
            r = db.execute(
                "SELECT tablename FROM pg_tables where schemaname=:schema;",
                {"schema": self.db_schema},
            )
            if r:
                table_identifiers = ", ".join(
                    (self.table_identifier(row[0]) for row in r)
                )
                db.execute(f"DROP TABLE IF EXISTS {table_identifiers};")

            # Drop functions
            r = db.execute(
                "SELECT proname from pg_proc WHERE pronamespace = (:schema)::regnamespace;",
                {"schema": self.db_schema},
            )
            if r:
                function_identifiers = ", ".join(
                    (self.table_identifier(row[0]) for row in r)
                )
                db.execute(f"DROP FUNCTION IF EXISTS {function_identifiers};")

            # Drop schema, unless keep_db_schema_if_possible=True
            if not keep_db_schema_if_possible:
                db.execute(f"""DROP SCHEMA IF EXISTS {self.DB_SCHEMA};""")

    def _create_table_for_dataset(self, db, dataset):
        table_spec = postgis_adapter.v2_schema_to_postgis_spec(dataset.schema, dataset)
        db.execute(
            f"""CREATE TABLE IF NOT EXISTS {self.table_identifier(dataset)} ({table_spec});"""
        )

    def _write_meta(self, db, dataset):
        """Write the title (as a comment) and the CRS. Other metadata is not stored in a PostGIS WC."""
        self._write_meta_title(db, dataset)
        self._write_meta_crs(db, dataset)

    def _write_meta_title(self, db, dataset):
        """Write the dataset title as a comment on the table."""
        db.execute(
            f"""COMMENT ON TABLE {self.table_identifier(dataset)} IS :comment""",
            {"comment": dataset.get_meta_item("title")},
        )

    def _write_meta_crs(self, db, dataset):
        """Populate the spatial_ref_sys table with data from this dataset."""
        spatial_refs = postgis_adapter.generate_postgis_spatial_ref_sys(dataset)
        if not spatial_refs:
            return

        # We do not automatically overwrite a CRS if it seems likely to be one
        # of the Postgis builtin definitions - Postgis has lots of EPSG and ESRI
        # definitions built-in, plus the 900913 (GOOGLE) definition.
        # See POSTGIS_WC.md for help on working with CRS definitions in a Postgis WC.
        db.execute(
            """
            INSERT INTO spatial_ref_sys AS SRS (srid, auth_name, auth_srid, srtext, proj4text)
            VALUES (:srid, :auth_name, :auth_srid, :srtext, :proj4text)
            ON CONFLICT (srid) DO UPDATE
                SET (auth_name, auth_srid, srtext, proj4text)
                = (EXCLUDED.auth_name, EXCLUDED.auth_srid, EXCLUDED.srtext, EXCLUDED.proj4text)
                WHERE SRS.auth_name NOT IN ('EPSG', 'ESRI') AND SRS.srid <> 900913;
            """,
            spatial_refs,
        )

    def delete_meta(self, dataset):
        """Delete any metadata that is only needed by this dataset."""
        pass  # There is no metadata except for the spatial_ref_sys table.

    def _create_spatial_index(self, db, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")

        geom_col = dataset.geom_column_name

        # Create the PostGIS Spatial Index
        L.debug("Creating spatial index for %s.%s", dataset.table_name, geom_col)
        t0 = time.monotonic()
        db.execute(
            f"""
            CREATE INDEX "{dataset.table_name}_idx_{geom_col}"
            ON {self.table_identifier(dataset)} USING GIST ({self.quote(geom_col)});
            """
        )
        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _drop_spatial_index(self, dbcur, dataset):
        # PostGIS deletes the spatial index automatically when the table is deleted.
        pass

    def _create_triggers(self, db, dataset):
        db.execute(
            f"""
            CREATE TRIGGER "sno_track" AFTER INSERT OR UPDATE OR DELETE ON {self.table_identifier(dataset)}
            FOR EACH ROW EXECUTE PROCEDURE {self.DB_SCHEMA}._sno_track_trigger(:pk_field)
            """,
            {"pk_field": dataset.primary_key},
        )

    @contextlib.contextmanager
    def _suspend_triggers(self, db, dataset):
        db.execute(
            f"""ALTER TABLE {self.table_identifier(dataset)} DISABLE TRIGGER "sno_track";"""
        )
        yield
        db.execute(
            f"""ALTER TABLE {self.table_identifier(dataset)} ENABLE TRIGGER "sno_track";"""
        )

    def meta_items(self, dataset):
        with self.session() as db:
            title = db.scalar(
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
            r = db.execute(
                table_info_sql,
                {"table_schema": self.db_schema, "table_name": dataset.table_name},
            )
            pg_table_info = list(r)

            spatial_ref_sys_sql = """
                SELECT SRS.* FROM spatial_ref_sys SRS
                LEFT OUTER JOIN geometry_columns GC ON (GC.srid = SRS.srid)
                WHERE GC.f_table_schema=:table_schema AND GC.f_table_name=:table_name;
            """
            r = db.execute(
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
    _UNSUPPORTED_META_ITEMS = ("description", "metadata/dataset.json")

    # Postgis approximates an int8 as an int16 - see super()._remove_hidden_meta_diffs
    _APPROXIMATED_TYPES = postgis_adapter.APPROXIMATED_TYPES

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

    def _db_geom_to_gpkg_geom(self, g):
        # This is already handled by register_type
        return g

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

    def _apply_meta_title(self, dataset, src_value, dest_value, db):
        db.execute(
            f"COMMENT ON TABLE {self._table_identifier(dataset.table_name)} IS :comment",
            {"comment": dest_value},
        )

    def _apply_meta_description(self, dataset, src_value, dest_value, db):
        pass  # This is a no-op for postgis

    def _apply_meta_metadata_dataset_json(self, dataset, src_value, dest_value, db):
        pass  # This is a no-op for postgis

    def _apply_meta_schema_json(self, dataset, src_value, dest_value, db):
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
            db.execute(
                f"""
                ALTER TABLE {self._table_identifier(table)}
                DROP COLUMN IF EXISTS {self.quote(src_name)};"""
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            db.execute(
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

            db.execute(
                f"""ALTER TABLE {self._table_identifier(table)} ALTER COLUMN {self.quote(col.name)} TYPE {dest_type};"""
            )

        if do_write_crs:
            self._write_meta_crs(db, dataset)
