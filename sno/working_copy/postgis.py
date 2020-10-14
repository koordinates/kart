import contextlib
import logging
import re
import time
from urllib.parse import urlsplit, urlunsplit

import pygit2
import psycopg2
from psycopg2.extensions import new_type, register_adapter, register_type, Binary
from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier, SQL


from .base import WorkingCopy
from . import postgis_adapter
from sno import geometry, crs_util
from sno.db_util import changes_rowcount
from sno.filter_util import UNFILTERED

"""
* database needs to exist
* database needs to have postgis enabled
* database user needs to be able to:
    1. create 'sno' schema & tables
    2. create & alter tables in the default (or specified) schema
    3. create triggers
"""

L = logging.getLogger("sno.working_copy.postgis")


def _adapt_geometry_to_db(g):
    return Binary(geometry.gpkg_geom_to_ewkb(g))


register_adapter(geometry.Geometry, _adapt_geometry_to_db)


def adapt_geometry_from_db(g, dbcur):
    return geometry.hexewkb_to_gpkg_geom(g)


def adapt_timestamp_from_db(t, dbcur):
    # TODO - revisit timezones.
    if isinstance(t, str):
        # This makes postgis timestamps behave more like GPKG ones.
        return t.replace(" ", "T") + "Z"
    return t


# See https://github.com/psycopg/psycopg2/blob/master/psycopg/typecast_builtins.c
TIMESTAMP_OID = 1114
TIMESTAMP = new_type((TIMESTAMP_OID,), "TIMESTAMP", adapt_timestamp_from_db)
psycopg2.extensions.register_type(TIMESTAMP)


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
            raise ValueError("Expecting postgresql://[host]/dbname/schema")
        url_path = f"/{path_parts[0]}"
        self.schema = path_parts[1]

        url_query = url.query
        if "fallback_application_name" not in url_query:
            url_query = "&".join(
                filter(None, [url_query, "fallback_application_name=sno"])
            )

        # rebuild DB URL suitable for libpq
        self.dburl = urlunsplit([url.scheme, url.netloc, url_path, url_query, ""])

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

    def _sno_table(self, sno_table):
        return self._table_identifier(f"_sno_{sno_table}")

    def _table_identifier(self, table):
        return Identifier(self.schema, table)

    @classmethod
    def default_schema(cls, repo):
        stem = repo.workdir_path.stem
        schema = re.sub("[^a-z0-9]+", "_", stem.lower()) + "_sno"
        if schema[0].isdigit():
            schema = "_" + schema
        return schema

    @contextlib.contextmanager
    def session(self, bulk=0):
        """
        Context manager for PostgreSQL DB sessions, yields a connection object inside a transaction

        Calling again yields the _same_ connection, the transaction/etc only happen in the outer one.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, "_db"):
            # inner - reuse
            L.debug("session: existing...")
            yield self._db
            L.debug("session: existing/done")
        else:
            L.debug("session: new...")
            self._db = psycopg2.connect(self.dburl, cursor_factory=DictCursor)
            self._register_geometry_type(self._db)

            try:
                yield self._db
            except:  # noqa
                self._db.rollback()
                raise
            else:
                self._db.commit()
            finally:
                self._db.close()
                del self._db
                L.debug("session: new/done")

    def _register_geometry_type(self, db):
        """
        Register adapt_geometry_from_db for the type with OID: 'geometry'::regtype::oid
        - which could be different in different postgis databases, and might not even exist.
        """
        dbcur = db.cursor()
        dbcur.execute("SELECT oid FROM pg_type WHERE typname='geometry';")
        r = dbcur.fetchone()
        if r:
            geometry_oid = r[0]
            geometry = new_type((geometry_oid,), "GEOMETRY", adapt_geometry_from_db)
            register_type(geometry, db)

    def is_created(self):
        """
        Returns true if the schema is created. The contents of the schema is the working copy,
        so the working copy is created if the schema is created.
        """
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(
                SQL(
                    "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name=%s)"
                ),
                (self.schema,),
            )
            return bool(dbcur.fetchone()[0])

    def create(self):
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(
                SQL("CREATE SCHEMA IF NOT EXISTS {}").format(Identifier(self.schema))
            )
            dbcur.execute(
                SQL(
                    """
                CREATE TABLE IF NOT EXISTS {} (
                    table_name TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NULL,
                    PRIMARY KEY (table_name, key)
                );
                """
                ).format(self.STATE_TABLE)
            )
            dbcur.execute(
                SQL(
                    """
                CREATE TABLE IF NOT EXISTS {} (
                    table_name TEXT NOT NULL,
                    pk TEXT NULL,
                    PRIMARY KEY (table_name, pk)
                );
                """
                ).format(self.TRACKING_TABLE)
            )
            dbcur.execute(
                SQL(
                    """
                CREATE OR REPLACE FUNCTION {func}() RETURNS TRIGGER AS $body$
                DECLARE
                    pk_field text := quote_ident(TG_ARGV[0]);
                    pk_old text;
                    pk_new text;
                BEGIN
                    IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
                        EXECUTE 'SELECT $1.' || pk_field USING NEW INTO pk_new;

                        INSERT INTO {tracking_table} (table_name,pk) VALUES
                        (TG_TABLE_NAME::TEXT, pk_new)
                        ON CONFLICT DO NOTHING;
                    END IF;
                    IF (TG_OP = 'UPDATE' OR TG_OP = 'DELETE') THEN
                        EXECUTE 'SELECT $1.' || pk_field USING OLD INTO pk_old;

                        INSERT INTO {tracking_table} (table_name,pk) VALUES
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
                ).format(
                    func=Identifier(self.schema, "_sno_track_trigger"),
                    tracking_table=self.TRACKING_TABLE,
                )
            )

    def delete(self):
        """ Delete all tables in the schema"""

        # TODO - don't use DROP SCHEMA CASCADE, since this could even delete user-created tables outside the schema
        # if they have been connected to the schema with a foreign key relation.
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(SQL("DROP SCHEMA {} CASCADE").format(Identifier(self.schema)))

    def write_meta(self, dataset):
        # TODO - we should be writing any custom CRSs to public.spatial_ref_sys
        # Not entirely straight-forward - what if the supplied CRS is a slightly modified EPSG definition?
        # Should we overwrite the data in the table?
        pass

    def _create_spatial_index(self, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}._create_spatial_index")

        geom_col = dataset.geom_column_name

        # Create the PostGIS Spatial Index
        sql = SQL("CREATE INDEX {} ON {} USING GIST ({})").format(
            Identifier(f"{dataset.table_name}_idx_{geom_col}"),
            self._table_identifier(dataset.table_name),
            Identifier(geom_col),
        )
        L.debug(
            "Creating spatial index for %s.%s: %s", dataset.table_name, geom_col, sql
        )
        t0 = time.monotonic()
        with self.session() as db:
            db.cursor().execute(sql)
        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _create_triggers(self, dbcur, dataset):
        pk_field = dataset.primary_key
        dbcur.execute(
            SQL(
                """
            CREATE TRIGGER {} AFTER INSERT OR UPDATE OR DELETE ON {}
            FOR EACH ROW EXECUTE PROCEDURE {}(%s)
            """
            ).format(
                Identifier("sno_track"),
                self._table_identifier(dataset.table_name),
                Identifier(self.schema, "_sno_track_trigger"),
            ),
            (pk_field,),
        )

    @contextlib.contextmanager
    def _suspend_triggers(self, dbcur, dataset):
        table = dataset.table_name
        dbcur.execute(
            SQL("ALTER TABLE {} DISABLE TRIGGER sno_track").format(
                self._table_identifier(table),
            )
        )

        try:
            yield
        finally:
            dbcur.execute(
                SQL("ALTER TABLE {} ENABLE TRIGGER sno_track").format(
                    self._table_identifier(table),
                )
            )

    def get_db_tree(self, table_name="*"):
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(
                SQL("SELECT value FROM {} WHERE table_name=%s AND key=%s").format(
                    self.STATE_TABLE
                ),
                (table_name, "tree"),
            )
            row = dbcur.fetchone()
            if not row:
                raise ValueError(f"No meta entry for {table_name}")

            wc_tree_id = row[0]
            return wc_tree_id

    def _placeholders_with_setsrid(self, dataset):
        # We have to call SetSRID on all geometries so that they will fit in their columns:
        # Unlike GPKG, the geometries in a column with a CRS of X can't all just have a CRS of 0.
        result = [SQL("%s")] * len(dataset.schema.columns)
        for i, col in enumerate(dataset.schema):
            if col.data_type != "geometry":
                continue
            crs_name = col.extra_type_info.get("geometryCRS", None)
            if crs_name is None:
                continue
            crs_id = crs_util.get_identifier_int_from_dataset(dataset, crs_name)
            result[i] = SQL(f"SetSRID(%s, {crs_id})")
        return result

    def write_full(self, commit, *datasets, **kwargs):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        with self.session(bulk=2) as db:
            dbcur = db.cursor()

            dbcur.execute(
                SQL("CREATE SCHEMA IF NOT EXISTS {};").format(Identifier(self.schema))
            )

            for dataset in datasets:
                table = dataset.table_name

                # Create the table
                table_spec = postgis_adapter.v2_schema_to_postgis_spec(
                    dataset.schema, dataset
                )
                col_names = [col.name for col in dataset.schema]

                dbcur.execute(
                    SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
                        self._table_identifier(table), table_spec
                    )
                )
                self.write_meta(dataset)

                L.info("Creating features...")
                sql_insert_features = SQL(
                    """
                    INSERT INTO {} ({}) VALUES ({});
                """
                ).format(
                    self._table_identifier(table),
                    SQL(",").join([Identifier(c) for c in col_names]),
                    SQL(",").join(self._placeholders_with_setsrid(dataset)),
                )

                feat_count = 0
                t0 = time.monotonic()
                t0p = t0

                CHUNK_SIZE = 10000
                for rows in self._chunk(dataset.feature_tuples(col_names), CHUNK_SIZE):

                    dbcur.executemany(sql_insert_features, rows)
                    feat_count += changes_rowcount(dbcur)

                    nc = feat_count / CHUNK_SIZE
                    if nc % 5 == 0 or not nc.is_integer():
                        t0a = time.monotonic()
                        L.info(
                            "%s features... @%.1fs (+%.1fs, ~%d F/s)",
                            feat_count,
                            t0a - t0,
                            t0a - t0p,
                            (CHUNK_SIZE * 5) / (t0a - t0p),
                        )
                        t0p = t0a

                t1 = time.monotonic()
                L.info("Added %d features to GPKG in %.1fs", feat_count, t1 - t0)
                L.info("Overall rate: %d features/s", (feat_count / (t1 - t0)))

                if dataset.has_geometry:
                    self._create_spatial_index(dataset)

                # Create triggers
                self._create_triggers(dbcur, dataset)

            dbcur.execute(
                SQL(
                    """
                    INSERT INTO {} (table_name, key, value) VALUES (%s, %s, %s)
                    ON CONFLICT (table_name, key) DO UPDATE
                    SET value=EXCLUDED.value;
                """
                ).format(self.STATE_TABLE),
                ("*", "tree", commit.peel(pygit2.Tree).hex),
            )

    def write_features(self, dbcur, dataset, pk_iter, *, ignore_missing=False):
        pk_field = dataset.primary_key
        col_names = [col.name for col in dataset.schema]

        sql_write_feature = SQL(
            """
            INSERT INTO {table} ({cols}) VALUES ({placeholders})
            ON CONFLICT ({pk}) DO UPDATE
            SET
        """
        ).format(
            table=self._table_identifier(dataset.table_name),
            cols=SQL(",").join([Identifier(k) for k in col_names]),
            pk=Identifier(pk_field),
            placeholders=SQL(",").join(self._placeholders_with_setsrid(dataset)),
        )
        upd_clause = []
        for k in col_names:
            upd_clause.append(SQL("{c}=EXCLUDED.{c}").format(c=Identifier(k)))
        sql_write_feature += SQL(", ").join(upd_clause)

        feat_count = 0
        CHUNK_SIZE = 10000
        for rows in self._chunk(
            dataset.get_feature_tuples(
                pk_iter, col_names, ignore_missing=ignore_missing
            ),
            CHUNK_SIZE,
        ):
            dbcur.executemany(sql_write_feature, (tuple(r) for r in rows))
            feat_count += changes_rowcount(dbcur)

        return feat_count

    def delete_features(self, dbcur, dataset, pk_iter):
        pk_field = dataset.primary_key

        sql_del_feature = SQL("DELETE FROM {} WHERE {}=%s;").format(
            self._table_identifier(dataset.table_name), Identifier(pk_field)
        )

        feat_count = 0
        CHUNK_SIZE = 10000
        for rows in self._chunk(zip(pk_iter), CHUNK_SIZE):
            dbcur.executemany(sql_del_feature, rows)
            feat_count += changes_rowcount(dbcur)

        return feat_count

    def _ds_meta_items(self, dataset):
        for key, value in dataset.meta_items():
            if key == "schema.json" or key.startswith("crs/"):
                yield key, value

    def _wc_meta_items(self, dataset):
        with self.session() as db:
            dbcur = db.cursor()
            table_info_sql = SQL(
                """
                SELECT
                    C.column_name, C.ordinal_position, C.data_type, C.udt_name,
                    C.character_maximum_length, C.numeric_precision, C.numeric_scale,
                    KCU.ordinal_position AS pk_ordinal_position,
                    GC.type AS geometry_type,
                    GC.srid AS geometry_srid
                FROM information_schema.columns C
                LEFT OUTER JOIN information_schema.key_column_usage KCU
                ON (KCU.table_schema = C.table_schema)
                AND (KCU.table_name = C.table_name)
                AND (KCU.column_name = C.column_name)
                LEFT OUTER JOIN public.geometry_columns GC
                ON (GC.f_table_schema = C.table_schema)
                AND (GC.f_table_name = C.table_name)
                AND (GC.f_geometry_column = C.column_name)
                WHERE C.table_schema=%s AND C.table_name=%s
                ORDER BY C.ordinal_position;
            """
            )
            dbcur.execute(table_info_sql, (self.schema, dataset.table_name))
            pg_table_info = list(dbcur)

            spatial_ref_sys_sql = SQL(
                """
                SELECT SRS.* FROM public.spatial_ref_sys SRS
                LEFT OUTER JOIN public.geometry_columns GC ON (GC.srid = SRS.srid)
                WHERE GC.f_table_schema=%s AND GC.f_table_name=%s;
            """
            )
            dbcur.execute(spatial_ref_sys_sql, (self.schema, dataset.table_name))
            pg_spatial_ref_sys = list(dbcur)

            id_salt = f"{self.schema} {dataset.table_name} {self.get_db_tree()}"

            schema = postgis_adapter.postgis_to_v2_schema(
                pg_table_info, pg_spatial_ref_sys, id_salt
            )

            yield "schema.json", schema.to_column_dicts()
            for crs_info in pg_spatial_ref_sys:
                wkt = crs_info["srtext"]
                yield f"crs/{crs_util.get_identifier_str(wkt)}.wkt", wkt

    def _db_geom_to_gpkg_geom(self, g):
        # This is already handled by register_type
        return g

    def _execute_diff_query(self, dbcur, dataset, feature_filter=None):
        feature_filter = feature_filter or UNFILTERED
        table = dataset.table_name
        pk_field = dataset.schema.pk_columns[0].name

        diff_sql = SQL(
            """
            SELECT
                {tracking_table}.pk AS ".__track_pk",
                {table}.*
            FROM {tracking_table} LEFT OUTER JOIN {table}
            ON ({tracking_table}.pk = {table}.{pk_field}::text)
            WHERE ({tracking_table}.table_name = %s)
            """
        ).format(
            tracking_table=self.TRACKING_TABLE,
            table=self._table_identifier(table),
            pk_field=Identifier(pk_field),
        )
        params = [table]

        if feature_filter is not UNFILTERED:
            diff_sql += SQL("\nAND {}.pk IN %s").format(self.TRACKING_TABLE)
            params.append(tuple([str(pk) for pk in feature_filter]))

        dbcur.execute(diff_sql, params)

    def _execute_dirty_rows_query(self, dbcur, dataset):
        sql_changed = SQL("SELECT pk FROM {} WHERE table_name=%s;").format(
            self.TRACKING_TABLE
        )
        dbcur.execute(sql_changed, (dataset.table_name,))

    def reset_tracking_table(self, reset_filter=UNFILTERED):
        reset_filter = reset_filter or UNFILTERED

        with self.session() as db:
            dbcur = db.cursor()
            if reset_filter == UNFILTERED:
                dbcur.execute(SQL("DELETE FROM {};").format(self.TRACKING_TABLE))
                return

            for dataset_path, dataset_filter in reset_filter.items():
                table = dataset_path.strip("/").replace("/", "__")
                if (
                    dataset_filter == UNFILTERED
                    or dataset_filter.get("feature") == UNFILTERED
                ):
                    dbcur.execute(
                        SQL("DELETE FROM {} WHERE table_name=%s;").format(
                            self.TRACKING_TABLE
                        ),
                        (table,),
                    )
                    continue

                CHUNK_SIZE = 100
                pks = dataset_filter.get("feature", ())
                for pk_chunk in self._chunk(pks, CHUNK_SIZE):
                    dbcur.execute(
                        SQL("DELETE FROM {} WHERE table_name=%s AND pk IN %s;").format(
                            self.TRACKING_TABLE
                        ),
                        (table, pk_chunk),
                    )

    def _reset_tracking_table_for_dataset(self, dbcur, dataset):
        dbcur.execute(
            SQL("DELETE FROM {} WHERE table_name=%s;").format(self.TRACKING_TABLE),
            (dataset.table_name,),
        )
        return changes_rowcount(dbcur)

    def _update_state_table_tree_impl(self, dbcur, tree_id):
        dbcur.execute(
            SQL("UPDATE {} SET value=%s WHERE table_name='*' AND key='tree';").format(
                self.STATE_TABLE
            ),
            (tree_id,),
        )
        return changes_rowcount(dbcur)
