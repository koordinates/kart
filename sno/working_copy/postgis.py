import contextlib
import logging
import re
import time
from urllib.parse import urlsplit, urlunsplit

import pygit2
import psycopg2
from psycopg2.extensions import register_adapter, Binary
from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier, SQL


from .base import WorkingCopy
from sno import geometry, crs_util
from sno.diff_structs import DeltaDiff
from sno.filter_util import UNFILTERED
from sno.geometry import gpkg_geom_to_ewkb

"""
* database needs to exist
* database needs to have postgis enabled
* database user needs to be able to:
    1. create 'sno' schema & tables
    2. create & alter tables in the default (or specified) schema
    3. create triggers
"""


def geometry_ewkb_adapter(g):
    return Binary(gpkg_geom_to_ewkb(g))


register_adapter(geometry.Geometry, geometry_ewkb_adapter)


class WorkingCopy_Postgis(WorkingCopy):
    def __init__(self, repo, uri):
        """
        uri: connection string of the form postgresql://[user[:password]@][netloc][:port][/dbname[/schema]][?param1=value1&...]
        """
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo
        self.uri = uri
        self.path = uri

        url = urlsplit(uri)

        if url.scheme != "postgresql":
            raise ValueError("Expecting postgresql://")

        url_path = url.path
        self.schema = None
        if url.path:
            path_parts = url.path[1:].split("/")
            if len(path_parts) > 2:
                raise ValueError("Expecting postgresql://[host]/dbname/schema")
            elif len(path_parts) == 2:
                self.schema = path_parts[1]
                url_path = f"/{path_parts[0]}"
            else:
                self.schema = self._default_schema(repo)

        # rebuild DB URL suitable for libpq
        self.dburl = urlunsplit([url.scheme, url.netloc, url_path, url.query, ""])

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

    def _default_schema(self, repo):
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

    _V2_TYPE_TO_PG_TYPE = {
        "boolean": "boolean",
        "blob": "bytea",
        "date": "date",
        "float": {0: "real", 32: "real", 64: "double precision"},
        "geometry": "geometry",
        "integer": {
            0: "integer",
            16: "smallint",
            32: "integer",
            64: "bigint",
        },
        "interval": "interval",
        "numeric": "numeric",
        "text": "text",
        "time": "time",
        "timestamp": "timestamp",
        # TODO - time and timestamp come in two flavours, with and without timezones.
        # Code for preserving these flavours in datasets and working copies needs more work.
    }

    def _v2_type_to_pg_type(self, dataset, column_schema):
        """Convert a v2 schema type to a postgis type."""

        v2_type = column_schema.data_type
        extra_type_info = column_schema.extra_type_info

        pg_type_info = self._V2_TYPE_TO_PG_TYPE.get(v2_type)
        if pg_type_info is None:
            raise ValueError(f"Unrecognised data type: {v2_type}")

        if isinstance(pg_type_info, dict):
            return pg_type_info.get(extra_type_info.get("size", 0))

        pg_type = pg_type_info
        if pg_type == "geometry":
            geometry_type = extra_type_info.get("geometryType", None)
            if geometry_type is not None:
                geometry_type = geometry_type.replace(" ", "")

            crs_name = extra_type_info.get("geometryCRS", None)
            crs_id = None
            if crs_name is not None:
                crs_id = crs_util.get_identifier_int_from_dataset(dataset, crs_name)

            if geometry_type is not None and crs_id is not None:
                return f"geometry({geometry_type},{crs_id})"
            elif geometry_type is not None:
                return f"geometry({geometry_type})"
            else:
                return "geometry"

        if pg_type == "text":
            length = extra_type_info.get("length", None)
            return f"varchar({length})" if length is not None else "text"

        if pg_type == "numeric":
            precision = extra_type_info.get("precision", None)
            scale = extra_type_info.get("scale", None)
            if precision is not None and scale is not None:
                return f"numeric({precision},{scale})"
            elif precision is not None:
                return f"numeric({precision})"
            else:
                return "numeric"

        return pg_type

    def _get_columns(self, dataset):
        pk_field = None
        pg_cols = {}
        for col in dataset.schema:
            pg_type = self._v2_type_to_pg_type(dataset, col)
            col_spec = [
                Identifier(col.name),
                SQL(pg_type),
            ]
            if col.pk_index is not None:
                col_spec.append(SQL("PRIMARY KEY NOT NULL"))
                pk_field = col.name
            pg_cols[col.name] = SQL(" ").join(col_spec)

        self.L.debug(
            "Schema -> Postgis column mapping: %s -> %s", dataset.schema, pg_cols
        )
        return pg_cols, pk_field

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
        pass

    def read_meta(self, dataset):
        return {}

    def save_config(self, **kwargs):
        self.repo.config["sno.workingcopy.version"] = 1
        self.repo.config["sno.workingcopy.path"] = self.uri

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

    def _create_triggers(self, dbcur, table, pk_field):
        # sqlite doesn't let you do param substitutions in CREATE TRIGGER
        dbcur.execute(
            SQL(
                """
            CREATE TRIGGER {} AFTER INSERT OR UPDATE OR DELETE ON {}
            FOR EACH ROW EXECUTE PROCEDURE {}(%s)
            """
            ).format(
                Identifier("sno_track"),
                self._table_identifier(table),
                Identifier(self.schema, "_sno_track_trigger"),
            ),
            (pk_field,),
        )

    @contextlib.contextmanager
    def _suspend_triggers(self, dbcur, table):
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
                SQL(
                    """
                SELECT value
                FROM {}
                WHERE table_name=%s AND key=%s
            """
                ).format(self.STATE_TABLE),
                (table_name, "tree"),
            )
            row = dbcur.fetchone()
            if not row:
                raise ValueError(f"No meta entry for {table_name}")

            wc_tree_id = row[0]
            return wc_tree_id

    def assert_db_tree_match(self, tree, *, table_name="*"):
        wc_tree_id = self.get_db_tree(table_name)
        tree_sha = tree.hex

        if wc_tree_id != tree_sha:
            raise self.Mismatch(wc_tree_id, tree_sha)
        return wc_tree_id

    def write_full(self, commit, *datasets, **kwargs):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        with self.session(bulk=2) as db:
            dbcur = db.cursor()
            for dataset in datasets:
                table = dataset.table_name

                self.write_meta(dataset)

                # Create the table
                cols, pk_field = self._get_columns(dataset)
                col_names = cols.keys()
                col_specs = cols.values()

                if self.schema:
                    dbcur.execute(
                        SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                            Identifier(self.schema)
                        )
                    )

                dbcur.execute(
                    SQL("CREATE TABLE IF NOT EXISTS {} ({});").format(
                        self._table_identifier(table), SQL(", ").join(col_specs)
                    )
                )

                L.info("Creating features...")

                # We have to call SetSRID on all geometries so that they will fit in their columns:
                # Unlike GPKG, the geometries in a column with a CRS of X can't all just have a CRS of 0.
                value_placeholders = [SQL("%s")] * len(col_names)
                for i, col in enumerate(dataset.schema):
                    if col.data_type != "geometry":
                        continue
                    crs_name = col.extra_type_info.get("geometryCRS", None)
                    if crs_name is None:
                        continue
                    crs_id = crs_util.get_identifier_int_from_dataset(dataset, crs_name)
                    value_placeholders[i] = SQL(f"SetSRID(%s, {crs_id})")

                sql_insert_features = SQL(
                    """
                    INSERT INTO {} ({}) VALUES ({});
                """
                ).format(
                    self._table_identifier(table),
                    SQL(",").join([Identifier(k) for k in col_names]),
                    SQL(",").join(value_placeholders),
                )

                crs_id = crs_util.get_identifier_int_from_dataset(dataset)

                feat_count = 0
                t0 = time.monotonic()
                t0p = t0

                CHUNK_SIZE = 10000
                for rows in self._chunk(dataset.feature_tuples(col_names), CHUNK_SIZE):

                    dbcur.executemany(sql_insert_features, rows)
                    feat_count += dbcur.rowcount

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
                self._create_triggers(dbcur, table, pk_field)

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
        cols, pk_field = self._get_columns(dataset)
        col_names = cols.keys()

        sql_write_feature = SQL(
            """
            INSERT INTO {table} ({cols}) VALUES %s
            ON CONFLICT ({pk}) DO UPDATE
            SET
        """
        ).format(
            table=self._table_identifier(dataset.table_name),
            cols=SQL(",").join([Identifier(k) for k in col_names]),
            pk=Identifier(pk_field),
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
            dbcur.executemany(sql_write_feature, ([tuple(r)] for r in rows))
            feat_count += dbcur.rowcount

        return feat_count

    def delete_features(self, dbcur, dataset, pk_iter):
        cols, pk_field = self._get_columns(dataset)

        sql_del_feature = SQL("DELETE FROM {} WHERE {}=%s;").format(
            self._table_identifier(dataset.table_name), Identifier(pk_field)
        )

        feat_count = 0
        CHUNK_SIZE = 10000
        for rows in self._chunk(zip(pk_iter), CHUNK_SIZE):
            dbcur.executemany(sql_del_feature, rows)
            feat_count += dbcur.rowcount

        return feat_count

    def diff_db_to_tree_meta(self, dataset, raise_if_dirty=False):
        # TODO: Implement meta diffs
        return DeltaDiff()

    def _db_geom_to_gpkg_geom(self, hex_ewkb_geom):
        return geometry.hexewkb_to_gpkg_geom(hex_ewkb_geom)

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
