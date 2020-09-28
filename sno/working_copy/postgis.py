import collections
import contextlib
import itertools
import logging
import os
import re
import time
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import click
import pygit2
import psycopg2
from psycopg2.extensions import register_adapter, Binary
from psycopg2.extras import DictCursor
from psycopg2.sql import Identifier, SQL

from .. import diff, geometry
from .base import WorkingCopy

"""
* database needs to exist
* database needs to have postgis enabled
* database user needs to be able to:
    1. create 'sno' schema & tables
    2. create & alter tables in the default (or specified) schema
    3. create triggers
"""


def gpkg_ewkb_adapter(gpkg_geom):
    return Binary(geometry.geom_to_ewkb(gpkg_geom))


register_adapter(geometry.Geometry, gpkg_ewkb_adapter)


class WorkingCopy_Postgis(WorkingCopy):
    def __init__(self, repo, uri):
        """
        uri: connection string of the form postgresql://[user[:password]@][netloc][:port][/dbname[/schema]][?param1=value1&...]
        """
        self.L = logging.getLogger(self.__class__.__qualname__)

        self.repo = repo
        self.uri = uri

        url = urlsplit(uri)

        if url.scheme != "postgresql":
            raise ValueError("Expecting postgresql://")

        u_path = url.path
        self.schema = None
        if url.path:
            path_parts = url.path[1:].split("/")
            if len(path_parts) > 2:
                raise ValueError("Expecting postgresql://[host]/dbname/schema")
            elif len(path_parts) == 2:
                self.schema = path_parts[1]
                u_path = f"/{path_parts[0]}"

        u_q = parse_qs(url.query)
        self.sno_schema = u_q.pop("sno.schema", ["sno"])[0]

        # rebuild DB URL suitable for libpq
        self.dburl = urlunsplit(
            [url.scheme, url.netloc, u_path, urlencode(u_q, doseq=True), ""]
        )

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

    def _table_identifier(self, table):
        if self.schema:
            return Identifier(self.schema, table)
        else:
            return Identifier(table)

    @contextlib.contextmanager
    def session(self, bulk=0):
        """
        Context manager for PostgreSQL DB sessions, yields a connection object inside a transaction

        Calling again yields the _same_ connection, the transaction/etc only happen in the outer one.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, "_db"):
            # inner - reuse
            L.debug(f"session: existing...")
            yield self._db
            L.debug(f"session: existing/done")
        else:
            L.debug(f"session: new...")
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
                L.debug(f"session: new/done")

    def _get_columns(self, dataset):
        pk_field = None
        cols = {}
        sqlite_cols = dataset.get_meta_item("sqlite_table_info")
        for col in sqlite_cols:
            gpkg_type = col["type"]
            if gpkg_type in (
                "POINT",
                "LINESTRING",
                "POLYGON",
                "MULTIPOINT",
                "MULTILINESTRING",
                "MULTIPOLYGON",
                "GEOMETRYCOLLECTION",
            ):
                srid = dataset.get_meta_item("gpkg_geometry_columns")["srs_id"]
                pg_type = f"GEOMETRY({gpkg_type}, {srid})"
            elif gpkg_type in ("GEOMETRY"):
                srid = dataset.get_meta_item("gpkg_geometry_columns")["srs_id"]
                pg_type = f"GEOMETRY(GEOMETRY, {srid})"
            elif gpkg_type in ("MEDIUMINT", "INT8"):
                pg_type = "integer"
            elif gpkg_type in ("TINYINT", "INT2"):
                pg_type = "smallint"
            elif gpkg_type in ("UNSIGNED BIG INT"):
                pg_type = "bigint"
            elif gpkg_type in ("FLOAT"):
                pg_type = "real"
            elif gpkg_type in ("DOUBLE"):
                pg_type = "double precision"
            elif gpkg_type in ("DATETIME"):
                pg_type = "timestamptz"
            elif re.match(
                r"((N?((ATIVE )|(VAR(YING )?))?CHAR(ACTER)?)|TEXT|CLOB)(\(\d+\))?$",
                gpkg_type,
            ):
                m = re.split(r"[\(\)]", gpkg_type)
                pg_type = f"VARCHAR({m[1]})" if len(m) == 3 else "TEXT"
            else:
                # FIXME
                pg_type = gpkg_type

            col_spec = [
                Identifier(col["name"]),
                SQL(pg_type),
            ]
            if col["pk"]:
                col_spec.append(SQL("PRIMARY KEY"))
                pk_field = col["name"]
            if col["notnull"]:
                col_spec.append(SQL("NOT NULL"))
            cols[col["name"]] = SQL(" ").join(col_spec)

        self.L.debug("Sqlite>Postgres column mapping: %s -> %s", sqlite_cols, cols)
        return cols, pk_field

    def delete(self):
        """ Delete the working copy tables and sno schema """
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(SQL("DROP TABLE {} CASCADE").format(self.META_TABLE))
            dbcur.execute(SQL("DROP TABLE {} CASCADE").format(self.TRACKING_TABLE))

            # TODO drop dataset tables

        # clear the config in the repo
        del self.repo.config["sno.workingcopy"]

    def create(self):
        with self.session() as db:
            dbcur = db.cursor()
            # Remove placeholder stuff GDAL creates
            dbcur.execute(
                SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
                    Identifier(self.sno_schema)
                )
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
                ).format(self.META_TABLE)
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
                    func=Identifier(self.sno_schema, "track_trigger"),
                    tracking_table=self.TRACKING_TABLE,
                )
            )

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
            Identifier(f"{dataset.name}_idx_{geom_col}"),
            self._table_identifier(dataset.name),
            Identifier(geom_col),
        )
        L.debug("Creating spatial index for %s.%s: %s", dataset.name, geom_col, sql)
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
                Identifier(f"sno_track"),
                self._table_identifier(table),
                Identifier(self.sno_schema, "track_trigger"),
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
                ).format(self.META_TABLE),
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

    def _chunk(self, iterable, size):
        it = iter(iterable)
        while True:
            chunk_it = itertools.islice(it, size)
            try:
                first_el = next(chunk_it)
            except StopIteration:
                return
            yield itertools.chain((first_el,), chunk_it)

    def write_full(self, commit, *datasets, **kwargs):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        with self.session(bulk=2) as db:
            dbcur = db.cursor()
            for dataset in datasets:
                table = dataset.name

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
                sql_insert_features = SQL(
                    """
                    INSERT INTO {} ({}) VALUES %s;
                """
                ).format(
                    self._table_identifier(table),
                    SQL(",").join([Identifier(k) for k in col_names]),
                )
                feat_count = 0
                t0 = time.monotonic()
                t0p = t0

                CHUNK_SIZE = 10000
                for rows in self._chunk(dataset.feature_tuples(col_names), CHUNK_SIZE):
                    dbcur.executemany(sql_insert_features, ([tuple(r)] for r in rows))
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
                ).format(self.META_TABLE),
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
            table=self._table_identifier(dataset.name),
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
            self._table_identifier(dataset.name), Identifier(pk_field)
        )

        feat_count = 0
        CHUNK_SIZE = 10000
        for rows in self._chunk(zip(pk_iter), CHUNK_SIZE):
            dbcur.executemany(sql_del_feature, rows)
            feat_count += dbcur.rowcount

        return feat_count

    def _db_to_repo_obj(self, dataset, db_obj):
        geom_col = dataset.geom_column_name
        if geom_col:
            db_obj[geom_col] = gpkg.hexewkb_to_geom(db_obj[geom_col])
        return db_obj

    def diff_db_to_tree(self, dataset, pk_filter=None):
        """
        Generates a diff between a working copy DB and the underlying repository tree

        Pass a list of PK values to filter results to them
        """
        with self.session() as db:
            dbcur = db.cursor()

            table = dataset.name

            meta_diff = {}
            meta_old = dict(dataset.iter_meta_items(exclude={"fields", "primary_key"}))
            meta_new = dict(self.read_meta(dataset))
            for name in set(meta_new.keys()) ^ set(meta_old.keys()):
                v_old = meta_old.get(name)
                v_new = meta_new.get(name)
                if v_old or v_new:
                    meta_diff[name] = (v_old, v_new)

            pk_field = dataset.primary_key

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
            if pk_filter:
                diff_sql += SQL("\nAND {}.pk IN %s").format(self.TRACKING_TABLE)
                params.append(tuple([str(pk) for pk in pk_filter]))
            dbcur.execute(diff_sql, params)

            candidates_ins = collections.defaultdict(list)
            candidates_upd = {}
            candidates_del = collections.defaultdict(list)
            for row in dbcur:
                track_pk = row[0]
                db_obj = {k: row[k] for k in row.keys() if k != ".__track_pk"}

                self._db_to_repo_obj(dataset, db_obj)

                try:
                    _, repo_obj = dataset.get_feature(track_pk, ogr_geoms=False)
                except KeyError:
                    repo_obj = None

                if db_obj[pk_field] is None:
                    if repo_obj:  # ignore INSERT+DELETE
                        blob_hash = pygit2.hash(dataset.encode_feature(repo_obj)).hex
                        candidates_del[blob_hash].append((track_pk, repo_obj))
                    continue

                elif not repo_obj:
                    # INSERT
                    blob_hash = pygit2.hash(dataset.encode_feature(db_obj)).hex
                    candidates_ins[blob_hash].append(db_obj)

                else:
                    # UPDATE
                    s_old = set(repo_obj.items())
                    s_new = set(db_obj.items())
                    if s_old ^ s_new:
                        candidates_upd[track_pk] = (repo_obj, db_obj)

            # detect renames
            for h in list(candidates_del.keys()):
                if h in candidates_ins:
                    track_pk, repo_obj = candidates_del[h].pop(0)
                    db_obj = candidates_ins[h].pop(0)

                    candidates_upd[track_pk] = (repo_obj, db_obj)

                    if not candidates_del[h]:
                        del candidates_del[h]
                    if not candidates_ins[h]:
                        del candidates_ins[h]

            return diff.Diff(
                dataset,
                meta=meta_diff,
                inserts=list(itertools.chain(*candidates_ins.values())),
                deletes=dict(itertools.chain(*candidates_del.values())),
                updates=candidates_upd,
            )

    def commit_callback(self, dataset, action, **kwargs):
        with self.session() as db:
            dbcur = db.cursor()

            if action in ("I", "U", "D", "META"):
                pass

            elif action == "INDEX":
                dbcur.execute(
                    SQL("DELETE FROM {} WHERE table_name=%s;").format(
                        self.TRACKING_TABLE
                    ),
                    (dataset.name,),
                )

            elif action == "TREE":
                new_tree = kwargs["tree"]
                print(f"Tree sha: {new_tree}")

                dbcur.execute(
                    SQL(
                        "UPDATE {} SET value=%s WHERE table_name='*' AND key='tree';"
                    ).format(self.META_TABLE),
                    (str(new_tree),),
                )
                assert (
                    dbcur.rowcount == 1
                ), f"{self.META_TABLE} update: expected 1Δ, got {dbcur.rowcount}"

            else:
                raise NotImplementedError(f"Unexpected action: {action}")

    def reset(
        self, commit, repo_structure, *, force=False, paths=None, update_meta=True
    ):
        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        L.debug("c=%s update-meta=%s", str(commit.id), update_meta)

        with self.session(bulk=1) as db:
            dbcur = db.cursor()

            base_tree_id = self.get_db_tree()
            base_tree = repo_structure.repo[base_tree_id]
            L.debug("base_tree_id: %s", base_tree_id)
            repo_tree_id = repo_structure.repo.head.peel(pygit2.Tree).hex

            if base_tree_id != repo_tree_id:
                L.debug(
                    "Working Copy DB is tree:%s, Repo HEAD has tree:%s",
                    base_tree_id,
                    repo_tree_id,
                )

            # check for dirty working copy
            dbcur.execute(SQL("SELECT COUNT(*) FROM {};").format(self.TRACKING_TABLE))
            is_dirty = dbcur.fetchone()[0]
            if is_dirty and not force:
                raise click.ClickException(
                    "You have uncommitted changes in your working copy. Commit or use --force to discard."
                )

            src_datasets = {ds.name: ds for ds in repo_structure.iter_at(base_tree)}
            dest_datasets = {ds.name: ds for ds in repo_structure.iter_at(commit.tree)}

            if paths:
                for path in paths:
                    src_datasets = {
                        ds.name: ds
                        for ds in src_datasets.values()
                        if os.path.commonpath([ds.path, path]) == path
                    }
                    dest_datasets = {
                        ds.name: ds
                        for ds in dest_datasets.values()
                        if os.path.commonpath([ds.path, path]) == path
                    }

            ds_names = set(src_datasets.keys()) | set(dest_datasets.keys())
            L.debug("Datasets: %s", ds_names)

            for table in ds_names:
                src_ds = src_datasets.get(table, None)
                dest_ds = dest_datasets.get(table, None)

                geom_col = dest_ds.geom_column_name

                if not dest_ds:
                    # drop table
                    raise NotImplementedError("Drop table via reset")
                elif not src_ds:
                    # new table
                    raise NotImplementedError("Create table via reset")
                elif src_ds.tree.id == dest_ds.tree.id and not is_dirty:
                    # unchanged table
                    pass
                else:
                    # existing table with update

                    # check for schema differences
                    base_meta_tree = src_ds.meta_tree
                    meta_tree = dest_ds.meta_tree
                    if base_meta_tree.diff_to_tree(meta_tree):
                        raise NotImplementedError(
                            "Sorry, no way to do changeset/meta/schema updates yet"
                        )

                    # todo: suspend/remove spatial index
                    if is_dirty:
                        with self._suspend_triggers(dbcur, table):
                            L.debug("Cleaning up dirty rows...")
                            sql_changed = SQL(
                                "SELECT pk FROM {} WHERE table_name=%s;"
                            ).format(self.TRACKING_TABLE)
                            dbcur.execute(sql_changed, (table,))
                            pk_list = [r[0] for r in dbcur]
                            track_count = dbcur.rowcount
                            count = self.delete_features(dbcur, src_ds, pk_list)
                            L.debug(
                                "reset(): dirty: removed %s features, tracking Δ count=%s",
                                count,
                                track_count,
                            )
                            count = self.write_features(
                                dbcur, src_ds, pk_list, ignore_missing=True
                            )
                            L.debug(
                                "reset(): dirty: wrote %s features, tracking Δ count=%s",
                                count,
                                track_count,
                            )

                            dbcur.execute(
                                SQL("DELETE FROM {} WHERE table_name=%s;").format(
                                    self.TRACKING_TABLE
                                ),
                                (table,),
                            )

                    if update_meta:
                        ctx = self._suspend_triggers(dbcur, table)
                    else:
                        # if we're not updating meta information, we want to track these changes
                        # as working copy edits so they can be committed.
                        ctx = contextlib.nullcontext()

                    with ctx:
                        # feature diff
                        diff_index = src_ds.tree.diff_to_tree(dest_ds.tree)
                        L.debug("Index diff: %s changes", len(diff_index))
                        for d in diff_index.deltas:
                            # TODO: improve this by grouping by status then calling
                            # write_features/delete_features passing multiple PKs?
                            if d.status == pygit2.GIT_DELTA_DELETED:
                                old_pk = src_ds.decode_pk(
                                    os.path.basename(d.old_file.path)
                                )
                                L.debug("reset(): D %s (%s)", d.old_file.path, old_pk)
                                self.delete_features(dbcur, src_ds, [old_pk])
                            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                                old_pk = src_ds.decode_pk(
                                    os.path.basename(d.old_file.path)
                                )
                                new_pk = dest_ds.decode_pk(
                                    os.path.basename(d.new_file.path)
                                )
                                L.debug(
                                    "reset(): M %s (%s) -> %s (%s)",
                                    d.old_file.path,
                                    old_pk,
                                    d.new_file.path,
                                    new_pk,
                                )
                                self.write_features(dbcur, dest_ds, [new_pk])
                            elif d.status == pygit2.GIT_DELTA_ADDED:
                                new_pk = dest_ds.decode_pk(
                                    os.path.basename(d.new_file.path)
                                )
                                L.debug("reset(): A %s (%s)", d.new_file.path, new_pk)
                                self.write_features(dbcur, dest_ds, [new_pk])
                            else:
                                # GIT_DELTA_RENAMED
                                # GIT_DELTA_COPIED
                                # GIT_DELTA_IGNORED
                                # GIT_DELTA_TYPECHANGE
                                # GIT_DELTA_UNMODIFIED
                                # GIT_DELTA_UNREADABLE
                                # GIT_DELTA_UNTRACKED
                                raise NotImplementedError(
                                    f"Delta status: {d.status_char()}"
                                )

            if update_meta:
                # update the tree id
                tree = commit.peel(pygit2.Tree)
                dbcur.execute(
                    SQL(
                        "UPDATE {} SET value=%s WHERE table_name='*' AND key='tree';"
                    ).format(self.META_TABLE),
                    (tree.hex,),
                )

    def status(self, dataset):
        diff = self.diff_db_to_tree(dataset)
        return diff.counts(dataset)
