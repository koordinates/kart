import contextlib
import itertools
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import click
import pygit2
from osgeo import gdal

from . import gpkg
from .structure import Dataset00, RepositoryStructure


@click.command('wc-new')
@click.pass_context
@click.argument("path", type=click.Path(writable=True, dir_okay=False))
@click.argument("datasets", nargs=-1)
def wc_new(ctx, path, datasets):
    # Temporary command to create a new working copy
    # Supports multiple datasets

    repo_dir = ctx.obj["repo_dir"]
    repo = pygit2.Repository(repo_dir)
    if not repo:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    repo_cfg = repo.config
    assert "kx.workingcopy" not in repo_cfg, "Existing v1 working copy"
    assert "snowdrop.workingcopy" not in repo_cfg, "Existing v2 working copy"

    rs = RepositoryStructure(repo)
    if not datasets:
        datasets = list(rs)
    else:
        datasets = [rs[ds_path] for ds_path in datasets]

    commit = repo.head.peel(pygit2.Commit)

    wc = WorkingCopy_GPKG_1(repo, path)
    wc.create()
    for dataset in datasets:
        wc.write_full(commit, dataset)
    wc.save_config(repo)


class WorkingCopy:
    @classmethod
    def open(cls, repo, dataset):
        repo_cfg = repo.config
        if "kx.workingcopy" in repo_cfg:
            fmt, path, layer = repo_cfg["kx.workingcopy"].split(":")
        else:
            return None

        if not os.path.isfile(path):
            raise FileNotFoundError(f"Working copy missing? {path}")

        if fmt != 'GPKG':
            raise NotImplementedError(f"Working copy format: {fmt}")

        if isinstance(dataset, Dataset00):
            return WorkingCopy_GPKG_0(repo, path)
        else:
            return WorkingCopy_GPKG_1(repo, path)

    @classmethod
    def new(cls, repo, dataset, fmt, path):
        if os.path.isfile(path):
            raise FileExistsError(path)

        if fmt != 'GPKG':
            raise NotImplementedError(f"Working copy format: {fmt}")

        if isinstance(dataset, Dataset00):
            return WorkingCopy_GPKG_0(repo, path)
        else:
            return WorkingCopy_GPKG_1(repo, path)


class WorkingCopyGPKG(WorkingCopy):
    META_PREFIX = ".sno-"

    def __init__(self, repo, path):
        self.repo = repo
        self.path = path

    @property
    def TRACKING_TABLE(self):
        return self._meta_name("track")

    @property
    def META_TABLE(self):
        return self._meta_name("meta")

    def _meta_name(self, name, suffix=""):
        n = f"{self.META_PREFIX}{name}"
        if suffix:
            n += "_" + suffix
        return gpkg.ident(n)

    @contextlib.contextmanager
    def session(self, bulk_load=False):
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, '_db'):
            # inner
            yield self._db
        else:
            self._db = gpkg.db(self.path, isolation_level=None)  # autocommit (also means manual transaction management)

            if bulk_load:
                L.debug("Invoking bulk-load mode")
                orig_journal = self._db.execute("PRAGMA journal_mode;").fetchone()[0]
                orig_locking = self._db.execute("PRAGMA locking_mode;").fetchone()[0]
                self._db.execute("PRAGMA synchronous = OFF;")
                self._db.execute("PRAGMA journal_mode = MEMORY;")
                self._db.execute("PRAGMA cache_size = -1048576;")  # -KiB => 1GiB
                self._db.execute("PRAGMA locking_mode = EXCLUSIVE;")

            self._db.execute("BEGIN")
            try:
                yield self._db
            except:  # noqa
                self._db.execute("ROLLBACK")
                raise
            else:
                self._db.execute("COMMIT")
            finally:
                if bulk_load:
                    L.debug("Disabling bulk-load mode (Journal: %s; Locking: %s)", orig_journal, orig_locking)
                    self._db.execute("PRAGMA synchronous = ON;")
                    self._db.execute(f"PRAGMA locking_mode = {orig_locking};")
                    self._db.execute("SELECT name FROM sqlite_master LIMIT 1;")  # unlock
                    self._db.execute(f"PRAGMA journal_mode = {orig_journal};")
                    self._db.execute("PRAGMA cache_size = -2000;")  # default

            del self._db

    def _get_columns(self, dataset):
        pk_field = None
        cols = {}
        for col in dataset.get_meta_item("sqlite_table_info"):
            col_spec = f"{gpkg.ident(col['name'])} {col['type']}"
            if col["pk"]:
                col_spec += " PRIMARY KEY"
                pk_field = col["name"]
            if col["notnull"]:
                col_spec += " NOT NULL"
            cols[col["name"]] = col_spec

        return cols, pk_field

    def create(self):
        # GDAL: Create GeoPackage
        # GDAL: Add metadata/etc
        gdal_driver = gdal.GetDriverByName("GPKG")
        gdal_ds = gdal_driver.Create(self.path, 0, 0, 0, gdal.GDT_Unknown)
        del gdal_ds

        with self.session() as db:
            # Remove placeholder stuff GDAL creates
            db.execute(
                "DELETE FROM gpkg_geometry_columns WHERE table_name='ogr_empty_table';"
            )
            db.execute("DELETE FROM gpkg_contents WHERE table_name='ogr_empty_table';")
            db.execute("DROP TABLE IF EXISTS ogr_empty_table;")

            # Create metadata tables
            db.execute("""
                CREATE TABLE IF NOT EXISTS gpkg_metadata (
                    id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC NOT NULL,
                    md_scope TEXT NOT NULL DEFAULT 'dataset',
                    md_standard_uri TEXT NOT NULL,
                    mime_type TEXT NOT NULL DEFAULT 'text/xml',
                    metadata TEXT NOT NULL DEFAULT ''
                );
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS gpkg_metadata_reference (
                    reference_scope TEXT NOT NULL,
                    table_name TEXT,
                    column_name TEXT,
                    row_id_value INTEGER,
                    timestamp DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
                    md_file_id INTEGER NOT NULL,
                    md_parent_id INTEGER,
                    CONSTRAINT crmr_mfi_fk FOREIGN KEY (md_file_id) REFERENCES gpkg_metadata(id),
                    CONSTRAINT crmr_mpi_fk FOREIGN KEY (md_parent_id) REFERENCES gpkg_metadata(id)
                );
            """)

            db.execute(f"""
                CREATE TABLE {self.META_TABLE} (
                    table_name TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NULL,
                    CONSTRAINT {self._meta_name('meta', 'pk')} PRIMARY KEY (table_name, key)
                );
            """)

    def write_meta(self, dataset):
        meta_info = dataset.get_meta_item("gpkg_contents")

        meta_geom = dataset.get_meta_item("gpkg_geometry_columns")
        meta_srs = dataset.get_meta_item("gpkg_spatial_ref_sys")

        meta_md = dataset.get_meta_item("gpkg_metadata") or {}
        meta_md_ref = dataset.get_meta_item("gpkg_metadata_reference") or {}

        with self.session() as db:
            # Update GeoPackage core tables
            for o in meta_srs:
                keys, values = zip(*o.items())
                sql = f"""
                    INSERT OR REPLACE INTO gpkg_spatial_ref_sys
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?'] * len(keys))});
                """
                db.execute(sql, values)

            keys, values = zip(*meta_info.items())
            # our repo copy doesn't include all fields from gpkg_contents
            # but the default value for last_change (now), and NULL for {min_x,max_x,min_y,max_y}
            # should deal with the remaining fields
            sql = f"""
                INSERT INTO gpkg_contents
                    ({','.join([gpkg.ident(k) for k in keys])})
                VALUES
                    ({','.join(['?'] * len(keys))});
            """
            db.execute(sql, values)

            if meta_geom:
                keys, values = zip(*meta_geom.items())
                sql = f"""
                    INSERT INTO gpkg_geometry_columns
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?']*len(keys))});
                """
                db.execute(sql, values)

            # Populate metadata tables
            # since there's FKs, need to remap joins
            dbcur = db.cursor()
            metadata_id_map = {}
            for o in meta_md:
                params = dict(o.items())
                params.pop('id')

                keys, values = zip(*params.items())
                sql = f"""
                    INSERT INTO gpkg_metadata
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?']*len(keys))});
                    """
                dbcur.execute(sql, values)
                metadata_id_map[o['id']] = dbcur.lastrowid

            for o in meta_md_ref:
                params = dict(o.items())
                params['md_file_id'] = metadata_id_map[o['md_file_id']]
                params['md_parent_id'] = metadata_id_map.get(o['md_parent_id'], None)

                keys, values = zip(*params.items())
                sql = f"""
                    INSERT INTO gpkg_metadata_reference
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?']*len(keys))});
                """
                dbcur.execute(sql, values)

    def read_meta(self, dataset):
        pass

    def save_config(self, repo):
        new_path = Path(self.path)
        if not new_path.is_absolute():
            new_path = os.path.relpath(new_path, Path(repo.path).resolve())

        repo.config["snow.workingcopy"] = f"GPKG:{new_path}"

    def write_full(self, commit, dataset):
        raise NotImplementedError()

    def _create_spatial_index(self, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        geom_col = dataset.geom_column_name

        # Create the GeoPackage Spatial Index
        t0 = time.time()
        gdal_ds = gdal.OpenEx(
            self.path,
            gdal.OF_VECTOR | gdal.OF_UPDATE | gdal.OF_VERBOSE_ERROR,
            ["GPKG"],
        )
        gdal_ds.ExecuteSQL(
            f"SELECT CreateSpatialIndex({gpkg.ident(dataset.name)}, {gpkg.ident(geom_col)});"
        )
        del gdal_ds
        L.info("Created spatial index in %ss", time.time()-t0)

    def update_gpkg_contents(self, commit, dataset):
        table = dataset.name
        commit_time = datetime.utcfromtimestamp(commit.commit_time)

        with self.session() as db:
            dbcur = db.cursor()

            if dataset.has_geometry:
                geom_col = dataset.geom_column_name
                sql = f"""
                    UPDATE gpkg_contents
                    SET
                        min_x=(SELECT ST_MinX({gpkg.ident(geom_col)}) FROM {gpkg.ident(table)}),
                        min_y=(SELECT ST_MinY({gpkg.ident(geom_col)}) FROM {gpkg.ident(table)}),
                        max_x=(SELECT ST_MaxX({gpkg.ident(geom_col)}) FROM {gpkg.ident(table)}),
                        max_y=(SELECT ST_MaxY({gpkg.ident(geom_col)}) FROM {gpkg.ident(table)}),
                        last_change=?
                    WHERE
                        table_name=?;
                """
            else:
                sql = f"""
                    UPDATE gpkg_contents
                    SET
                        min_x=NULL,
                        min_y=NULL,
                        max_x=NULL,
                        max_y=NULL,
                        last_change=?
                    WHERE
                        table_name=?;
                """

            dbcur.execute(sql, (
                commit_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # GPKG Spec Req.15
                table
            ))

            assert (
                dbcur.rowcount == 1
            ), f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"


class WorkingCopy_GPKG_0(WorkingCopyGPKG):
    """
    GeoPackage Working Copy for v0.0 repositories

    Key difference here is a separate UUID feature-key from the PK.
    """
    META_PREFIX = "__kxg_"

    @property
    def TRACKING_TABLE(self):
        return self._meta_name("map")

    def _gen_tree_id(self, commit, dataset):
        tree = commit.peel(pygit2.Tree)

        # Check-only
        layer_tree = (tree / dataset.path).obj
        assert layer_tree == dataset.tree, \
            f"Tree mismatch between dataset ({dataset.tree.hex}) and commit ({layer_tree.hex})"

        return tree.hex

    def create(self):
        super().create()

        with self.session() as db:
            db.execute(f"""
                CREATE TABLE {self.TRACKING_TABLE} (
                    table_name TEXT NOT NULL,
                    feature_key VARCHAR(36) NULL,
                    feature_id INTEGER NOT NULL,
                    state INTEGER NOT NULL DEFAULT 0,
                    CONSTRAINT {self._meta_name('map', 'u')} UNIQUE (table_name, feature_key)
                );
            """)

    def _create_triggers(self, dbcur, table):
        pk = gpkg.pk(dbcur, table)

        # sqlite doesn't let you do param substitutions in CREATE TRIGGER
        dbcur.execute(
            f"""
            CREATE TRIGGER {self._meta_name(table, "ins")}
               AFTER INSERT
               ON {gpkg.ident(table)}
            BEGIN
                INSERT INTO {self.TRACKING_TABLE} (table_name, feature_key, feature_id, state)
                    VALUES ({gpkg.param_str(table)}, NULL, NEW.{gpkg.ident(pk)}, 1);
            END;
        """
        )
        dbcur.execute(
            f"""
            CREATE TRIGGER {self._meta_name(table, "upd")}
               AFTER UPDATE
               ON {gpkg.ident(table)}
            BEGIN
                UPDATE {self.TRACKING_TABLE}
                    SET state=1, feature_id=NEW.{gpkg.ident(pk)}
                    WHERE table_name={gpkg.param_str(table)}
                        AND feature_id=OLD.{gpkg.ident(pk)}
                        AND state >= 0;
            END;
        """
        )
        dbcur.execute(
            f"""
            CREATE TRIGGER {self._meta_name(table, "del")}
               AFTER DELETE
               ON {gpkg.ident(table)}
            BEGIN
                UPDATE {self.TRACKING_TABLE}
                SET state=-1
                WHERE table_name={gpkg.param_str(table)}
                    AND feature_id=OLD.{gpkg.ident(pk)};
            END;
        """
        )

    def write_full(self, commit, dataset):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        table = dataset.name

        with self.session(bulk_load=True) as db:
            dbcur = db.cursor()

            self.write_meta(dataset)

            tree_id = self._gen_tree_id(commit, dataset)
            dbcur.execute(
                f"INSERT INTO {self.META_TABLE} (table_name, key, value) VALUES (?, ?, ?);",
                (table, "tree", tree_id),
            )

            # Create the table
            cols, pk_field = self._get_columns(dataset)
            col_names = cols.keys()
            col_specs = cols.values()
            db.execute(f"""
                CREATE TABLE {gpkg.ident(table)}
                ({', '.join(col_specs)});
            """)

            L.info("Creating features...")
            sql_insert_features = f"""
                INSERT INTO {gpkg.ident(table)}
                    ({','.join([gpkg.ident(k) for k in col_names])})
                VALUES
                    ({','.join(['?']*len(col_names))});
            """
            sql_insert_ids = f"""
                INSERT INTO {self.TRACKING_TABLE}
                    (table_name, feature_key, feature_id, state)
                VALUES
                    (?,?,?,0);
            """
            feat_count = 0
            t0 = time.time()
            t0p = t0

            wip_features = []
            wip_idmap = []
            for key, feature in dataset.features():
                wip_features.append([feature[c] for c in col_names])
                wip_idmap.append([table, key, feature[pk_field]])
                feat_count += 1

                if len(wip_features) == 1000:
                    dbcur.executemany(sql_insert_features, wip_features)
                    dbcur.executemany(sql_insert_ids, wip_idmap)
                    t0a = time.time()
                    L.info("%s features... @%.1fs (+%.1fs)", feat_count, (t0a-t0), (t0a-t0p))
                    wip_features = []
                    wip_idmap = []
                    t0p = t0a

            if len(wip_features):
                dbcur.executemany(sql_insert_features, wip_features)
                dbcur.executemany(sql_insert_ids, wip_idmap)
                t0a = time.time()
                L.info("%s features... @%.1fs (+%.1fs)", feat_count, (t0a-t0), (t0a-t0p))
                del wip_features
                del wip_idmap

            t1 = time.time()
            L.info("Added %d features to GPKG in %.1fs", feat_count, t1-t0)
            L.info("Overall rate: %d features/s", (feat_count / (t1 - t0)))

        if dataset.has_geometry:
            self._create_spatial_index(dataset)

        with self.session() as db:
            self.update_gpkg_contents(commit, dataset)

            # Create triggers
            self._create_triggers(db, table)


class WorkingCopy_GPKG_1(WorkingCopyGPKG):
    """
    GeoPackage Working Copy for v0.1/v0.2 repositories
    """
    def create(self):
        super().create()

        with self.session() as db:
            db.execute(f"""
                CREATE TABLE {self.TRACKING_TABLE} (
                    table_name TEXT NOT NULL,
                    pk TEXT NULL,
                    CONSTRAINT {self._meta_name('track', 'pk')} PRIMARY KEY (table_name, pk)
                );
            """)

    def _create_triggers(self, dbcur, table):
        pkf = gpkg.ident(gpkg.pk(dbcur, table))
        ts = gpkg.param_str(table)

        # sqlite doesn't let you do param substitutions in CREATE TRIGGER
        dbcur.execute(
            f"""
            CREATE TRIGGER {self._meta_name(table, 'ins')}
               AFTER INSERT
               ON {gpkg.ident(table)}
            BEGIN
                INSERT OR REPLACE INTO {self.TRACKING_TABLE}
                    (table_name, pk)
                VALUES (tr, NEW.{pkf});
            END;
        """
        )
        dbcur.execute(
            f"""
            CREATE TRIGGER {self._meta_name(table, 'upd')}
               AFTER UPDATE
               ON {gpkg.ident(table)}
            BEGIN
                INSERT OR REPLACE INTO {self.TRACKING_TABLE}
                    (table_name, pk)
                VALUES
                    ({ts}, NEW.{pkf}),
                    ({ts}, OLD.{pkf});
            END;
        """
        )
        dbcur.execute(
            f"""
            CREATE TRIGGER {self._meta_name(table, 'del')}
               AFTER DELETE
               ON {gpkg.ident(table)}
            BEGIN
                INSERT OR REPLACE INTO {self.TRACKING_TABLE}
                    (table_name, pk)
                VALUES
                    ({ts}, OLD.{pkf});
            END;
        """
        )

    def _chunk(self, iterable, size):
        it = iter(iterable)
        while True:
            chunk_it = itertools.islice(it, size)
            try:
                first_el = next(chunk_it)
            except StopIteration:
                return
            yield itertools.chain((first_el,), chunk_it)

    def write_full(self, commit, dataset):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        table = dataset.name

        with self.session(bulk_load=True) as db:
            dbcur = db.cursor()

            self.write_meta(dataset)

            dbcur.execute(
                f"INSERT INTO {self.META_TABLE} (table_name, key, value) VALUES (?, ?, ?);",
                (table, "tree", dataset.tree.hex),
            )

            # Create the table
            cols, pk_field = self._get_columns(dataset)
            col_names = cols.keys()
            col_specs = cols.values()
            db.execute(f"""
                CREATE TABLE {gpkg.ident(table)}
                ({', '.join(col_specs)});
            """)

            L.info("Creating features...")
            sql_insert_features = f"""
                INSERT INTO {gpkg.ident(table)}
                    ({','.join([gpkg.ident(k) for k in col_names])})
                VALUES
                    ({','.join(['?'] * len(col_names))});
            """
            feat_count = 0
            t0 = time.time()
            t0p = t0

            CHUNK_SIZE = 10000
            for rows in self._chunk(dataset.feature_tuples(col_names), CHUNK_SIZE):
                dbcur.executemany(sql_insert_features, rows)
                feat_count += dbcur.rowcount

                nc = feat_count / CHUNK_SIZE
                if nc % 5 == 0 or not nc.is_integer():
                    t0a = time.time()
                    L.info("%s features... @%.1fs (+%.1fs, ~%d F/s)", feat_count, t0a-t0, t0a-t0p, (CHUNK_SIZE*5)/(t0a-t0p))
                    t0p = t0a

            t1 = time.time()
            L.info("Added %d features to GPKG in %.1fs", feat_count, t1-t0)
            L.info("Overall rate: %d features/s", (feat_count / (t1 - t0)))

        if dataset.has_geometry:
            self._create_spatial_index(dataset)

        with self.session() as db:
            self.update_gpkg_contents(commit, dataset)

            # Create triggers
            self._create_triggers(db, table)
