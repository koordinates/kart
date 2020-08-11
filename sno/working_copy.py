import collections
import contextlib
import itertools
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import pygit2
from osgeo import gdal

from . import gpkg
from .diff_structs import RepoDiff, DatasetDiff, DeltaDiff, Delta
from .exceptions import InvalidOperation
from .filter_util import UNFILTERED
from .schema import Schema
from .structure import RepositoryStructure
from .structure_version import get_structure_version

L = logging.getLogger("sno.working_copy")


class WorkingCopyDirty(Exception):
    pass


class WorkingCopy:
    VALID_VERSIONS = (1, 2)

    @classmethod
    def get(cls, repo, create_if_missing=False):
        if create_if_missing:
            cls.ensure_config_exists(repo)

        repo_cfg = repo.config
        if "sno.workingcopy.path" not in repo_cfg:
            return None

        path = repo_cfg["sno.workingcopy.path"]
        if not (Path(repo.path) / path).is_file() and not create_if_missing:
            return None

        version = repo_cfg.get_int("sno.workingcopy.version")
        if version not in cls.VALID_VERSIONS:
            raise NotImplementedError(f"Working copy version: {version}")
        if version < 2:
            return WorkingCopy_GPKG_1(repo, path)
        else:
            return WorkingCopy_GPKG_2(repo, path)

    @classmethod
    def ensure_config_exists(cls, repo):
        repo_cfg = repo.config
        if "sno.workingcopy.bare" in repo_cfg and repo_cfg.get_bool(
            "sno.workingcopy.bare"
        ):
            return
        path = (
            repo_cfg["sno.workingcopy.path"]
            if "sno.workingcopy.path" in repo_cfg
            else None
        )
        version = (
            repo_cfg["sno.workingcopy.version"]
            if "sno.workingcopy.version" in repo_cfg
            else None
        )
        if path is None or version is None:
            cls.write_config(repo, path, version)

    @classmethod
    def write_config(cls, repo, path=None, version=None, bare=False):
        repo_cfg = repo.config

        def del_repo_cfg(key):
            if key in repo_cfg:
                del repo_cfg[key]

        if bare:
            repo_cfg["sno.workingcopy.bare"] = True
            del_repo_cfg("sno.workingcopy.path")
            del_repo_cfg("sno.workingcopy.version")
            return

        path = path or f"{Path(repo.path).resolve().stem}.gpkg"
        version = version or get_structure_version(repo)
        repo_cfg["sno.workingcopy.path"] = str(path)
        repo_cfg["sno.workingcopy.version"] = version
        del_repo_cfg("sno.workingcopy.bare")

    class Mismatch(ValueError):
        def __init__(self, working_copy_tree_id, match_tree_id):
            self.working_copy_tree_id = working_copy_tree_id
            self.match_tree_id = match_tree_id

        def __str__(self):
            return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"


class WorkingCopyGPKG(WorkingCopy):
    def __init__(self, repo, path):
        self.repo = repo
        self.path = path

    @property
    def full_path(self):
        """ Return a full absolute path to the working copy """
        return (Path(self.repo.path) / self.path).resolve()

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
    def session(self, bulk=0):
        """
        Context manager for GeoPackage DB sessions, yields a connection object inside a transaction

        Calling again yields the _same_ connection, the transaction/etc only happen in the outer one.

        @bulk controls bulk-loading operating mode:
            0: default, no bulk operations (normal)
            1: synchronous, larger cache (bulk changes)
            2: exclusive locking, memory journal (bulk load)
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, "_db"):
            # inner - reuse
            L.debug(f"session(bulk={bulk}): existing...")
            with self._db:
                yield self._db
            L.debug(f"session(bulk={bulk}): existing/done")
        else:
            L.debug(f"session(bulk={bulk}): new...")
            self._db = gpkg.db(self.full_path,)
            dbcur = self._db.cursor()

            if bulk:
                L.debug("Invoking bulk mode %s", bulk)
                orig_journal = dbcur.execute("PRAGMA journal_mode;").fetchone()[0]
                orig_locking = dbcur.execute("PRAGMA locking_mode;").fetchone()[0]

                dbcur.execute("PRAGMA synchronous = OFF;")
                dbcur.execute("PRAGMA cache_size = -1048576;")  # -KiB => 1GiB

                if bulk >= 2:
                    dbcur.execute("PRAGMA journal_mode = MEMORY;")
                    dbcur.execute("PRAGMA locking_mode = EXCLUSIVE;")

            try:
                with self._db:
                    yield self._db
            except Exception:
                raise
            finally:
                if bulk:
                    L.debug(
                        "Disabling bulk %s mode (Journal: %s; Locking: %s)",
                        bulk,
                        orig_journal,
                        orig_locking,
                    )
                    dbcur.execute("PRAGMA synchronous = ON;")
                    dbcur.execute("PRAGMA cache_size = -2000;")  # default

                    if bulk >= 2:
                        dbcur.execute(f"PRAGMA locking_mode = {orig_locking};")
                        dbcur.execute(
                            "SELECT name FROM sqlite_master LIMIT 1;"
                        )  # unlock
                        dbcur.execute(f"PRAGMA journal_mode = {orig_journal};")

                del dbcur
                self._db.close()
                del self._db
                L.debug(f"session(bulk={bulk}): new/done")

    def is_dirty(self):
        """
        Returns True if there are uncommitted changes in the working copy,
        or False otherwise.
        """
        try:
            self.diff_to_tree(raise_if_dirty=True)
            return False
        except WorkingCopyDirty:
            return True

    def check_not_dirty(self, help_message=None):
        """Checks the working copy has no changes in it. Otherwise, raises InvalidOperation"""
        if not help_message:
            help_message = "Commit these changes (`sno commit`) or discard these changes (`sno reset`) first."
        if self.is_dirty():
            raise InvalidOperation(
                f"You have uncommitted changes in your working copy.\n{help_message}"
            )

    def _get_columns(self, dataset):
        pk_field = None
        cols = {}
        for col in dataset.get_meta_item("sqlite_table_info"):
            col_spec = f"{gpkg.ident(col['name'])} {col['type']}"
            if col["pk"]:
                col_spec += " PRIMARY KEY"
                pk_field = col["name"]
                # TODO: Should INTEGER PRIMARY KEYs ever be non-AUTOINCREMENT?
                # See https://github.com/koordinates/sno/issues/188
                if col['type'] == "INTEGER":
                    col_spec += " AUTOINCREMENT"
            if col["notnull"]:
                col_spec += " NOT NULL"
            cols[col["name"]] = col_spec

        return cols, pk_field

    def delete(self):
        """ Delete the working copy files """
        self.full_path.unlink()

        # for sqlite this might include wal/journal/etc files
        # app.gpkg -> app.gpkg-wal, app.gpkg-journal
        # https://www.sqlite.org/shortnames.html
        for f in Path(self.full_path).parent.glob(Path(self.path).name + "-*"):
            f.unlink()

    def is_created(self):
        return self.full_path.is_file()

    def create(self):
        # GDAL: Create GeoPackage
        # GDAL: Add metadata/etc
        gdal_driver = gdal.GetDriverByName("GPKG")
        gdal_ds = gdal_driver.Create(str(self.full_path), 0, 0, 0, gdal.GDT_Unknown)
        del gdal_ds

        with self.session() as db:
            dbcur = db.cursor()
            # Remove placeholder stuff GDAL creates
            dbcur.execute(
                "DELETE FROM gpkg_geometry_columns WHERE table_name='ogr_empty_table';"
            )
            dbcur.execute(
                "DELETE FROM gpkg_contents WHERE table_name='ogr_empty_table';"
            )
            dbcur.execute("DROP TABLE IF EXISTS ogr_empty_table;")

            # Create metadata tables
            dbcur.execute(
                """
                CREATE TABLE IF NOT EXISTS gpkg_metadata (
                    id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC NOT NULL,
                    md_scope TEXT NOT NULL DEFAULT 'dataset',
                    md_standard_uri TEXT NOT NULL,
                    mime_type TEXT NOT NULL DEFAULT 'text/xml',
                    metadata TEXT NOT NULL DEFAULT ''
                );
            """
            )
            dbcur.execute(
                """
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
            """
            )

            dbcur.execute(
                f"""
                CREATE TABLE {self.META_TABLE} (
                    table_name TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT NULL,
                    CONSTRAINT {self._meta_name('meta', 'pk')} PRIMARY KEY (table_name, key)
                );
            """
            )

            dbcur.execute(
                f"""
                CREATE TABLE {self.TRACKING_TABLE} (
                    table_name TEXT NOT NULL,
                    pk TEXT NULL,
                    CONSTRAINT {self._meta_name('track', 'pk')} PRIMARY KEY (table_name, pk)
                );
            """
            )

    def write_meta(self, dataset):
        meta_info = dataset.get_meta_item("gpkg_contents")
        meta_info["table_name"] = dataset.name

        # FIXME: find a better way to roundtrip identifiers
        identifier_prefix = f"{dataset.name}: "
        if not meta_info["identifier"].startswith(identifier_prefix):
            meta_info["identifier"] = identifier_prefix + meta_info['identifier']

        meta_geom = dataset.get_meta_item("gpkg_geometry_columns")
        meta_srs = dataset.get_meta_item("gpkg_spatial_ref_sys")

        meta_md = dataset.get_meta_item("gpkg_metadata") or {}
        meta_md_ref = dataset.get_meta_item("gpkg_metadata_reference") or {}

        with self.session() as db:
            dbcur = db.cursor()
            # Update GeoPackage core tables
            for o in meta_srs:
                keys, values = zip(*o.items())
                sql = f"""
                    INSERT OR REPLACE INTO gpkg_spatial_ref_sys
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?'] * len(keys))});
                """
                dbcur.execute(sql, values)

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
            dbcur.execute(sql, values)

            if meta_geom:
                keys, values = zip(*meta_geom.items())
                sql = f"""
                    INSERT INTO gpkg_geometry_columns
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?']*len(keys))});
                """
                dbcur.execute(sql, values)

            # Populate metadata tables
            # since there's FKs, need to remap joins
            dbcur = db.cursor()
            metadata_id_map = {}
            for o in meta_md:
                params = dict(o.items())
                params.pop("id")

                keys, values = zip(*params.items())
                sql = f"""
                    INSERT INTO gpkg_metadata
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?']*len(keys))});
                    """
                dbcur.execute(sql, values)
                metadata_id_map[o["id"]] = db.last_insert_rowid()

            for o in meta_md_ref:
                params = dict(o.items())
                params["md_file_id"] = metadata_id_map[o["md_file_id"]]
                params["md_parent_id"] = metadata_id_map.get(o["md_parent_id"], None)
                params["table_name"] = dataset.name

                keys, values = zip(*params.items())
                sql = f"""
                    INSERT INTO gpkg_metadata_reference
                        ({','.join([gpkg.ident(k) for k in keys])})
                    VALUES
                        ({','.join(['?']*len(keys))});
                """
                dbcur.execute(sql, values)

    def iter_meta_items(self, dataset):
        """
        Extract all the metadata of this GPKG and convert to dataset V2 format.
        Note that the extracted schema will not be aligned to any existing schema
        - the generated column IDs are stable, but do not necessarily match the ones in the dataset.
        Calling Schema.align_* is required to find how the columns matches the existing schema.
        """
        with self.session() as db:
            gpkg_meta_items = dict(gpkg.get_meta_info(db, dataset.name))

        class GpkgTableAsV1Dataset:
            def __init__(self, name, gpkg_meta_items):
                self.name = name
                self.gpkg_meta_items = gpkg_meta_items

            def get_meta_item(self, path):
                return gpkg_meta_items[path]

        from . import gpkg_adapter

        gpkg_name = os.path.basename(self.path)
        yield from gpkg_adapter.iter_v2_meta_items(
            GpkgTableAsV1Dataset(dataset.name, gpkg_meta_items),
            id_salt=f"{gpkg_name}/{dataset.name}",
        )

    def delete_meta(self, dataset):
        with self.session() as db:
            dbcur = db.cursor()

            # FOREIGN KEY constraints are still active, so we delete in a particular order:
            for table in (
                "gpkg_metadata_reference",
                "gpkg_geometry_columns",
                "gpkg_contents",
            ):
                dbcur.execute(
                    f"""DELETE FROM {table} WHERE table_name = ?;""", (dataset.name,)
                )

            gpkg_metadata = dataset.get_meta_item("gpkg_metadata") or {}
            for row in gpkg_metadata:
                params = dict(row.items())
                params.pop("id")

                keys, values = zip(*params.items())
                sql = f"""
                    DELETE FROM gpkg_metadata WHERE
                        ({','.join([gpkg.ident(k) for k in keys])})
                    =
                        ({','.join(['?']*len(keys))});
                    """
                dbcur.execute(sql, values)

    def _create_spatial_index(self, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        geom_col = dataset.geom_column_name

        # Create the GeoPackage Spatial Index
        t0 = time.monotonic()
        gdal_ds = gdal.OpenEx(
            str(self.full_path),
            gdal.OF_VECTOR | gdal.OF_UPDATE | gdal.OF_VERBOSE_ERROR,
            ["GPKG"],
        )
        sql = f"SELECT CreateSpatialIndex({gpkg.param_str(dataset.name)}, {gpkg.param_str(geom_col)});"
        L.debug("Creating spatial index for %s.%s: %s", dataset.name, geom_col, sql)
        gdal_ds.ExecuteSQL(sql)
        del gdal_ds
        L.info("Created spatial index in %ss", time.monotonic() - t0)

    def _drop_spatial_index(self, dataset):
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        geom_col = dataset.geom_column_name

        # Delete the GeoPackage Spatial Index
        t0 = time.monotonic()
        gdal_ds = gdal.OpenEx(
            str(self.full_path),
            gdal.OF_VECTOR | gdal.OF_UPDATE | gdal.OF_VERBOSE_ERROR,
            ["GPKG"],
        )
        sql = f"SELECT DisableSpatialIndex({gpkg.param_str(dataset.name)}, {gpkg.param_str(geom_col)});"
        L.debug("Dropping spatial index for %s.%s: %s", dataset.name, geom_col, sql)
        gdal_ds.ExecuteSQL(sql)
        del gdal_ds
        L.info("Dropped spatial index in %ss", time.monotonic() - t0)

    def _drop_triggers(self, dbcur, table):
        dbcur.execute(f"DROP TRIGGER {self._meta_name(table, 'ins')}")
        dbcur.execute(f"DROP TRIGGER {self._meta_name(table, 'upd')}")
        dbcur.execute(f"DROP TRIGGER {self._meta_name(table, 'del')}")

    @contextlib.contextmanager
    def _suspend_triggers(self, dbcur, table):
        self._drop_triggers(dbcur, table)
        try:
            yield
        finally:
            self._create_triggers(dbcur, table)

    def update_gpkg_contents(self, dataset, change_time):
        table = dataset.name

        with self.session() as db:
            dbcur = db.cursor()

            if dataset.has_geometry:
                geom_col = dataset.geom_column_name
                sql = f"""
                    UPDATE gpkg_contents
                    SET
                        min_x=(SELECT ST_MinX(Extent({gpkg.ident(geom_col)})) FROM {gpkg.ident(table)}),
                        min_y=(SELECT ST_MinY(Extent({gpkg.ident(geom_col)})) FROM {gpkg.ident(table)}),
                        max_x=(SELECT ST_MaxX(Extent({gpkg.ident(geom_col)})) FROM {gpkg.ident(table)}),
                        max_y=(SELECT ST_MaxY(Extent({gpkg.ident(geom_col)})) FROM {gpkg.ident(table)}),
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

            dbcur.execute(
                sql,
                (
                    change_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # GPKG Spec Req.15
                    table,
                ),
            )

            rowcount = db.changes()
            assert rowcount == 1, f"gpkg_contents update: expected 1Δ, got {rowcount}"

    def get_db_tree(self, table_name="*"):
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(
                f"""
                    SELECT value
                    FROM {self.META_TABLE}
                    WHERE table_name=? AND key=?;
                """,
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

    def _create_triggers(self, dbcur, table):
        pkf = gpkg.ident(gpkg.pk(dbcur.getconnection(), table))
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
                VALUES ({ts}, NEW.{pkf});
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
        """
        Generator. Yield successive chunks from iterable of length <size>.
        """
        it = iter(iterable)
        while True:
            chunk = tuple(itertools.islice(it, size))
            if not chunk:
                return
            yield chunk

    def write_full(self, target_tree_or_commit, *datasets, safe=True):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        commit = (
            target_tree_or_commit
            if isinstance(target_tree_or_commit, pygit2.Commit)
            else None
        )
        if commit:
            change_time = datetime.utcfromtimestamp(commit.commit_time)
        else:
            change_time = datetime.utcnow()

        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")
        with self.session(bulk=(0 if safe else 2)) as db:
            for dataset in datasets:
                table = dataset.name

                dbcur = db.cursor()
                self.write_meta(dataset)

                # Create the table
                cols, pk_field = self._get_columns(dataset)
                col_names = cols.keys()
                col_specs = cols.values()
                dbcur.execute(
                    f"""
                    CREATE TABLE {gpkg.ident(table)}
                    ({', '.join(col_specs)});
                """
                )

                L.info("Creating features...")
                sql_insert_features = f"""
                    INSERT INTO {gpkg.ident(table)}
                        ({','.join([gpkg.ident(k) for k in col_names])})
                    VALUES
                        ({','.join(['?'] * len(col_names))});
                """
                feat_progress = 0
                t0 = time.monotonic()
                t0p = t0

                CHUNK_SIZE = 10000
                total_features = dataset.feature_count()
                for rows in self._chunk(dataset.feature_tuples(col_names), CHUNK_SIZE):
                    dbcur.executemany(sql_insert_features, rows)
                    feat_progress += len(rows)

                    t0a = time.monotonic()
                    L.info(
                        "%.1f%% %d/%d features... @%.1fs (+%.1fs, ~%d F/s)",
                        feat_progress / total_features * 100,
                        feat_progress,
                        total_features,
                        t0a - t0,
                        t0a - t0p,
                        CHUNK_SIZE / (t0a - t0p or 0.001),
                    )
                    t0p = t0a

                t1 = time.monotonic()
                L.info("Added %d features to GPKG in %.1fs", feat_progress, t1 - t0)
                L.info(
                    "Overall rate: %d features/s", (feat_progress / (t1 - t0 or 0.001))
                )

        for dataset in datasets:
            if dataset.has_geometry:
                self._create_spatial_index(dataset)

        with self.session() as db:
            dbcur = db.cursor()
            for dataset in datasets:
                table = dataset.name

                self.update_gpkg_contents(dataset, change_time)

                # Create triggers
                self._create_triggers(dbcur, table)

            dbcur.execute(
                f"INSERT OR REPLACE INTO {self.META_TABLE} (table_name, key, value) VALUES (?, ?, ?);",
                ("*", "tree", target_tree_or_commit.peel(pygit2.Tree).hex),
            )

    def write_features(self, dbcur, dataset, pk_iter, *, ignore_missing=False):
        cols, pk_field = self._get_columns(dataset)
        col_names = cols.keys()

        sql_write_feature = f"""
            INSERT OR REPLACE INTO {gpkg.ident(dataset.name)}
                ({','.join([gpkg.ident(k) for k in col_names])})
            VALUES
                ({','.join(['?'] * len(col_names))});
        """

        feat_count = 0
        CHUNK_SIZE = 10000
        for rows in self._chunk(
            dataset.get_feature_tuples(
                pk_iter, col_names, ignore_missing=ignore_missing
            ),
            CHUNK_SIZE,
        ):
            dbcur.executemany(sql_write_feature, rows)
            feat_count += dbcur.getconnection().changes()

        return feat_count

    def delete_features(self, dbcur, dataset, pk_iter):
        cols, pk_field = self._get_columns(dataset)

        sql_del_feature = f"""
            DELETE FROM {gpkg.ident(dataset.name)}
            WHERE {gpkg.ident(pk_field)}=?;
        """

        feat_count = 0
        CHUNK_SIZE = 10000
        for rows in self._chunk(zip(pk_iter), CHUNK_SIZE):
            dbcur.executemany(sql_del_feature, rows)
            feat_count += dbcur.getconnection().changes()

        return feat_count

    def drop_table(self, target_tree_or_commit, *datasets):
        with self.session() as db:
            dbcur = db.cursor()
            for dataset in datasets:
                table = dataset.name
                if dataset.has_geometry:
                    self._drop_spatial_index(dataset)

                dbcur.execute(f"""DROP TABLE {gpkg.ident(table)};""")
                self.delete_meta(dataset)

                dbcur.execute(
                    f"""DELETE FROM {self.TRACKING_TABLE} WHERE table_name=?;""",
                    (table,),
                )

    def diff_db_to_tree_meta(self, dataset):
        """
        Returns a DeltaDiff showing all the changes of metadata between the dataset and this working copy.
        How the metadata is formatted depends on the version of the dataset.
        """
        meta_old = dict(dataset.iter_meta_items())
        meta_new = dict(self.iter_meta_items(dataset))
        if "schema" in meta_old and "schema" in meta_new:
            Schema.align_schema_cols(meta_old["schema"], meta_new["schema"])
        return DeltaDiff.diff_dicts(meta_old, meta_new)

    def diff_db_to_tree(self, dataset, ds_filter=UNFILTERED, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for a single dataset only.

        Pass a list of PK values to filter results to them
        """
        ds_filter = ds_filter or UNFILTERED
        pk_filter = ds_filter.get("feature", ())
        with self.session() as db:
            dbcur = db.cursor()

            table = dataset.name
            pk_field = dataset.primary_key

            ds_diff = DatasetDiff()
            do_find_renames = True

            ds_diff["meta"] = self.diff_db_to_tree_meta(dataset)

            if raise_if_dirty and ds_diff["meta"]:
                raise WorkingCopyDirty()

            diff_sql = f"""
                SELECT
                    {self.TRACKING_TABLE}.pk AS ".__track_pk",
                    {gpkg.ident(table)}.*
                FROM {self.TRACKING_TABLE} LEFT OUTER JOIN {gpkg.ident(table)}
                ON ({self.TRACKING_TABLE}.pk = {gpkg.ident(table)}.{gpkg.ident(pk_field)})
                WHERE ({self.TRACKING_TABLE}.table_name = ?)
            """
            params = [table]
            if pk_filter is not UNFILTERED:
                diff_sql += f"\nAND {self.TRACKING_TABLE}.pk IN ({','.join(['?']*len(pk_filter))})"
                params += [str(pk) for pk in pk_filter]
            dbcur.execute(diff_sql, params)

            feature_diff = DeltaDiff()
            insert_count = delete_count = 0

            for row in dbcur:
                track_pk = row[0]  # This is always a str
                db_obj = {k: row[k] for k in row.keys() if k != ".__track_pk"}

                if db_obj[pk_field] is None:
                    db_obj = None

                try:
                    repo_obj = dataset.get_feature(track_pk, ogr_geoms=False)
                except KeyError:
                    repo_obj = None

                if repo_obj == db_obj:
                    # DB was changed and then changed back - eg INSERT then DELETE.
                    # TODO - maybe delete track_pk from tracking table?
                    continue

                if raise_if_dirty:
                    raise WorkingCopyDirty()

                if db_obj and not repo_obj:  # INSERT
                    insert_count += 1
                    feature_diff.add_delta(Delta.insert((db_obj[pk_field], db_obj)))

                elif repo_obj and not db_obj:  # DELETE
                    delete_count += 1
                    feature_diff.add_delta(Delta.delete((repo_obj[pk_field], repo_obj)))

                else:  # UPDATE
                    pk = db_obj[pk_field]
                    feature_diff.add_delta(Delta.update((pk, repo_obj), (pk, db_obj)))

        if (
            self.can_find_renames(ds_diff["meta"])
            and (insert_count + delete_count) <= 400
        ):
            self.find_renames(feature_diff, dataset)

        ds_diff["feature"] = feature_diff
        return ds_diff

    def can_find_renames(self, meta_diff):
        """There's no point looking for renames if the schema has changed."""
        if "schema" not in meta_diff:
            return True

        schema_delta = meta_diff["schema"]
        if not schema_delta.old_value or not schema_delta.new_value:
            return False

        old_col_ids = [c["id"] for c in schema_delta.old_value]
        new_col_ids = [c["id"] for c in schema_delta.new_value]
        return old_col_ids == new_col_ids

    def find_renames(self, feature_diff, dataset):
        """
        Matches inserts + deletes into renames on a best effort basis.
        changes at most one matching insert and delete into an update per blob-hash.
        Modifies feature_diff in place.
        """
        hash_feature = lambda f: pygit2.hash(dataset.encode_feature_blob(f)).hex

        inserts = {}
        deletes = {}

        search_size = 0
        for delta in feature_diff.values():
            if delta.type == "insert":
                inserts[hash_feature(delta.new_value)] = delta
            elif delta.type == "delete":
                deletes[hash_feature(delta.old_value)] = delta

        for h in deletes:
            if h in inserts:
                delete_delta = deletes[h]
                insert_delta = inserts[h]

                del feature_diff[delete_delta.key]
                del feature_diff[insert_delta.key]
                update_delta = delete_delta + insert_delta
                feature_diff.add_delta(update_delta)

    def diff_to_tree(self, repo_filter=UNFILTERED, raise_if_dirty=False):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for every dataset in the given repository structure.
        """
        repo_filter = repo_filter or UNFILTERED

        repo_diff = RepoDiff()
        for dataset in RepositoryStructure.lookup(self.repo, self.get_db_tree()):
            if dataset.path not in repo_filter:
                continue
            ds_diff = self.diff_db_to_tree(
                dataset,
                ds_filter=repo_filter[dataset.path],
                raise_if_dirty=raise_if_dirty,
            )
            repo_diff[dataset.path] = ds_diff
        repo_diff.prune()
        return repo_diff

    def reset_tracking_table(self, reset_filter=UNFILTERED):
        reset_filter = reset_filter or UNFILTERED

        with self.session() as db:
            dbcur = db.cursor()
            if reset_filter == UNFILTERED:
                dbcur.execute(f"DELETE FROM {self.TRACKING_TABLE};")
                return

            for dataset, dataset_filter in reset_filter.items():
                if (
                    dataset_filter == UNFILTERED
                    or dataset_filter.get("feature") == UNFILTERED
                ):
                    dbcur.execute(
                        f"DELETE FROM {self.TRACKING_TABLE} WHERE table_name=?;",
                        (dataset,),
                    )
                    continue

                CHUNK_SIZE = 100
                pks = dataset_filter.get("feature", ())
                for pk_chunk in self._chunk(pks, CHUNK_SIZE):
                    dbcur.execute(
                        f"DELETE FROM {self.TRACKING_TABLE} WHERE table_name=? AND pk IN ({','.join('?' * len(pk_chunk))});",
                        (dataset, *pk_chunk),
                    )

    def update_meta_table(self, new_tree):
        with self.session() as db:
            dbcur = db.cursor()
            L.info(f"Tree sha: {new_tree}")

            dbcur.execute(
                f"UPDATE {self.META_TABLE} SET value=? WHERE table_name='*' AND key='tree';",
                (str(new_tree),),
            )
            assert (
                db.changes() == 1
            ), f"{self.META_TABLE} update: expected 1Δ, got {db.changes()}"

    def _is_meta_update_supported(self, src_ds, dest_ds):
        """
        Returns True if the given meta-diff is supported *without* dropping and rewriting the table.
        (Any meta change is supported - even in datasets v1 - if we drop and rewrite the table,
        but of course it is less efficient).
        """
        meta_diff = src_ds.meta_tree.diff_to_tree(dest_ds.meta_tree)
        if not meta_diff:
            return True

        # Sanity check:
        for patch in meta_diff:
            delta = patch.delta
            if delta.old_file and delta.new_file:
                assert delta.old_file.path == delta.new_file.path

        if src_ds.version < 2:
            # Dataset1 doesn't support meta changes at all - except by rewriting the entire table.
            return False

        src_schema = src_ds.schema
        dest_schema = dest_ds.schema
        dt = src_schema.diff_type_counts(dest_schema)
        dt.pop("name_updates")  # We do support name_updates.
        if sum(dt.values()):
            return False  # But we don't support any other type of schema update - except by rewriting the entire table.

        return True

    def _apply_meta_title(self, src_ds, dest_ds, src_value, dest_value, dbcur):
        # TODO - find a better way to roundtrip titles while keeping them unique
        value = f"{dest_ds.name}: {dest_value}"
        dbcur.execute(
            '''
            UPDATE gpkg_contents SET identifier = ?
            WHERE table_name = ?
            ''',
            [value, dest_ds.name],
        )

    def _apply_meta_description(self, src_ds, dest_ds, src_value, dest_value, dbcur):
        dbcur.execute(
            '''
            UPDATE gpkg_contents SET description = ?
            WHERE table_name = ?
            ''',
            [dest_value, dest_ds.name],
        )

    def _apply_meta_schema(self, src_ds, dest_ds, src_value, dest_value, dbcur):
        table_name = dest_ds.name
        src_schema = Schema.from_column_dicts(src_value)
        dest_schema = Schema.from_column_dicts(dest_value)

        diff_types = src_schema.diff_types(dest_schema)
        name_updates = diff_types.pop("name_updates")
        if any(dt for dt in diff_types.values()):
            raise RuntimeError(
                f"This schema change not supported by update - should be drop + rewrite_full: {diff_types}"
            )

        for col_id in name_updates:
            src_name = src_schema[col_id].name
            dest_name = dest_schema[col_id].name
            dbcur.execute(
                f'''ALTER TABLE {gpkg.ident(table_name)} RENAME COLUMN {gpkg.ident(src_name)} TO {gpkg.ident(dest_name)}'''
            )

    def _apply_meta_diff(self, src_ds, dest_ds, dbcur, meta_diff_index):
        L.debug("Meta diff: %s changes", len(meta_diff_index))
        for d in meta_diff_index.deltas:

            path = d.old_file.path or d.new_file.path
            try:
                func = getattr(self, f'_apply_meta_{path.replace("/", "__")}')
            except AttributeError:
                continue
            else:
                # for deltas we care about, we dont' support a whole lot.
                # in particular we don't currently support deletes
                if d.status not in (pygit2.GIT_DELTA_MODIFIED, pygit2.GIT_DELTA_ADDED,):
                    # GIT_DELTA_DELETED
                    # GIT_DELTA_RENAMED
                    # GIT_DELTA_COPIED
                    # GIT_DELTA_IGNORED
                    # GIT_DELTA_TYPECHANGE
                    # GIT_DELTA_UNMODIFIED
                    # GIT_DELTA_UNREADABLE
                    # GIT_DELTA_UNTRACKED
                    raise NotImplementedError(f"Delta status: {d.status_char()}")

                src_value = src_ds.get_meta_item(path)
                dest_value = dest_ds.get_meta_item(path)
                func(src_ds, dest_ds, src_value, dest_value, dbcur)

    def _apply_feature_diff(self, src_ds, dest_ds, dbcur, feature_diff_index):
        L.debug("Feature diff: %s changes", len(feature_diff_index))
        for d in feature_diff_index.deltas:
            # TODO: improve this by grouping by status then calling
            # write_features/delete_features passing multiple PKs?
            if d.status == pygit2.GIT_DELTA_DELETED:
                old_pk = src_ds.decode_path_to_1pk(os.path.basename(d.old_file.path))
                L.debug("reset(): D %s (%s)", d.old_file.path, old_pk)
                self.delete_features(dbcur, src_ds, [old_pk])
            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                old_pk = src_ds.decode_path_to_1pk(os.path.basename(d.old_file.path))
                new_pk = dest_ds.decode_path_to_1pk(os.path.basename(d.new_file.path))
                L.debug(
                    "reset(): M %s (%s) -> %s (%s)",
                    d.old_file.path,
                    old_pk,
                    d.new_file.path,
                    new_pk,
                )
                self.write_features(dbcur, dest_ds, [new_pk])
            elif d.status == pygit2.GIT_DELTA_ADDED:
                new_pk = dest_ds.decode_path_to_1pk(os.path.basename(d.new_file.path))
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
                raise NotImplementedError(f"Delta status: {d.status_char()}")

    def reset(
        self, target_tree_or_commit, *, force=False, paths=None, update_meta=True,
    ):
        """
        Resets the working copy to the given tree (or the tree pointed to by the given commit)

        If there are uncommitted changes, raises InvalidOperation, unless force=True is given
        (in which case the changes are discarded)

        If update_meta=True (the default) the tree ID in the .sno-meta table gets set
        to the new tree ID. Otherwise it is unchanged.
        """
        if not force:
            self.check_not_dirty()

        L = logging.getLogger(f"{self.__class__.__qualname__}.reset")
        commit = None
        if isinstance(target_tree_or_commit, pygit2.Commit):
            commit = target_tree_or_commit
            target_tree = commit.tree
        else:
            commit = None
            target_tree = target_tree_or_commit
        L.debug(
            f"c={commit.id if commit else 'none'} t={target_tree.hex} update-meta={update_meta}",
        )

        base_tree_id = self.get_db_tree()
        base_tree = self.repo[base_tree_id]
        L.debug("base_tree_id: %s", base_tree_id)
        repo_tree_id = self.repo.head.peel(pygit2.Tree).hex

        L.debug(
            "Working Copy DB is tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree:%s",
            base_tree_id,
            repo_tree_id,
            target_tree,
        )

        repo_structure = RepositoryStructure(self.repo)
        src_datasets = {ds.name: ds for ds in repo_structure.iter_at(base_tree)}
        dest_datasets = {ds.name: ds for ds in repo_structure.iter_at(target_tree)}

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

        table_inserts = dest_datasets.keys() - src_datasets.keys()
        table_deletes = src_datasets.keys() - dest_datasets.keys()
        table_updates = src_datasets.keys() & dest_datasets.keys()
        table_updates_unsupported = set()

        for table in table_updates:
            src_ds = src_datasets[table]
            dest_ds = dest_datasets[table]
            update_supported = self._is_meta_update_supported(src_ds, dest_ds)
            if not update_supported:
                table_updates_unsupported.add(table)

        for table in table_updates_unsupported:
            table_updates.remove(table)
            table_inserts.add(table)
            table_deletes.add(table)

        L.debug(
            "table_inserts: %s\ntable_deletes: %s\ntable_updates %s",
            table_inserts,
            table_deletes,
            table_updates,
        )

        # Delete old tables
        if table_deletes:
            self.drop_table(
                target_tree_or_commit, *[src_datasets[d] for d in table_deletes]
            )
        # Write new tables
        if table_inserts:
            # Note: write_full doesn't work if called from within an existing db session.
            self.write_full(
                target_tree_or_commit, *[dest_datasets[d] for d in table_inserts]
            )

        with self.session(bulk=1) as db:
            dbcur = db.cursor()

            for table in table_updates:
                src_ds = src_datasets[table]
                dest_ds = dest_datasets[table]
                self._update_table(
                    commit,
                    src_ds,
                    dest_ds,
                    db,
                    dbcur,
                    track_changes_as_dirty=not update_meta,
                )

            if update_meta:
                # update the tree id
                dbcur.execute(
                    f"UPDATE {self.META_TABLE} SET value=? WHERE table_name='*' AND key='tree';",
                    (target_tree.hex,),
                )

    def _update_table(
        self, commit, src_ds, dest_ds, db, dbcur, track_changes_as_dirty=False
    ):
        """
        Update the given table in working copy from src_ds to dest_ds.
        The table must exist in the working copy in the source and continue to exist in the destination,
        and, the table must not have any unsupported meta changes.
        """
        # Existing table with update:
        # check for schema differences

        table = src_ds.name

        sql_changed = f"SELECT pk FROM {self.TRACKING_TABLE} " "WHERE table_name=?;"
        dbcur.execute(sql_changed, (table,))
        dirty_pk_list = [r[0] for r in dbcur]
        if dirty_pk_list:
            with self._suspend_triggers(dbcur, table):
                # todo: suspend/remove spatial index
                L.debug("Cleaning up dirty rows...")

                track_count = db.changes()
                count = self.delete_features(dbcur, src_ds, dirty_pk_list)
                L.debug(
                    "reset(): dirty: removed %s features, tracking Δ count=%s",
                    count,
                    track_count,
                )
                count = self.write_features(
                    dbcur, src_ds, dirty_pk_list, ignore_missing=True
                )
                L.debug(
                    "reset(): dirty: wrote %s features, tracking Δ count=%s",
                    count,
                    track_count,
                )

                dbcur.execute(
                    f"DELETE FROM {self.TRACKING_TABLE} WHERE table_name=?;", (table,),
                )

        if src_ds.tree == dest_ds.tree:
            return

        if not track_changes_as_dirty:
            ctx = self._suspend_triggers(dbcur, table)
        else:
            # We want to track these changes as working copy edits so they can be committed.
            ctx = contextlib.nullcontext()

        with ctx:
            self._apply_meta_diff(
                src_ds,
                dest_ds,
                dbcur,
                src_ds.meta_tree.diff_to_tree(dest_ds.meta_tree),
            )
            self._apply_feature_diff(
                src_ds,
                dest_ds,
                dbcur,
                src_ds.feature_tree.diff_to_tree(dest_ds.feature_tree),
            )

        # Update gpkg_contents
        if commit:
            change_time = datetime.utcfromtimestamp(commit.commit_time)
        else:
            change_time = datetime.utcnow()

        geom_col = dest_ds.geom_column_name
        if geom_col is not None:
            # FIXME: Why doesn't Extent(geom) work here as an aggregate?
            dbcur.execute(
                f"""
                WITH _BBOX AS (
                    SELECT
                        Min(MbrMinX({gpkg.ident(geom_col)})) AS min_x,
                        Min(MbrMinY({gpkg.ident(geom_col)})) AS min_y,
                        Max(MbrMaxX({gpkg.ident(geom_col)})) AS max_x,
                        Max(MbrMaxY({gpkg.ident(geom_col)})) AS max_y
                    FROM {gpkg.ident(table)}
                )
                UPDATE gpkg_contents
                SET
                    last_change=?,
                    min_x=(SELECT min_x FROM _BBOX),
                    min_y=(SELECT min_y FROM _BBOX),
                    max_x=(SELECT max_x FROM _BBOX),
                    max_y=(SELECT max_y FROM _BBOX)
                WHERE
                    table_name=?;
                """,
                (
                    change_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # GPKG Spec Req.15
                    table,
                ),
            )
        else:
            dbcur.execute(
                f"""
                UPDATE gpkg_contents
                SET
                    last_change=?
                WHERE
                    table_name=?;
                """,
                (
                    change_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # GPKG Spec Req.15
                    table,
                ),
            )

        rowcount = db.changes()
        assert rowcount == 1, f"gpkg_contents update: expected 1Δ, got {rowcount}"


class WorkingCopy_GPKG_1(WorkingCopyGPKG):
    """
    GeoPackage Working Copy for v0.1-v0.4 repositories
    """

    META_PREFIX = ".sno-"


class WorkingCopy_GPKG_2(WorkingCopyGPKG):
    """
    GeoPackage Working Copy for v0.5+ repositories
    """

    # Using this prefix means OGR/QGIS doesn't list these tables as datasets
    META_PREFIX = "gpkg_sno_"
