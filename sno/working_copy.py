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

from . import gpkg, diff
from .exceptions import InvalidOperation
from .filter_util import UNFILTERED
from .gpkg_adapter import GPKG_META_ITEMS

L = logging.getLogger("sno.working_copy")


class WorkingCopy:
    @classmethod
    def open(cls, repo):
        repo_cfg = repo.config
        if "sno.workingcopy.version" in repo_cfg:
            version = repo_cfg["sno.workingcopy.version"]
            if repo_cfg.get_int("sno.workingcopy.version") != 1:
                raise NotImplementedError(f"Working copy version: {version}")

            path = repo_cfg["sno.workingcopy.path"]
            if not (Path(repo.path) / path).is_file():
                raise FileNotFoundError(f"Working copy missing? {path}")

            return WorkingCopy_GPKG_1(repo, path)

        else:
            return None

    @classmethod
    def new(cls, repo, path, version=1, **kwargs):
        if (Path(repo.path) / path).exists():
            raise FileExistsError(path)

        return WorkingCopy_GPKG_1(repo, path, **kwargs)

    class Mismatch(ValueError):
        def __init__(self, working_copy_tree_id, match_tree_id):
            self.working_copy_tree_id = working_copy_tree_id
            self.match_tree_id = match_tree_id

        def __str__(self):
            return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"


class WorkingCopyGPKG(WorkingCopy):
    META_PREFIX = ".sno-"

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
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(f"SELECT COUNT(*) FROM {self.TRACKING_TABLE};")
            return dbcur.fetchone()[0]

    def check_not_dirty(
        self,
        message="You have uncommitted changes in your working copy. Commit or discard first",
    ):
        """
        Checks the working copy has no changes in it.
        Otherwise, raises InvalidOperation
        """
        if self.is_dirty():
            raise InvalidOperation(message)

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

    def delete(self):
        """ Delete the working copy files """
        self.full_path.unlink()

        # for sqlite this might include wal/journal/etc files
        # app.gpkg -> app.gpkg-wal, app.gpkg-journal
        # https://www.sqlite.org/shortnames.html
        for f in Path(self.full_path).parent.glob(Path(self.path).name + "-*"):
            f.unlink()

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

    def write_meta(self, dataset):
        meta_info = dataset.get_meta_item("gpkg_contents")
        meta_info["table_name"] = dataset.name
        meta_info["identifier"] = f"{dataset.name}: {meta_info['identifier']}"

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

    def read_meta(self, dataset):
        with self.session() as db:
            yield from gpkg.get_meta_info(db, dataset.name)

    def save_config(self, **kwargs):
        new_path = Path(self.path)
        if (not new_path.is_absolute()) and (str(new_path.parent) != "."):
            new_path = (
                Path(os.path.relpath(new_path.parent, Path(self.repo.path).resolve()))
                / new_path.name
            )

        self.repo.config["sno.workingcopy.version"] = 1
        self.repo.config["sno.workingcopy.path"] = str(new_path)

    def write_full(self, commit, dataset, safe=True):
        raise NotImplementedError()

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

    def _create_triggers(self, dbcur, table):
        raise NotImplementedError()

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
                    commit_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),  # GPKG Spec Req.15
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


class WorkingCopy_GPKG_1(WorkingCopyGPKG):
    """
    GeoPackage Working Copy for v0.1/v0.2 repositories
    """

    def create(self):
        super().create()

        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(
                f"""
                CREATE TABLE {self.TRACKING_TABLE} (
                    table_name TEXT NOT NULL,
                    pk TEXT NULL,
                    CONSTRAINT {self._meta_name('track', 'pk')} PRIMARY KEY (table_name, pk)
                );
            """
            )

    def delete(self):
        super().delete()

        # clear the config in the repo
        del self.repo.config["sno.workingcopy"]

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

    def write_full(self, commit, *datasets, safe=True):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
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

                self.update_gpkg_contents(commit, dataset)

                # Create triggers
                self._create_triggers(dbcur, table)

            dbcur.execute(
                f"INSERT OR REPLACE INTO {self.META_TABLE} (table_name, key, value) VALUES (?, ?, ?);",
                ("*", "tree", commit.peel(pygit2.Tree).hex),
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

    def diff_db_to_tree(self, dataset, pk_filter=UNFILTERED):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for a single dataset only.

        Pass a list of PK values to filter results to them
        """
        pk_filter = pk_filter or UNFILTERED
        with self.session() as db:
            dbcur = db.cursor()

            table = dataset.name

            # TODO - diff meta items.

            pk_field = dataset.primary_key

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

            candidates_ins = collections.defaultdict(list)
            candidates_upd = {}
            candidates_del = collections.defaultdict(list)

            for row in dbcur:
                track_pk = row[0]
                db_obj = {k: row[k] for k in row.keys() if k != ".__track_pk"}

                try:
                    repo_obj = dataset.get_feature(track_pk, ogr_geoms=False)
                except KeyError:
                    repo_obj = None

                if db_obj[pk_field] is None:
                    if repo_obj:  # ignore INSERT+DELETE
                        blob_hash = pygit2.hash(
                            dataset.encode_feature_blob(repo_obj)
                        ).hex
                        candidates_del[blob_hash].append((track_pk, repo_obj))
                    continue

                elif not repo_obj:
                    # INSERT
                    blob_hash = pygit2.hash(dataset.encode_feature_blob(db_obj)).hex
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

            def extract_key(obj):
                return obj[pk_field], obj

            from sno.diff import Delta, Diff

            ins = [Delta.insert(extract_key(v[0])) for v in candidates_ins.values()]
            dels = [Delta.delete(extract_key(v[0][1])) for v in candidates_del.values()]
            upd = [
                Delta.update(extract_key(old), extract_key(new))
                for old, new in candidates_upd.values()
            ]

            return Diff(dataset.path, ins + dels + upd)

    def diff_to_tree(self, repo_structure, feature_filter=UNFILTERED):
        """
        Generates a diff between a working copy DB and the underlying repository tree,
        for every dataset in the given repository structure.
        """
        feature_filter = feature_filter or UNFILTERED

        result = diff.Diff()
        for dataset in repo_structure:
            if dataset.path not in feature_filter:
                continue
            ds_diff = self.diff_db_to_tree(
                dataset, pk_filter=feature_filter[dataset.path]
            )
            result.add_child(ds_diff)
        return result

    def reset_tracking_table(self, reset_filter=UNFILTERED):
        reset_filter = reset_filter or UNFILTERED

        with self.session() as db:
            dbcur = db.cursor()
            if reset_filter == UNFILTERED:
                dbcur.execute(f"DELETE FROM {self.TRACKING_TABLE};")
                return

            for dataset, pks in reset_filter.items():
                if pks == UNFILTERED:
                    dbcur.execute(
                        f"DELETE FROM {self.TRACKING_TABLE} WHERE table_name=?;",
                        (dataset,),
                    )
                    continue

                CHUNK_SIZE = 100
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

    def reset(
        self,
        target_tree_or_commit,
        repo_structure,
        *,
        force=False,
        paths=None,
        update_meta=True,
    ):
        """
        Resets the working copy to the given tree (or the tree pointed to by the given commit)

        If there are uncommitted changes, raises InvalidOperation, unless force=True is given
        (in which case the changes are discarded)

        If update_meta=True (the default) the tree ID in the .sno-meta table gets set
        to the new tree ID. Otherwise it is unchanged.
        """

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

        with self.session(bulk=1) as db:
            dbcur = db.cursor()

            base_tree_id = self.get_db_tree()
            base_tree = repo_structure.repo[base_tree_id]
            L.debug("base_tree_id: %s", base_tree_id)
            repo_tree_id = repo_structure.repo.head.peel(pygit2.Tree).hex

            L.debug(
                "Working Copy DB is tree:%s, Repo HEAD has tree:%s. Resetting working copy to tree:%s",
                base_tree_id,
                repo_tree_id,
                target_tree,
            )

            # check for dirty working copy
            is_dirty = self.is_dirty()
            if not force:
                self.check_not_dirty(
                    "You have uncommitted changes in your working copy. Commit or use --force to discard."
                )

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
                            sql_changed = (
                                f"SELECT pk FROM {self.TRACKING_TABLE} "
                                "WHERE table_name=?;"
                            )
                            dbcur.execute(sql_changed, (table,))
                            pk_list = [r[0] for r in dbcur]
                            track_count = db.changes()
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
                                f"DELETE FROM {self.TRACKING_TABLE} WHERE table_name=?;",
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
                                old_pk = src_ds.decode_path_to_1pk(
                                    os.path.basename(d.old_file.path)
                                )
                                L.debug("reset(): D %s (%s)", d.old_file.path, old_pk)
                                self.delete_features(dbcur, src_ds, [old_pk])
                            elif d.status == pygit2.GIT_DELTA_MODIFIED:
                                old_pk = src_ds.decode_path_to_1pk(
                                    os.path.basename(d.old_file.path)
                                )
                                new_pk = dest_ds.decode_path_to_1pk(
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
                                new_pk = dest_ds.decode_path_to_1pk(
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

                    # Update gpkg_contents
                    if commit:
                        change_time = datetime.utcfromtimestamp(commit.commit_time)
                    else:
                        change_time = datetime.utcnow()
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
                                change_time.strftime(
                                    "%Y-%m-%dT%H:%M:%S.%fZ"
                                ),  # GPKG Spec Req.15
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
                                change_time.strftime(
                                    "%Y-%m-%dT%H:%M:%S.%fZ"
                                ),  # GPKG Spec Req.15
                                table,
                            ),
                        )

                    rowcount = db.changes()
                    assert (
                        rowcount == 1
                    ), f"gpkg_contents update: expected 1Δ, got {rowcount}"

            if update_meta:
                # update the tree id
                dbcur.execute(
                    f"UPDATE {self.META_TABLE} SET value=? WHERE table_name='*' AND key='tree';",
                    (target_tree.hex,),
                )
