import collections
import contextlib
import itertools
import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import click
import pygit2
from osgeo import gdal

from . import gpkg, core
from .structure import RepositoryStructure


@click.command('wc-new')
@click.pass_context
@click.argument("path", type=click.Path(writable=True, dir_okay=False))
@click.argument("datasets", nargs=-1)
def wc_new(ctx, path, datasets):
    """ Temporary command to create a new working copy """
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
    wc.save_config()


class WorkingCopy:
    @classmethod
    def open(cls, repo):
        repo_cfg = repo.config
        if "snowdrop.workingcopy.version" in repo_cfg:
            version = repo_cfg['snowdrop.workingcopy.version']
            if repo_cfg.get_int('snowdrop.workingcopy.version') != 1:
                raise NotImplementedError(f"Working copy version: {version}")

            path = repo_cfg['snowdrop.workingcopy.path']
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Working copy missing? {path}")

            return WorkingCopy_GPKG_1(repo, path)

        elif "kx.workingcopy" in repo_cfg:
            path, table = repo_cfg['kx.workingcopy'].split(':')[1:]
            if not os.path.isfile(path):
                raise FileNotFoundError(f"Working copy missing? {path}")

            return WorkingCopy_GPKG_0(repo, path, table=table)

        else:
            return None

    @classmethod
    def new(cls, repo, path, version=1, **kwargs):
        if os.path.isfile(path):
            raise FileExistsError(path)

        if version == 0:
            return WorkingCopy_GPKG_0(repo, path, **kwargs)
        else:
            return WorkingCopy_GPKG_1(repo, path, **kwargs)


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
    def session(self, bulk_load=False):
        L = logging.getLogger(f"{self.__class__.__qualname__}.session")

        if hasattr(self, '_db'):
            # inner
            L.debug(f"session(bulk_load={bulk_load}): existing...")
            yield self._db
            L.debug(f"session(bulk_load={bulk_load}): existing/done")
        else:
            L.debug(f"session(bulk_load={bulk_load}): new...")
            self._db = gpkg.db(self.path, isolation_level=None)  # autocommit (also means manual transaction management)

            if bulk_load:
                L.debug("Invoking bulk-load mode")
                orig_journal = self._db.execute("PRAGMA journal_mode;").fetchone()[0]
                orig_locking = self._db.execute("PRAGMA locking_mode;").fetchone()[0]
                self._db.execute("PRAGMA synchronous = OFF;")
                self._db.execute("PRAGMA journal_mode = MEMORY;")
                self._db.execute("PRAGMA cache_size = -1048576;")  # -KiB => 1GiB
                self._db.execute("PRAGMA locking_mode = EXCLUSIVE;")

            try:
                self._db.execute("BEGIN")
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
            L.debug(f"session(bulk_load={bulk_load}): new/done")

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
        print(f">>> DEL {self.full_path}")
        self.full_path.unlink()

        # for sqlite this might include wal/journal/etc files
        # app.gpkg -> app.gpkg-wal, app.gpkg-journal
        # https://www.sqlite.org/shortnames.html
        for f in Path(self.path).parent.glob(Path(self.path).name + "-*"):
            print(f">>> DEL '{f}'")
            # f.unlink()

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

        try:
            meta_md = dataset.get_meta_item("gpkg_metadata")
        except KeyError:
            meta_md = {}
        try:
            meta_md_ref = dataset.get_meta_item("gpkg_metadata_reference")
        except KeyError:
            meta_md_ref = {}

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
        with self.session() as db:
            return gpkg.get_meta_info(db, dataset.name, dataset.version)

    def save_config(self, **kwargs):
        new_path = Path(self.path)
        if not new_path.is_absolute():
            new_path = os.path.relpath(new_path, Path(self.repo.path).resolve())

        self.repo.config["snowdrop.workingcopy.version"] = 1
        self.repo.config["snowdrop.workingcopy.path"] = str(new_path)

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

    def assert_db_tree_match(self, tree, *, table_name='*'):
        with self.session() as db:
            dbcur = db.cursor()
            dbcur.execute(
                f"""
                    SELECT value
                    FROM {self.META_TABLE}
                    WHERE table_name=? AND key=?;
                """,
                (table_name, 'tree')
            )
            row = dbcur.fetchone()
            if not row:
                raise ValueError(f"No meta entry for {table_name}")

            wc_tree_id = row[0]

            tree_sha = tree.hex

            if wc_tree_id != tree_sha:
                raise core.WorkingCopyMismatch(wc_tree_id, tree_sha)
            return wc_tree_id


class WorkingCopy_GPKG_0(WorkingCopyGPKG):
    """
    GeoPackage Working Copy for v0.0 repositories

    Key difference here is a separate UUID feature-key from the PK.
    """
    META_PREFIX = "__kxg_"

    def __init__(self, *args, table, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = table

    @property
    def TRACKING_TABLE(self):
        return self._meta_name("map")

    def save_config(self):
        new_path = Path(self.path)
        if not new_path.is_absolute():
            new_path = os.path.relpath(new_path, Path(self.repo.path).resolve())

        self.repo.config["kx.workingcopy"] = f"GPKG:{new_path}:{self.table}"

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

    def delete(self):
        super().delete()

        # clear the config in the repo
        del self.repo.config["kx.workingcopy"]

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

    def diff_db_to_tree(self, dataset):
        from . import diff

        with self.session() as db:
            return diff.db_to_tree(self.repo, self.table, db, dataset.tree)

    def assert_db_tree_match(self, tree):
        return super().assert_db_tree_match(tree, table_name=self.table)

    def commit(self, tree, wcdiff, message):
        from . import diff

        table = self.table

        with self.session() as db:
            self.assert_db_tree_match(tree)

            wcdiff = diff.db_to_tree(self.repo, table, db)
            if not any(wcdiff.values()):
                raise click.ClickException("No changes to commit")

            dbcur = db.cursor()

            git_index = pygit2.Index()
            git_index.read_tree(tree)

            for k, (obj_old, obj_new) in wcdiff["META"].items():
                object_path = f"{table}/meta/{k}"
                value = json.dumps(obj_new).encode("utf8")

                blob = self.repo.create_blob(value)
                idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
                git_index.add(idx_entry)
                click.secho(f"Δ {object_path}", fg="yellow")

            pk_field = gpkg.pk(db, table)

            for feature_key in wcdiff["D"].keys():
                object_path = f"{table}/features/{feature_key[:4]}/{feature_key}"
                git_index.remove_all([f"{object_path}/**"])
                click.secho(f"- {object_path}", fg="red")

                dbcur.execute(
                    "DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?",
                    (table, feature_key),
                )
                assert (
                    dbcur.rowcount == 1
                ), f"__kxg_map delete: expected 1Δ, got {dbcur.rowcount}"

            for obj in wcdiff["I"]:
                feature_key = str(uuid.uuid4())
                for k, value in obj.items():
                    object_path = f"{table}/features/{feature_key[:4]}/{feature_key}/{k}"
                    if not isinstance(value, bytes):  # blob
                        value = json.dumps(value).encode("utf8")

                    blob = self.repo.create_blob(value)
                    idx_entry = pygit2.IndexEntry(
                        object_path, blob, pygit2.GIT_FILEMODE_BLOB
                    )
                    git_index.add(idx_entry)
                    click.secho(f"+ {object_path}", fg="green")

                dbcur.execute(
                    "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);",
                    (table, feature_key, obj[pk_field]),
                )
            dbcur.execute(
                "DELETE FROM __kxg_map WHERE table_name=? AND feature_key IS NULL;",
                (table,),
            )

            for feature_key, (obj_old, obj_new) in wcdiff["U"].items():
                s_old = set(obj_old.items())
                s_new = set(obj_new.items())

                diff_add = dict(s_new - s_old)
                diff_del = dict(s_old - s_new)
                all_keys = set(diff_del.keys()) | set(diff_add.keys())

                for k in all_keys:
                    object_path = f"{table}/features/{feature_key[:4]}/{feature_key}/{k}"
                    if k in diff_add:
                        value = obj_new[k]
                        if not isinstance(value, bytes):  # blob
                            value = json.dumps(value).encode("utf8")

                        blob = self.repo.create_blob(value)
                        idx_entry = pygit2.IndexEntry(
                            object_path, blob, pygit2.GIT_FILEMODE_BLOB
                        )
                        git_index.add(idx_entry)
                        click.secho(f"Δ {object_path}", fg="yellow")
                    else:
                        git_index.remove(object_path)
                        click.secho(f"- {object_path}", fg="red")

            dbcur.execute(
                "UPDATE __kxg_map SET state=0 WHERE table_name=? AND state != 0;", (table,)
            )

            print("Writing tree...")
            new_tree = git_index.write_tree(self.repo)
            print(f"Tree sha: {new_tree}")

            dbcur.execute(
                "UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';",
                (str(new_tree), table),
            )
            assert (
                dbcur.rowcount == 1
            ), f"__kxg_meta update: expected 1Δ, got {dbcur.rowcount}"

            print("Committing...")
            user = self.repo.default_signature
            # this will also update the ref (branch) to point to the current commit
            new_commit = self.repo.create_commit(
                "HEAD",  # reference_name
                user,  # author
                user,  # committer
                message,  # message
                new_tree,  # tree
                [self.repo.head.target],  # parents
            )
            print(f"Commit: {new_commit}")

            # TODO: update reflog
            return new_commit


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

    def delete(self):
        super().delete()

        # clear the config in the repo
        del self.repo.config["snowdrop.workingcopy"]

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
        it = iter(iterable)
        while True:
            chunk_it = itertools.islice(it, size)
            try:
                first_el = next(chunk_it)
            except StopIteration:
                return
            yield itertools.chain((first_el,), chunk_it)

    def write_full(self, commit, *datasets):
        """
        Writes a full layer into a working-copy table

        Use for new working-copy checkouts.
        """
        L = logging.getLogger(f"{self.__class__.__qualname__}.write_full")

        with self.session(bulk_load=True) as db:
            for dataset in datasets:
                table = dataset.name

                dbcur = db.cursor()
                self.write_meta(dataset)

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

        for dataset in datasets:
            if dataset.has_geometry:
                self._create_spatial_index(dataset)

        with self.session() as db:
            for dataset in datasets:
                table = dataset.name

                self.update_gpkg_contents(commit, dataset)

                # Create triggers
                self._create_triggers(db, table)

            db.execute(
                f"INSERT INTO {self.META_TABLE} (table_name, key, value) VALUES (?, ?, ?);",
                ('*', 'tree', commit.peel(pygit2.Tree).hex),
            )

    def diff_db_to_tree(self, dataset):
        """ Generates a diff between a working copy DB and the underlying repository tree """
        with self.session() as db:
            dbcur = db.cursor()

            table = dataset.name

            meta_diff = {}
            meta_old = dict(dataset.iter_meta_items(exclude={'fields', 'primary_key'}))
            meta_new = dict(self.read_meta(dataset))
            for name in set(meta_new.keys()) ^ set(meta_old.keys()):
                meta_diff[name] = (meta_old.get(name), meta_new.get(name))

            pk_field = dataset.primary_key

            diff_sql = f"""
                SELECT
                    {self.TRACKING_TABLE}.pk AS ".__track_pk",
                    {gpkg.ident(table)}.*
                FROM {self.TRACKING_TABLE} LEFT OUTER JOIN {gpkg.ident(table)}
                ON ({self.TRACKING_TABLE}.pk = {gpkg.ident(table)}.{gpkg.ident(pk_field)})
                WHERE ({self.TRACKING_TABLE}.table_name = ?)
            """
            dbcur.execute(diff_sql, (table,))

            candidates_ins = collections.defaultdict(list)
            candidates_upd = {}
            candidates_del = collections.defaultdict(list)
            for row in dbcur:
                track_pk = row[0]
                db_obj = {k: row[k] for k in row.keys() if k != '.__track_pk'}

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

            return {
                "META": meta_diff,
                "I": list(itertools.chain(*candidates_ins.values())),
                "D": dict(itertools.chain(*candidates_del.values())),
                "U": candidates_upd,
            }


    def commit(self, tree, wcdiff, message):
        raise NotImplementedError()
        # table = self.table

        # with self.session() as db:
        #     core.assert_db_tree_match(db, table, tree)

        #     wcdiff = diff.db_to_tree(self.repo, table, db)
        #     if not any(wcdiff.values()):
        #         raise click.ClickException("No changes to commit")

        #     dbcur = db.cursor()

        #     git_index = pygit2.Index()
        #     git_index.read_tree(tree)

        #     for k, (obj_old, obj_new) in wcdiff["META"].items():
        #         object_path = f"{table}/meta/{k}"
        #         value = json.dumps(obj_new).encode("utf8")

        #         blob = self.repo.create_blob(value)
        #         idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
        #         git_index.add(idx_entry)
        #         click.secho(f"Δ {object_path}", fg="yellow")

        #     pk_field = gpkg.pk(db, table)

        #     for feature_key in wcdiff["D"].keys():
        #         object_path = f"{table}/features/{feature_key[:4]}/{feature_key}"
        #         git_index.remove_all([f"{object_path}/**"])
        #         click.secho(f"- {object_path}", fg="red")

        #         dbcur.execute(
        #             "DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?",
        #             (table, feature_key),
        #         )
        #         assert (
        #             dbcur.rowcount == 1
        #         ), f"__kxg_map delete: expected 1Δ, got {dbcur.rowcount}"

        #     for obj in wcdiff["I"]:
        #         feature_key = str(uuid.uuid4())
        #         for k, value in obj.items():
        #             object_path = f"{table}/features/{feature_key[:4]}/{feature_key}/{k}"
        #             if not isinstance(value, bytes):  # blob
        #                 value = json.dumps(value).encode("utf8")

        #             blob = self.repo.create_blob(value)
        #             idx_entry = pygit2.IndexEntry(
        #                 object_path, blob, pygit2.GIT_FILEMODE_BLOB
        #             )
        #             git_index.add(idx_entry)
        #             click.secho(f"+ {object_path}", fg="green")

        #         dbcur.execute(
        #             "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);",
        #             (table, feature_key, obj[pk_field]),
        #         )
        #     dbcur.execute(
        #         "DELETE FROM __kxg_map WHERE table_name=? AND feature_key IS NULL;",
        #         (table,),
        #     )

        #     for feature_key, (obj_old, obj_new) in wcdiff["U"].items():
        #         s_old = set(obj_old.items())
        #         s_new = set(obj_new.items())

        #         diff_add = dict(s_new - s_old)
        #         diff_del = dict(s_old - s_new)
        #         all_keys = set(diff_del.keys()) | set(diff_add.keys())

        #         for k in all_keys:
        #             object_path = f"{table}/features/{feature_key[:4]}/{feature_key}/{k}"
        #             if k in diff_add:
        #                 value = obj_new[k]
        #                 if not isinstance(value, bytes):  # blob
        #                     value = json.dumps(value).encode("utf8")

        #                 blob = self.repo.create_blob(value)
        #                 idx_entry = pygit2.IndexEntry(
        #                     object_path, blob, pygit2.GIT_FILEMODE_BLOB
        #                 )
        #                 git_index.add(idx_entry)
        #                 click.secho(f"Δ {object_path}", fg="yellow")
        #             else:
        #                 git_index.remove(object_path)
        #                 click.secho(f"- {object_path}", fg="red")

        #     dbcur.execute(
        #         "UPDATE __kxg_map SET state=0 WHERE table_name=? AND state != 0;", (table,)
        #     )

        #     print("Writing tree...")
        #     new_tree = git_index.write_tree(self.repo)
        #     print(f"Tree sha: {new_tree}")

        #     dbcur.execute(
        #         "UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';",
        #         (str(new_tree), table),
        #     )
        #     assert (
        #         dbcur.rowcount == 1
        #     ), f"__kxg_meta update: expected 1Δ, got {dbcur.rowcount}"

        #     print("Committing...")
        #     user = self.repo.default_signature
        #     # this will also update the ref (branch) to point to the current commit
        #     new_commit = self.repo.create_commit(
        #         "HEAD",  # reference_name
        #         user,  # author
        #         user,  # committer
        #         message,  # message
        #         new_tree,  # tree
        #         [self.repo.head.target],  # parents
        #     )
        #     print(f"Commit: {new_commit}")

        #     # TODO: update reflog
        #     return new_commit
