#!/usr/bin/env python3
import collections
import contextlib
import itertools
import json
import os
import re
import sqlite3
import struct
import subprocess
import sys
import time
import typing
import uuid
from datetime import datetime, timezone

import click
import pygit2
from osgeo import gdal, ogr, osr


gdal.UseExceptions()


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    import osgeo

    click.echo("kxgit proof of concept")
    click.echo(f"GDAL v{osgeo._gdal.__version__}")
    click.echo(f"PyGit2 v{pygit2.__version__}; Libgit2 v{pygit2.LIBGIT2_VERSION}")
    ctx.exit()


@click.group()
@click.option('repo_dir', '--repo', type=click.Path(file_okay=False, dir_okay=True), default=os.curdir, metavar="PATH")
@click.option('--version', is_flag=True, callback=print_version, expose_value=False, is_eager=True, help='Show version information and exit.')
@click.pass_context
def cli(ctx, repo_dir):
    ctx.ensure_object(dict)
    ctx.obj['repo_dir'] = repo_dir


def _execvp(file, args):
    if '_SNOWDROP_NO_EXEC' in os.environ:
        # testing. This is pretty hackzy
        p = subprocess.run([file] + args[1:], capture_output=True, encoding='utf-8')
        sys.stdout.write(p.stdout)
        sys.stderr.write(p.stderr)
        sys.exit(p.returncode)
    else:
        os.execvp(file, args)


def sqlite_ident(identifier):
    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


def sqlite_param_str(value):
    if value is None:
        return "NULL"
    escaped = value.replace('\'', '\'\'')
    return f'\'{escaped}\''


def _get_db(path, **kwargs):
    db = sqlite3.connect(path, **kwargs)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON;")
    db.enable_load_extension(True)
    db.execute("SELECT load_extension('mod_spatialite');")
    return db


def _dump_gpkg_meta_info(db, layer):
    yield (
        'version',
        json.dumps({
            "version": "0.0.1"
        })
    )

    dbcur = db.cursor()
    table = layer

    QUERIES = {
        'gpkg_contents': (
            # we ignore dynamic fields (last-change, min_x, min_y, max_x, max_y)
            f'SELECT table_name, data_type, identifier, description, srs_id FROM gpkg_contents WHERE table_name=?;',
            (table,),
            dict
        ),
        'gpkg_geometry_columns': (
            f'SELECT table_name, column_name, geometry_type_name, srs_id, z, m FROM gpkg_geometry_columns WHERE table_name=?;',
            (table,),
            dict,
        ),
        'sqlite_table_info': (
            f'PRAGMA table_info({sqlite_ident(table)});',
            (),
            list,
        ),
        'gpkg_metadata_reference': ('''
            SELECT MR.*
            FROM gpkg_metadata_reference MR
                INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
            WHERE
                MR.table_name=?
                AND MR.column_name IS NULL
                AND MR.row_id_value IS NULL;
            ''',
            (table,),
            list,
        ),
        'gpkg_metadata': ('''
            SELECT M.*
            FROM gpkg_metadata_reference MR
                INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
            WHERE
                MR.table_name=?
                AND MR.column_name IS NULL
                AND MR.row_id_value IS NULL;
            ''',
            (table,),
            list,
        ),
        'gpkg_spatial_ref_sys': ('''
            SELECT DISTINCT SRS.*
            FROM gpkg_spatial_ref_sys SRS
                LEFT OUTER JOIN gpkg_contents C ON (C.srs_id = SRS.srs_id)
                LEFT OUTER JOIN gpkg_geometry_columns G ON (G.srs_id = SRS.srs_id)
            WHERE
                (C.table_name=? OR G.table_name=?)
            ''',
            (table, table),
            list,
        ),
    }
    try:
        for filename, (sql, params, rtype) in QUERIES.items():
            # check table exists, the metadata ones may not
            if not filename.startswith('sqlite_'):
                dbcur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (filename,))
                if not dbcur.fetchone():
                    continue

            dbcur.execute(sql, params)
            value = [collections.OrderedDict(sorted(zip(row.keys(), row))) for row in dbcur]
            if rtype is dict:
                value = value[0]
            yield (filename, json.dumps(value))
    except Exception:
        print(f"Error building meta/{filename}")
        raise


@cli.command('import-gpkg')
@click.pass_context
@click.argument('geopackage', type=click.Path(exists=True))
@click.argument('table')
def import_gpkg(ctx, geopackage, table):
    """ Import a Koordinates GeoPackage to a new repository """
    click.echo(f'Importing {geopackage} ...')

    repo_dir = ctx.obj['repo_dir']
    if os.path.exists(repo_dir):
        repo = pygit2.Repository(repo_dir)
        assert repo.is_bare, "Not a bare repository?!"

        assert not repo.is_empty, "Looks like you already have commits in this repository"
    else:
        if not repo_dir.endswith('.git'):
            raise click.BadParameter("Path should end in .git for now", param_hint="--repo")
        repo = pygit2.init_repository(repo_dir, bare=True)

    db = _get_db(geopackage)
    with db:
        dbcur = db.cursor()

        index = pygit2.Index()
        print("Writing meta bits...")
        for name, value in _dump_gpkg_meta_info(db, layer=table):
            blob_id = repo.create_blob(value.encode('utf8'))
            entry = pygit2.IndexEntry(f"{table}/meta/{name}", blob_id, pygit2.GIT_FILEMODE_BLOB)
            index.add(entry)

        dbcur.execute(f"SELECT COUNT(*) FROM {sqlite_ident(table)};")
        row_count = dbcur.fetchone()[0]
        print(f"Found {row_count} features in {table}")

        # iterate features
        t0 = time.time()
        dbcur.execute(f"SELECT * FROM {sqlite_ident(table)};")
        t1 = time.time()

        # Repo Structure
        # layer-name/
        #   meta/
        #     version
        #     schema
        #     geometry
        #   features/
        #     {uuid[:4]}/
        #       {uuid}/
        #         {field} => value
        #         ...
        #       ...
        #     ...

        print(f"Query ran in {t1-t0:.1f}s")
        for i, row in enumerate(dbcur):
            feature_id = str(uuid.uuid4())

            for field in row.keys():
                object_path = f"{table}/features/{feature_id[:4]}/{feature_id}/{field}"

                value = row[field]
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode('utf8')

                blob_id = repo.create_blob(value)
                entry = pygit2.IndexEntry(object_path, blob_id, pygit2.GIT_FILEMODE_BLOB)
                index.add(entry)
            #print(feature_id, object_path, field, value, entry)

            if i and i % 500 == 0:
                print(f"  {i+1} features... @{time.time()-t1:.1f}s")

        t2 = time.time()

        print(f"Added {i+1} Features to index in {t2-t1:.1f}s")
        print(f"Overall rate: {((i+1)/(t2-t0)):.0f} features/s)")

        print("Writing tree...")
        tree_id = index.write_tree(repo)
        del index
        t3 = time.time()
        print(f"Tree sha: {tree_id} (in {(t3-t2):.0f}s)")

        print("Committing...")
        user = repo.default_signature
        commit = repo.create_commit(
            'refs/heads/master',
            user,
            user,
            f"Import from {os.path.split(geopackage)[1]}",
            tree_id,
            []
        )
        t4 = time.time()
        print(f"Commit: {commit} (in {(t4-t3):.0f}s)")

        print(f"Garbage-collecting...")
        subprocess.check_call(['git', '-C', repo_dir, 'gc'])
        t5 = time.time()
        print(f"GC completed in {(t5-t4):.1f}s")


class WorkingCopy(typing.NamedTuple):
    path: str
    fmt: str
    layer: str


def _get_working_copy(repo):
    repo_cfg = repo.config
    if 'kx.workingcopy' in repo_cfg:
        fmt, path, layer = repo_cfg['kx.workingcopy'].split(':')
        if not os.path.isfile(path):
            raise FileNotFoundError(f"Working copy missing? {path}")
        return WorkingCopy(fmt=fmt, path=path, layer=layer)
    else:
        return None


@cli.command()
@click.pass_context
@click.option('branch', '-b', is_flag=True, help="Create a new branch")
@click.option('fmt', '--format', type=click.Choice(['GPKG']))
@click.option('layer', '--layer')
@click.option('--force', '-f', is_flag=True)
@click.option('--working-copy', type=click.Path(writable=True, dir_okay=False))
@click.argument('commitish', default=None, required=False)
def checkout(ctx, branch, commitish, working_copy, layer, force, fmt):
    repo_dir = ctx.obj['repo_dir']
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    base_commit = repo.head.peel(pygit2.Commit)

    if branch:
        commit = repo.revparse_single(commitish)
        if commitish in repo.branches:
            print(f"Creating new branch '{commitish}' to track '{commitish}'...")
            new_branch = repo.create_branch(branch, commit, force)
            new_branch.upstream = commitish
        else:
            print(f"Creating new branch '{branch}'...")
            new_branch = repo.create_branch(branch, commit, force)

        repo.set_head(new_branch.name)
    elif not commitish:  # HEAD
        commit = repo.head.peel(pygit2.Commit)
    else:
        try:
            if commitish in repo.branches.local:
                commit = repo.revparse_single(commitish)
                repo.set_head(repo.branches.local[commitish].name)
        except pygit2.InvalidSpecError:
            pass

        if commitish:
            commit = repo.revparse_single(commitish)
            repo.set_head(commit.id)

    wc = _get_working_copy(repo)
    if wc:
        if working_copy is not None:
            raise click.BadParameter(f"This repository already has a working copy at: {wc.path}", param_hint='WORKING_COPY')

        click.echo(f'Updating {wc.path} ...')
        return _checkout_update(repo, wc.path, wc.layer, commit, force=force, base_commit=base_commit)

    # new working-copy path
    if not working_copy:
        raise click.BadParameter("No existing working copy, specify --working-copy path", param_hint='--working-copy')
    if not layer:
        raise click.BadParameter("No existing working copy, specify layer", param_hint='--layer')

    if not fmt:
        fmt = 'GPKG'

    click.echo(f'Checkout {layer}@{commitish or "HEAD"} to {working_copy} as {fmt} ...')

    repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)

    _checkout_new(repo, working_copy, layer, commit, fmt)

    repo.config["kx.workingcopy"] = f"{fmt}:{working_copy}:{layer}"


def _feature_blobs_to_dict(repo, tree_entries, geom_column_name):
    o = {}
    for te in tree_entries:
        assert te.type == 'blob'

        blob = te.obj
        if te.name == geom_column_name:
            value = blob.data
        else:
            value = json.loads(blob.data)
        o[te.name] = value
    return o


def _diff_feature_to_dict(repo, diff_deltas, geom_column_name, select):
    o = {}
    for dd in diff_deltas:
        if select == 'old':
            df = dd.old_file
        elif select == 'new':
            df = dd.new_file
        else:
            raise ValueError("select should be 'old' or 'new'")

        blob = repo[df.id]
        assert isinstance(blob, pygit2.Blob)

        name = df.path.rsplit('/', 1)[-1]
        if name == geom_column_name:
            value = blob.data
        else:
            value = json.loads(blob.data)
        o[name] = value
    return o


@contextlib.contextmanager
def _suspend_triggers(db, table):
    """
    Context manager to suspend triggers (drop & recreate)
    Switches the DB into exclusive locking mode if it isn't already.
    Starts a transaction if we're not in one already
    """
    if not db.in_transaction:
        cm = db
    else:
        cm = contextlib.nullcontext()

    with cm:
        dbcur = db.cursor()
        dbcur.execute("PRAGMA locking_mode;")
        orig_locking = dbcur.fetchone()[0];

        if orig_locking.lower() != 'exclusive':
            dbcur.execute("PRAGMA locking_mode=EXCLUSIVE;")

        try:
            # if we error here just bail out, we're in a transaction anyway
            _drop_triggers(db, table)
            yield
            _create_triggers(db, table)
        finally:
            dbcur.execute(f"PRAGMA locking_mode={orig_locking};")
            # Simply setting the locking-mode to NORMAL is not enough
            # - locks are not released until the next time the database file is accessed.
            dbcur.execute(f"SELECT COUNT(*) FROM gpkg_contents;")


def _drop_triggers(dbcur, table):
    dbcur.execute(f"""
        DROP TRIGGER IF EXISTS {sqlite_ident(f"__kxg_{table}_ins")};
    """)
    dbcur.execute(f"""
        DROP TRIGGER IF EXISTS {sqlite_ident(f"__kxg_{table}_upd")};
    """)
    dbcur.execute(f"""
        DROP TRIGGER IF EXISTS {sqlite_ident(f"__kxg_{table}_del")};
    """)

def _create_triggers(dbcur, table):
    # sqlite doesn't let you do param substitutions in CREATE TRIGGER
    dbcur.execute(f"""
        CREATE TRIGGER {sqlite_ident(f"__kxg_{table}_ins")}
           AFTER INSERT
           ON {sqlite_ident(table)}
        BEGIN
            INSERT INTO __kxg_map (table_name, feature_key, feature_id, state)
                VALUES ({sqlite_param_str(table)}, NULL, NEW.fid, 1);
        END;
    """)
    dbcur.execute(f"""
        CREATE TRIGGER {sqlite_ident(f"__kxg_{table}_upd")}
           AFTER UPDATE
           ON {sqlite_ident(table)}
        BEGIN
            UPDATE __kxg_map
                SET state=1, feature_id=NEW.fid
                WHERE table_name={sqlite_param_str(table)}
                    AND feature_id=OLD.fid
                    AND state >= 0;
        END;
    """)
    dbcur.execute(f"""
        CREATE TRIGGER {sqlite_ident(f"__kxg_{table}_del")}
           AFTER DELETE
           ON {sqlite_ident(table)}
        BEGIN
            UPDATE __kxg_map
            SET state=-1
            WHERE table_name={sqlite_param_str(table)}
                AND feature_id=OLD.fid;
        END;
    """)


def _get_columns(meta_cols):
    pk_field = 'fid'
    cols = {}
    for col in meta_cols:
        col_spec = f"{sqlite_ident(col['name'])} {col['type']}"
        if col['pk']:
            col_spec += " PRIMARY KEY"
            pk_field = col['name']
        if col['notnull']:
            col_spec += " NOT NULL"
        cols[col['name']] = col_spec

    return cols, pk_field


OFTMap = {
    'INTEGER': ogr.OFTInteger,
    'MEDIUMINT': ogr.OFTInteger,
    'TEXT': ogr.OFTString,
    'REAL': ogr.OFTReal,
}

def _checkout_new(repo, working_copy, layer, commit, fmt):
    if fmt != "GPKG":
        raise NotImplementedError(fmt)

    repo.reset(commit.id, pygit2.GIT_RESET_SOFT)

    tree = commit.tree
    click.echo(f'Commit: {commit} Tree: {tree}')

    layer_tree = commit.tree / layer
    meta_tree = layer_tree / 'meta'
    meta_info = json.loads((meta_tree / 'gpkg_contents').obj.data)

    if meta_info['table_name'] != layer:
        assert False, f"Layer mismatch (table_name={meta_info['table_name']}; layer={layer}"
    table = layer

    meta_geom = json.loads((meta_tree / 'gpkg_geometry_columns').obj.data)
    meta_cols = json.loads((meta_tree / 'sqlite_table_info').obj.data)
    meta_srs = json.loads((meta_tree / 'gpkg_spatial_ref_sys').obj.data)
    geom_column_name = meta_geom['column_name']

    if 'gpkg_metadata' in meta_tree:
        meta_md = json.loads((meta_tree / 'gpkg_metadata').obj.data)
    else:
        meta_md = {}
    if 'gpkg_metadata_reference' in meta_tree:
        meta_md_ref = json.loads((meta_tree / 'gpkg_metadata_reference').obj.data)
    else:
        meta_md_ref = {}

    # GDAL: Create GeoPackage
    # GDAL: Add metadata/etc
    gdal_driver = gdal.GetDriverByName(fmt)
    gdal_ds = gdal_driver.Create(working_copy, 0, 0, 0, gdal.GDT_Unknown)
    del gdal_ds

    db = _get_db(working_copy, isolation_level='DEFERRED')
    db.execute("PRAGMA synchronous = OFF;")
    db.execute("PRAGMA locking_mode = EXCLUSIVE;")
    with db:
        dbcur = db.cursor()

        # Update GeoPackage core tables
        for o in meta_srs:
            keys, values = zip(*o.items())
            sql = f"INSERT OR REPLACE INTO gpkg_spatial_ref_sys ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
            db.execute(sql, values)

        keys, values = zip(*meta_info.items())
        # our repo copy doesn't include all fields from gpkg_contents
        # but the default value for last_change (now), and NULL for {min_x,max_x,min_y,max_y} should deal with the remaining fields
        sql = f"INSERT INTO gpkg_contents ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
        db.execute(sql, values)

        keys, values = zip(*meta_geom.items())
        sql = f"INSERT INTO gpkg_geometry_columns ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
        db.execute(sql, values)

        # Remove placeholder stuff GDAL creates
        db.execute("DELETE FROM gpkg_geometry_columns WHERE table_name='ogr_empty_table';")
        db.execute("DELETE FROM gpkg_contents WHERE table_name='ogr_empty_table';")
        db.execute("DROP TABLE IF EXISTS ogr_empty_table;")

        # Create metadata tables
        db.execute("""CREATE TABLE IF NOT EXISTS gpkg_metadata (
            id INTEGER CONSTRAINT m_pk PRIMARY KEY ASC NOT NULL,
            md_scope TEXT NOT NULL DEFAULT 'dataset',
            md_standard_uri TEXT NOT NULL,
            mime_type TEXT NOT NULL DEFAULT 'text/xml',
            metadata TEXT NOT NULL DEFAULT ''
        );
        """)
        db.execute("""CREATE TABLE IF NOT EXISTS gpkg_metadata_reference (
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
        # Populate metadata tables
        for o in meta_md:
            keys, values = zip(*o.items())
            sql = f"INSERT INTO gpkg_metadata ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
            db.execute(sql, values)

        for o in meta_md_ref:
            keys, values = zip(*o.items())
            sql = f"INSERT INTO gpkg_metadata_reference ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
            db.execute(sql, values)

        cols, pk_field = _get_columns(meta_cols)
        col_names = cols.keys()
        col_specs = cols.values()
        db.execute(f"CREATE TABLE {sqlite_ident(table)} ({', '.join(col_specs)});")

        db.execute(f"CREATE TABLE __kxg_map (table_name TEXT NOT NULL, feature_key VARCHAR(36) NULL, feature_id INTEGER NOT NULL, state INTEGER NOT NULL DEFAULT 0);")
        db.execute(f"CREATE TABLE __kxg_meta (table_name TEXT NOT NULL, key TEXT NOT NULL, value TEXT NULL);")

        db.execute("INSERT INTO __kxg_meta (table_name, key, value) VALUES (?, ?, ?);", (table, 'tree', tree.hex))

        click.echo("Creating features...")
        sql_insert_features = f"INSERT INTO {sqlite_ident(table)} ({','.join([sqlite_ident(k) for k in col_names])}) VALUES ({','.join(['?']*len(col_names))});"
        sql_insert_ids = "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);"
        feat_count = 0
        t0 = time.time()

        wip_features = []
        wip_idmap = []
        for te_ftree_prefix in (layer_tree / 'features').obj:
            if te_ftree_prefix.type != 'tree':
                continue
            ftree_prefix = te_ftree_prefix.obj

            for te_ftree in ftree_prefix:
                ftree = te_ftree.obj

                te_blobs = [te for te in ftree if te.type == 'blob']
                feature = _feature_blobs_to_dict(repo, te_blobs, geom_column_name)

                wip_features.append([feature[c] for c in col_names])
                wip_idmap.append([table, te_ftree.name, feature[pk_field]])
                feat_count += 1

                if len(wip_features) == 1000:
                    db.executemany(sql_insert_features, wip_features)
                    db.executemany(sql_insert_ids, wip_idmap)
                    print(f"  {feat_count} features... @{time.time()-t0:.1f}s")
                    wip_features = []
                    wip_idmap = []

        if len(wip_features):
            db.executemany(sql_insert_features, wip_features)
            db.executemany(sql_insert_ids, wip_idmap)
            print(f"  {feat_count} features... @{time.time()-t0:.1f}s")
            del wip_features
            del wip_idmap

        t1 = time.time()

        # Create triggers
        _create_triggers(db, table)

        # Update gpkg_contents
        # We do  spatial index built.
        commit_time = datetime.utcfromtimestamp(commit.commit_time)
        dbcur.execute(f"""
            UPDATE gpkg_contents
            SET
                min_x=NULL,
                min_y=NULL,
                max_x=NULL,
                max_y=NULL,
                last_change=?
            WHERE
                table_name=?;
            """,
            (
                commit_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),  # GPKG Spec Req.15
                table,
            )
        )
        assert dbcur.rowcount == 1, f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"

        db.execute("PRAGMA locking_mode = NORMAL;")

    print(f"Added {feat_count} Features to GPKG") # in {t1-t0:.1f}s")
    print(f"Overall rate: {(feat_count/(t1-t0)):.0f} features/s)")

    # Create the GeoPackage Spatial Index
    gdal_ds = gdal.OpenEx(working_copy, gdal.OF_VECTOR | gdal.OF_UPDATE | gdal.OF_VERBOSE_ERROR, ["GPKG"])
    gdal_ds.ExecuteSQL(f'SELECT CreateSpatialIndex({sqlite_ident(table)}, {sqlite_ident(meta_geom["column_name"])});')
    print(f"Created spatial index") # in {time.time()-t1:.1f}s")
    del gdal_ds

    # update the bounds
    dbcur.execute(f"""
        UPDATE gpkg_contents
        SET
            min_x=(SELECT ST_MinX({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)}),
            min_y=(SELECT ST_MinY({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)}),
            max_x=(SELECT ST_MaxX({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)}),
            max_y=(SELECT ST_MaxY({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)})
        WHERE
            table_name=?;
        """,
        (table,)
    )
    assert dbcur.rowcount == 1, f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"


def _checkout_update(repo, working_copy, layer, commit, force=False, base_commit=None):
    table = layer
    tree = commit.tree

    db = _get_db(working_copy, isolation_level='DEFERRED')
    db.execute("PRAGMA synchronous = OFF;")
    with db:
        dbcur = db.cursor()

        # check for dirty working copy
        dbcur.execute("SELECT COUNT(*) FROM __kxg_map WHERE state != 0;")
        is_dirty = dbcur.fetchone()[0]
        if is_dirty:
            if not force:
                raise click.Abort("You have uncommitted changes in your working copy. Commit or use --force to discard.")
            # force: delete changes
            dbcur.execute(f"DELETE FROM {sqlite_ident(table)} WHERE fid IN (SELECT feature_id FROM __kxg_map WHERE state != 0);")

        # this is where we're starting from
        if not base_commit:
            base_commit = repo.head.peel(pygit2.Commit)
        base_tree = base_commit.tree
        try:
            _assert_db_tree_match(db, table, base_tree.id)
        except WorkingCopyMismatch as e:
            if force:
                try:
                    # try and find the tree we _do_ have
                    base_tree = repo[e.working_copy_tree_id]
                    assert isinstance(base_tree, pygit2.Tree)
                    print(f"Warning: {e}")
                except ValueError:
                    raise e
            else:
                raise

        # check for schema differences
        # TODO: libgit2 supports pathspec, pygit2 doesn't
        base_meta_tree = (base_tree / layer / 'meta').obj
        meta_tree = (tree / layer / 'meta').obj
        if base_meta_tree.diff_to_tree(meta_tree):
            raise NotImplementedError("Sorry, no way to do changeset/meta/schema updates yet")

        meta_tree = commit.tree / layer / 'meta'
        meta_cols = json.loads((meta_tree / 'sqlite_table_info').obj.data)
        meta_geom = json.loads((meta_tree / 'gpkg_geometry_columns').obj.data)
        geom_column_name = meta_geom['column_name']

        cols, pk_field = _get_columns(meta_cols)
        col_names = cols.keys()

        sql_insert_feature = f"INSERT INTO {sqlite_ident(table)} ({','.join([sqlite_ident(k) for k in col_names])}) VALUES ({','.join(['?']*len(col_names))});"
        sql_insert_id = "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);"

        sql_delete_feature = f"DELETE FROM {sqlite_ident(table)} WHERE {sqlite_ident(pk_field)}=?;"
        sql_delete_id = f"DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?;"

        # feature diff
        # todo: suspend/remove spatial index
        with _suspend_triggers(db, table):
            base_index_tree = (base_tree / layer / 'features').obj
            index_tree = (tree / layer / 'features').obj
            diff_index = base_index_tree.diff_to_tree(index_tree)
            diff_index_list = list(diff_index.deltas)
            diff_index_list.sort(key=lambda d: (d.old_file.path, d.new_file.path))

            re_obj_a_path = re.compile(f'[0-9a-f]{{4}}/(?P<fk>[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}})/')
            re_obj_b_path = re.compile(f'{re.escape(layer)}/features/[0-9a-f]{{4}}/(?P<fk>[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}})/')
            def _get_feature_key_a(diff):
                m = re_obj_a_path.match(diff.old_file.path)
                assert m, f"Diff object path doesn't match expected path pattern? '{diff.old_file.path}'"
                return m.group('fk')
            def _get_feature_key_b(diff):
                m = re_obj_b_path.match(diff.new_file.path)
                assert m, f"Diff object path doesn't match expected path pattern? '{diff.new_file.path}'"
                return m.group('fk')

            def _filter_delta_status(*statuses):
                return filter(lambda d: d.status in statuses, diff_index_list)

            # deletes
            wip_features = []
            wip_idmap = []
            for feature_key, feature_diffs in itertools.groupby(_filter_delta_status(pygit2.GIT_DELTA_DELETED), _get_feature_key_a):
                feature = _diff_feature_to_dict(repo, feature_diffs, geom_column_name, select='old')
                wip_features.append((feature[pk_field],))
                wip_idmap.append((table, feature_key))

            if wip_features:
                dbcur.executemany(sql_delete_feature, wip_features)
                assert dbcur.rowcount == len(wip_features), f"checkout delete: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"
                dbcur.executemany(sql_delete_id, wip_idmap)
                assert dbcur.rowcount == len(wip_features), f"checkout delete-id: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"

            # updates
            for feature_key, feature_diffs in itertools.groupby(_filter_delta_status(pygit2.GIT_DELTA_MODIFIED), _get_feature_key_a):
                feature = _diff_feature_to_dict(repo, feature_diffs, geom_column_name, select='new')

                if feature:
                    sql_update_feature = f"""
                        UPDATE {sqlite_ident(table)}
                        SET {','.join([f'{sqlite_ident(k)}=?' for k in feature.keys()])}
                        WHERE {sqlite_ident(pk_field)}=(SELECT feature_id FROM __kxg_map WHERE table_name=? AND feature_key=?);
                    """
                    params = list(feature.values()) + [table, feature_key]
                    dbcur.execute(sql_update_feature, params)
                    assert dbcur.rowcount == 1, f"checkout update: expected Δ1, got {dbcur.rowcount}"

                    if 'fid' in feature:
                        # fid change
                        sql_update_id = f"UPDATE __kxg_map SET feature_id=? WHERE table_name=? AND feature_key=?;"
                        dbcur.execute(sql_update_id, (feature[pk_field], table, feature_key))
                        assert dbcur.rowcount == 1, f"checkout update-id: expected Δ1, got {dbcur.rowcount}"

            # adds/inserts
            wip_features = []
            wip_idmap = []
            for feature_key, feature_diffs in itertools.groupby(_filter_delta_status(pygit2.GIT_DELTA_ADDED), _get_feature_key_b):
                feature = _diff_feature_to_dict(repo, feature_diffs, geom_column_name, select='new')
                wip_features.append([feature[c] for c in col_names])
                wip_idmap.append((table, feature_key, feature[pk_field]))

            if wip_features:
                dbcur.executemany(sql_insert_feature, wip_features)
                dbcur.executemany(sql_insert_id, wip_idmap)

            # unexpected things
            unsupported_deltas = _filter_delta_status(
                pygit2.GIT_DELTA_COPIED,
                pygit2.GIT_DELTA_IGNORED,
                pygit2.GIT_DELTA_RENAMED,
                pygit2.GIT_DELTA_TYPECHANGE,
                pygit2.GIT_DELTA_UNMODIFIED,
                pygit2.GIT_DELTA_UNREADABLE,
                pygit2.GIT_DELTA_UNTRACKED,
            )
            if any(unsupported_deltas):
                raise NotImplementedError("Deltas for unsupported diff states:\n" + diff_index.stats.format(pygit2.GIT_DIFF_STATS_FULL | pygit2.GIT_DIFF_STATS_INCLUDE_SUMMARY, 80))

            # Update gpkg_contents
            commit_time = datetime.utcfromtimestamp(commit.commit_time)
            dbcur.execute(f"""
                UPDATE gpkg_contents
                SET
                    last_change=?,
                    min_x=(SELECT ST_MinX({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)}),
                    min_y=(SELECT ST_MinY({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)}),
                    max_x=(SELECT ST_MaxX({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)}),
                    max_y=(SELECT ST_MaxY({sqlite_ident(geom_column_name)}) FROM {sqlite_ident(table)})
                WHERE
                    table_name=?;
                """,
                (
                    commit_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),  # GPKG Spec Req.15
                    table,
                )
            )
            assert dbcur.rowcount == 1, f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"

            # update the tree id
            db.execute("UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';", (tree.hex, table))

            repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)


def _gpkg_geom_to_ogr(gpkg_geom, parse_srs=False):
    """
    Parse GeoPackage geometry values to an OGR Geometry object
    http://www.geopackage.org/spec/#gpb_format
    """
    if gpkg_geom is None:
        return None

    if not isinstance(gpkg_geom, bytes):
        raise TypeError("Expected bytes")

    if gpkg_geom[0:2] != b'GP':  # 0x4750
        raise ValueError("Expected GeoPackage Binary Geometry")
    (version, flags) = struct.unpack_from('BB', gpkg_geom, 2)
    if version != 0:
        raise NotImplementedError("Expected GeoPackage v1 geometry, got %d", version)

    is_le = (flags & 0b0000001) != 0  # Endian-ness

    if flags & (0b00100000):  # GeoPackageBinary type
        raise NotImplementedError("ExtendedGeoPackageBinary")

    envelope_typ = (flags & 0b000001110) >> 1
    wkb_offset = 8
    if envelope_typ == 1:
        wkb_offset += 32
    elif envelope_typ in (2, 3):
        wkb_offset += 48
    elif envelope_typ == 4:
        wkb_offset += 64
    elif envelope_typ > 4:
        wkb_offset += 32
    else:  # 0
        pass

    geom = ogr.CreateGeometryFromWkb(gpkg_geom[wkb_offset:])

    if parse_srs:
        srid = struct.unpack_from(f"{'<' if is_le else '>'}i", gpkg_geom, 4)[0]
        if srid > 0:
            srs = osr.SpatialReference()
            srs.ImportFromEPSG(srid)
            geom.AssignSpatialReference(srs)

    return geom


def _repr_row(row, prefix=''):
    m = []
    for k in row.keys():
        if k.startswith("__"):
            continue

        v = row[k]

        if isinstance(v, bytes):
            g = _gpkg_geom_to_ogr(v)
            v = f"{g.GetGeometryName()}(...)"
            del g

        v = "␀" if v is None else v
        m.append("{prefix}{k:>40} = {v}".format(k=k, v=v, prefix=prefix))

    return "\n".join(m)


def _build_db_diff(repo, layer, db, tree=None):
    """ Generates a diff between a working copy DB and the underlying repository tree """
    table = layer
    dbcur = db.cursor()

    if not tree:
        dbcur.execute("SELECT value FROM __kxg_meta WHERE table_name=? AND key=?;", (table, 'tree'))
        tree = repo[dbcur.fetchone()[0]]
        assert tree.type == pygit2.GIT_OBJ_TREE, tree.type

    layer_tree = (tree / layer)
    meta_tree = (layer_tree / 'meta')

    meta_diff = {}
    for name, mv_new in _dump_gpkg_meta_info(db, layer):
        if name in meta_tree:
            mv_old = json.loads(repo[(meta_tree / name).id].data)
        else:
            mv_old = []
        mv_new = json.loads(mv_new)
        if mv_old != mv_new:
            meta_diff[name] = (mv_old, mv_new)

    meta_geom = json.loads((meta_tree / 'gpkg_geometry_columns').obj.data)

    candidates = {'I': [], 'U': {}, 'D': {}}

    diff_sql = f"""
        SELECT M.feature_key AS __fk, M.state AS __s, M.feature_id AS __fid, T.*
        FROM __kxg_map AS M
            LEFT OUTER JOIN {sqlite_ident(table)} AS T
            ON (M.feature_id = T.fid)
        WHERE
            M.table_name = ?
            AND M.state != 0
            AND NOT (M.feature_key IS NULL AND M.state < 0)  -- ignore INSERT then DELETE
        ORDER BY M.feature_key;
    """
    for row in dbcur.execute(diff_sql, (table,)):
        o = {k: row[k] for k in row.keys() if not k.startswith('__')}
        if row["__s"] < 0:
            candidates['D'][row['__fk']] = o
        elif row['__fk'] is None:
            candidates['I'].append(o)
        else:
            candidates['U'][row['__fk']] = o

    results = {
        'META': meta_diff,
        'I': candidates['I'],
        'D': candidates['D'],
        'U': {},
    }

    features_tree = (tree / layer / 'features')
    for feature_key, db_obj in candidates['U'].items():
        ftree = (features_tree / feature_key[:4] / feature_key).obj
        assert ftree.type == pygit2.GIT_OBJ_TREE

        repo_obj = _feature_blobs_to_dict(
            repo=repo,
            tree_entries=ftree,
            geom_column_name=meta_geom['column_name'],
        )

        s_old = set(repo_obj.items())
        s_new = set(db_obj.items())

        if s_old ^ s_new:
            results['U'][feature_key] = (repo_obj, db_obj)

    return results


@cli.command()
@click.pass_context
def diff(ctx):
    repo_dir = ctx.obj['repo_dir']
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    working_copy = _get_working_copy(repo)
    assert working_copy, f"No working copy? Try `kxgit checkout`"

    db = _get_db(working_copy.path, isolation_level='DEFERRED')
    with db:
        head_tree = repo.head.peel(pygit2.Tree)
        _assert_db_tree_match(db, working_copy.layer, head_tree)
        diff = _build_db_diff(repo, working_copy.layer, db)

    for k, (v_old, v_new) in diff['META'].items():
        click.secho(f"--- meta/{k}\n+++ meta/{k}", bold=True)

        s_old = set(v_old.items())
        s_new = set(v_new.items())

        diff_add = dict(s_new - s_old)
        diff_del = dict(s_old - s_new)
        all_keys = set(diff_del.keys()) | set(diff_add.keys())

        for k in all_keys:
            if k in diff_del:
                click.secho(_repr_row({k: diff_del[k]}, prefix='- '), fg='red')
            if k in diff_add:
                click.secho(_repr_row({k: diff_add[k]}, prefix='+ '), fg='green')

    for k, o in diff['D'].items():
        click.secho(f"--- {k}", bold=True)
        click.secho(_repr_row(o, prefix='- '), fg='red')

    for o in diff['I']:
        click.secho("+++ {new feature}", bold=True)
        click.secho(_repr_row(o, prefix='+ '), fg='green')

    for feature_key, (v_old, v_new) in diff['U'].items():
        click.secho(f"--- {feature_key}\n+++ {feature_key}", bold=True)

        s_old = set(v_old.items())
        s_new = set(v_new.items())

        diff_add = dict(s_new - s_old)
        diff_del = dict(s_old - s_new)
        all_keys = set(diff_del.keys()) | set(diff_add.keys())

        if 'fid' not in all_keys:
            click.echo(_repr_row({'fid': v_new['fid']}, prefix='  '))

        for k in all_keys:
            if k in diff_del:
                click.secho(_repr_row({k: diff_del[k]}, prefix='- '), fg='red')
            if k in diff_add:
                click.secho(_repr_row({k: diff_add[k]}, prefix='+ '), fg='green')


class WorkingCopyMismatch(ValueError):
    def __init__(self, working_copy_tree_id, match_tree_id):
        self.working_copy_tree_id = working_copy_tree_id
        self.match_tree_id = match_tree_id

    def __str__(self):
        return f"Working Copy is tree {self.working_copy_tree_id}; expecting {self.match_tree_id}"


def _assert_db_tree_match(db, table, tree):
    dbcur = db.cursor()
    dbcur.execute("SELECT value FROM __kxg_meta WHERE table_name=? AND key=?;", (table, 'tree'))
    wc_tree_id = dbcur.fetchone()[0]

    tree_sha = tree.hex

    if (wc_tree_id != tree_sha):
        raise WorkingCopyMismatch(wc_tree_id, tree_sha)
    return wc_tree_id


@cli.command()
@click.pass_context
@click.option('--message', '-m', required=True)
def commit(ctx, message):
    repo_dir = ctx.obj['repo_dir']
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')
    commit = repo.head.peel(pygit2.Commit)
    tree = commit.tree

    if 'kx.workingcopy' not in repo.config:
        click.UsageError("No working-copy, use 'checkout'")

    fmt, working_copy, layer = repo.config["kx.workingcopy"].split(':')
    assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

    table = layer

    db = _get_db(working_copy, isolation_level='DEFERRED')
    with db:
        _assert_db_tree_match(db, table, tree)

        diff = _build_db_diff(repo, layer, db)
        if not any(diff.values()):
            print("No changes to commit")
            return

        dbcur = db.cursor()

        git_index = pygit2.Index()
        git_index.read_tree(tree)

        for k, (obj_old, obj_new) in diff['META'].items():
            object_path = f"{layer}/meta/{k}"
            value = json.dumps(obj_new).encode('utf8')

            blob = repo.create_blob(value)
            idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
            git_index.add(idx_entry)
            click.secho(f"Δ {object_path}", fg='yellow')

        for feature_key in diff['D'].keys():
            object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}"
            git_index.remove(object_path)
            click.secho(f"- {object_path}", fg='red')

            dbcur.execute("DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?", (table, feature_key))
            assert dbcur.rowcount == 1, f"__kxg_map delete: expected 1Δ, got {dbcur.rowcount}"

        for obj in diff['I']:
            feature_key = str(uuid.uuid4())
            for k, value in obj.items():
                object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}/{k}"
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode('utf8')

                blob = repo.create_blob(value)
                idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
                git_index.add(idx_entry)
                click.secho(f"+ {object_path}", fg='green')

            dbcur.execute("INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);", (table, feature_key, obj['fid']))
        dbcur.execute("DELETE FROM __kxg_map WHERE table_name=? AND feature_key IS NULL;", (table,))

        for feature_key, (obj_old, obj_new) in diff['U'].items():
            s_old = set(obj_old.items())
            s_new = set(obj_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}/{k}"
                if k in diff_add:
                    value = obj_new[k]
                    if not isinstance(value, bytes):  # blob
                        value = json.dumps(value).encode('utf8')

                    blob = repo.create_blob(value)
                    idx_entry = pygit2.IndexEntry(object_path, blob, pygit2.GIT_FILEMODE_BLOB)
                    git_index.add(idx_entry)
                    click.secho(f"Δ {object_path}", fg='yellow')
                else:
                    git_index.remove(object_path)
                    click.secho(f"- {object_path}", fg='red')

        dbcur.execute("UPDATE __kxg_map SET state=0 WHERE table_name=? AND state != 0;", (table,))

        print("Writing tree...")
        new_tree = git_index.write_tree(repo)
        print(f"Tree sha: {new_tree}")

        dbcur.execute("UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';", (str(new_tree), table))
        assert dbcur.rowcount == 1, f"__kxg_meta update: expected 1Δ, got {dbcur.rowcount}"

        print("Committing...")
        user = repo.default_signature
        # this will also update the ref (branch) to point to the current commit
        new_commit = repo.create_commit(
            "HEAD",             # reference_name
            user,               # author
            user,               # committer
            message,            # message
            new_tree,           # tree
            [repo.head.target]  # parents
        )
        print(f"Commit: {new_commit}")

        #TODO: update reflog


@cli.command(help=(
    "Incorporates changes from the named commits (usually other branch heads) into the current branch."
))
@click.option('--ff/--no-ff', default=True, help=(
    "When the merge resolves as a fast-forward, only update the branch pointer, without creating a merge commit. "
    "With --no-ff create a merge commit even when the merge resolves as a fast-forward."
))
@click.option('--ff-only', default=False, is_flag=True, help=(
    "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date or the merge can be resolved as a fast-forward."
))
@click.argument('commits', nargs=-1, required=True, metavar='COMMIT...')
@click.pass_context
def merge(ctx, ff, ff_only, commits):
    repo_dir = ctx.obj['repo_dir']
    repo = pygit2.Repository(repo_dir)

    if len(commits) != 1:
        raise NotImplementedError("Only one merge source currently supported")

    if ff_only and not ff:
        raise click.BadParameter("Conflicting parameters: --no-ff & --ff-only", param_hint='--ff-only')

    c_base = repo[repo.head.target]

    # accept ref-ish things (refspec, branch, commit)
    r_head = None
    if pygit2.reference_is_valid_name(commits[0]):
        try:
            r_head = repo.lookup_reference(commits[0])
        except KeyError:
            pass
    if not r_head:
        r_head = repo.lookup_branch(commits[0], pygit2.GIT_BRANCH_LOCAL)
    if not r_head:
        r_head = repo.lookup_branch(commits[0], pygit2.GIT_BRANCH_REMOTE)

    if r_head:
        c_head = r_head.peel(pygit2.Commit)
    else:
        c_head = repo.revparse_single(commits[0])

    print(f"Merging {c_head.id} to {c_base.id} ...")
    merge_base = repo.merge_base(c_base.oid, c_head.oid)
    print(f"Found merge base: {merge_base}")

    # We're up-to-date if we're trying to merge our own common ancestor.
    if merge_base == c_head.oid:
        print("Already merged!")
        return

    # We're fastforwardable if we're our own common ancestor.
    can_ff = (merge_base == c_base.id)

    if ff_only and not can_ff:
        print("Can't resolve as a fast-forward merge and --ff-only specified")
        ctx.exit(1)

    if can_ff and ff:
        # do fast-forward merge
        repo.head.set_target(c_head.id, "merge: Fast-forward")
        commit_id = c_head.id
        print("Fast-forward")
    else:
        ancestor_tree = repo[merge_base].tree

        merge_index = repo.merge_trees(
            ancestor=ancestor_tree,
            ours=c_base.tree,
            theirs=c_head.tree,
        )
        if merge_index.conflicts:
            print("Merge conflicts!")
            for path, (ancestor, ours, theirs) in merge_index.conflicts:
                print(f"Conflict: {path:60} {ancestor} | {ours} | {theirs}")
            ctx.exit(1)

        print("No conflicts!")
        merge_tree_id = merge_index.write_tree(repo)
        print(f"Merge tree: {merge_tree_id}")

        user = repo.default_signature
        merge_message = "Merge '{}'".format(r_head.shorthand if r_head else c_head.id)
        commit_id = repo.create_commit(
            repo.head.name,
            user,
            user,
            merge_message,
            merge_tree_id,
            [c_base.oid, c_head.oid],
        )
        print(f"Merge commit: {commit_id}")

    # update our working copy
    wc = _get_working_copy(repo)
    click.echo(f'Updating {wc.path} ...')
    return _checkout_update(repo, wc.path, wc.layer, commit_id, base_commit=c_base.id)


# straight process-replace commands

@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def log(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    _execvp("git", ["git", "-C", repo_dir, "log"] + list(args))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def push(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    _execvp("git", ["git", "-C", repo_dir, "push"] + list(args))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def fetch(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    _execvp("git", ["git", "-C", repo_dir, "fetch"] + list(args))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def branch(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    _execvp("git", ["git", "-C", repo_dir, "branch"] + list(args))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.argument('repository', nargs=1)
@click.argument('directory', required=False)
def clone(repository, directory):
    repo_dir = directory or os.path.split(repository)[1]
    if not repo_dir.endswith('.git') or len(repo_dir) == 4:
        raise click.BadParameter("Repository should be foo.git")

    subprocess.check_call(["git", "clone", "--bare", repository, repo_dir])
    subprocess.check_call(["git", "-C", repo_dir, "config", "--local", "--add", "remote.origin.fetch", "+refs/heads/*:refs/remotes/origin/*"])
    subprocess.check_call(["git", "-C", repo_dir, "fetch"])

    repo = pygit2.Repository(repo_dir)
    head_ref = repo.head.shorthand  # master
    subprocess.check_call(['git', "-C", repo_dir, 'config', "--local", f"branch.{head_ref}.remote", "origin"])
    subprocess.check_call(['git', "-C", repo_dir, 'config', "--local", f"branch.{head_ref}.merge", "refs/heads/master"])


if __name__ == "__main__":
    cli()
