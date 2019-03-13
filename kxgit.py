#!/usr/bin/env python3
import collections
import contextlib
import io
import itertools
import json
import os
import re
import sqlite3
import time
import uuid
from datetime import timezone

import click
import git
import gitdb
import pygit2
from osgeo import gdal, ogr, osr


gdal.UseExceptions()

repo_params = {
    'odbt': git.GitCmdObjectDB,
}


def print_version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    import osgeo

    click.echo("kxgit proof of concept")
    click.echo(f"GDAL v{osgeo._gdal.__version__}")
    click.echo(f"PyGit2 v{pygit2.__version__}; Libgit2 v{pygit2.LIBGIT2_VERSION}")
    click.echo(f"GitPython v{git.__version__}")
    ctx.exit()


@click.group()
@click.option('repo_dir', '--repo', type=click.Path(file_okay=False, dir_okay=True), default=os.curdir, metavar="PATH")
@click.option('--version', is_flag=True, callback=print_version, expose_value=False, is_eager=True, help='Show version information and exit.')
@click.pass_context
def cli(ctx, repo_dir):
    ctx.ensure_object(dict)
    ctx.obj['repo_dir'] = repo_dir


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
    click.echo(f'Importing {geopackage} ...')

    repo_dir = ctx.obj['repo_dir']
    if os.path.exists(repo_dir):
        repo = git.Repo(repo_dir, **repo_params)
        assert repo.bare, "Not a bare repository?!"

        assert not repo.heads, "Looks like you already have commits in this repository"
    else:
        if not repo_dir.endswith('.git'):
            raise click.BadParameter("Path should end in .git", param_hint="--repo")
        repo = git.Repo.init(repo_dir, bare=True, **repo_params)

    db = _get_db(geopackage)
    with db:
        dbcur = db.cursor()

        index_entries = []
        print("Writing meta bits...")
        for name, value in _dump_gpkg_meta_info(db, layer=table):
            istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value.encode('utf8'))))
            file_mode = 0o100644
            entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, f"{table}/meta/{name}"))
            index_entries.append(entry)

        git_index = repo.index
        git_index.add(index_entries, write=False)

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
        index_entries = []
        for i, row in enumerate(dbcur):
            feature_id = str(uuid.uuid4())

            for field in row.keys():
                object_path = f"{table}/features/{feature_id[:4]}/{feature_id}/{field}"

                value = row[field]
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode('utf8')

                istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value)))
                file_mode = 0o100644
                entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, object_path))
                index_entries.append(entry)
            print(feature_id, object_path, field, value, entry)

            if i and i % 500 == 0:
                print(f"  {i+1} features... @{time.time()-t1:.1f}s")

        t2 = time.time()
        print("Adding to index...")
        git_index.add(index_entries, write=True)
        t3 = time.time()

        print(f"Added {i+1} Features to index in {t3-t2:.1f}s")
        print(f"Overall rate: {((i+1)/(t3-t0)):.0f} features/s)")

        print("Writing tree...")
        tree_sha = git_index.write_tree()
        print(f"Tree sha: {tree_sha}")

        print("Committing...")
        commit = repo.index.commit(f"Import from {os.path.split(geopackage)[1]}")
        print(f"Commit: {commit}")

        master_ref = repo.create_head('master', commit)
        repo.head.set_reference(master_ref)

        print(f"Garbage-collecting...")
        t4 = time.time()
        repo.git.gc()
        print(f"GC completed in {time.time()-t4:.1f}s")



@cli.command()
@click.pass_context
@click.option('fmt', '--format', type=click.Choice(['GPKG']))
@click.option('layer', '--layer')
@click.option('--force', '-f', is_flag=True)
@click.option('--working-copy', type=click.Path(writable=True, dir_okay=False))
@click.argument('commitish', default=None, required=False)
def checkout(ctx, commitish, working_copy, layer, force, fmt):
    repo_dir = ctx.obj['repo_dir']
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    if commitish:
        commit = repo.commit(commitish)
    else:
        commit = repo.head.commit

    repo_cfg = repo.config_reader('repository')
    if repo_cfg.has_option('kx', 'workingcopy'):
        if working_copy is not None:
            raise click.BadParameter(f"This repository already has a working copy at: {repo_cfg.get('kx', 'workingcopy')}", param_hint='WORKING_COPY')
        fmt, working_copy, layer = repo_cfg.get('kx', 'workingcopy').split(':')

        assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

        click.echo(f'Updating {working_copy} ...')
        return _checkout_update(repo, working_copy, layer, commit, force=force)

    # new working-copy path
    if not working_copy:
        raise click.BadParameter("No existing working copy, specify --working-copy path", param_hint='--working-copy')
    if not layer:
        raise click.BadParameter("No existing working copy, specify layer", param_hint='--layer')

    if not fmt:
        fmt = 'GPKG'

    click.echo(f'Checkout {layer}@{commitish or "HEAD"} to {working_copy} as {fmt} ...')

    _checkout_new(repo, working_copy, layer, commit, fmt)

    repo_cfg = repo.config_writer()
    if not repo_cfg.has_section("kx"):
        repo_cfg.add_section("kx")
    repo_cfg.set("kx", "workingcopy", f"{fmt}:{working_copy}:{layer}")
    repo_cfg.write()
    del repo_cfg


def _feature_blobs_to_dict(blobs, geom_column_name):
    o = {}
    for blob in blobs:
        if blob.name == geom_column_name:
            value = blob.data_stream.read()
            assert value[:2] == b'GP', "Not a standard GeoPackage geometry"
        else:
            value = json.load(blob.data_stream)
        o[blob.name] = value
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

        if orig_locking.lower() != 'normal':
            dbcur.execute("PRAGMA locking_mode=EXCLUSIVE;")

        try:
            # if we error here just bail out, we're in a transaction anyway
            _drop_triggers(db, table)
            yield
            _create_triggers(db, table)
        finally:
            dbcur.execute(f"PRAGMA locking_mode={orig_locking};")


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

    repo.head.reset(commit=commit, working_tree=False, index=False)

    commit = repo.head.commit
    tree = commit.tree
    click.echo(f'Commit: {commit} Tree: {tree}')

    layer_tree = commit.tree / layer
    meta_tree = layer_tree / 'meta'
    meta_info = json.load((meta_tree / 'gpkg_contents').data_stream)

    if meta_info['table_name'] != layer:
        assert False, f"Layer mismatch (table_name={meta_info['table_name']}; layer={layer}"
    table = layer

    meta_geom = json.load((meta_tree / 'gpkg_geometry_columns').data_stream)
    meta_cols = json.load((meta_tree / 'sqlite_table_info').data_stream)
    meta_md = json.load((meta_tree / 'gpkg_metadata').data_stream)
    meta_md_ref = json.load((meta_tree / 'gpkg_metadata_reference').data_stream)
    meta_srs = json.load((meta_tree / 'gpkg_spatial_ref_sys').data_stream)
    geom_column_name = meta_geom['column_name']

    # GDAL: Create GeoPackage
    # GDAL: Add metadata/etc
    gdal_driver = gdal.GetDriverByName(fmt)
    gdal_ds = gdal_driver.Create(working_copy, 0, 0, 0, gdal.GDT_Unknown)
    del gdal_ds

    db = _get_db(working_copy, isolation_level='DEFERRED')
    db.execute("PRAGMA synchronous = OFF;")
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

        db.execute("INSERT INTO __kxg_meta (table_name, key, value) VALUES (?, ?, ?);", (table, 'tree', str(tree)))

        click.echo("Creating features...")
        sql_insert_features = f"INSERT INTO {sqlite_ident(table)} ({','.join([sqlite_ident(k) for k in col_names])}) VALUES ({','.join(['?']*len(col_names))});"
        sql_insert_ids = "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);"
        feat_count = 0
        t0 = time.time()

        wip_features = []
        wip_idmap = []
        for ftree_prefix in (layer_tree / 'features').trees:
            for ftree in ftree_prefix.trees:
                feature = _feature_blobs_to_dict(ftree.blobs, geom_column_name)

                wip_features.append([feature[c] for c in col_names])
                wip_idmap.append([table, ftree.name, feature[pk_field]])
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
                commit.committed_datetime.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),  # GPKG Spec Req.15
                table,
            )
        )
        assert dbcur.rowcount == 1, f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"

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


def _checkout_update(repo, working_copy, layer, commit, force=False):
    table = layer
    tree = commit.tree

    db = _get_db(working_copy, isolation_level='DEFERRED')
    db.execute("PRAGMA synchronous = OFF;")
    with db:
        dbcur = db.cursor()
        #dbcur.execute("PRAGMA locking_mode = EXCLUSIVE;")

        # check for dirty working copy
        dbcur.execute("SELECT COUNT(*) FROM __kxg_map WHERE state != 0;")
        is_dirty = dbcur.fetchone()[0]
        if is_dirty:
            if not force:
                raise click.Abort("You have uncommitted changes in your working copy. Commit or use --force to discard.")
            # force: delete changes
            dbcur.execute(f"DELETE FROM {sqlite_ident(table)} WHERE fid IN (SELECT feature_id FROM __kxg_map WHERE state != 0);")

        # this is where we're starting from
        base_commit = repo.head.commit
        base_tree = base_commit.tree
        _assert_db_tree_match(db, table, base_tree)

        # check for schema differences
        if base_tree.diff(tree, paths=f"{layer}/meta"):
            raise NotImplementedError("Sorry, no way to do changeset/meta/schema updates yet")

        meta_tree = commit.tree / layer / 'meta'
        meta_cols = json.load((meta_tree / 'sqlite_table_info').data_stream)
        meta_geom = json.load((meta_tree / 'gpkg_geometry_columns').data_stream)
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
            diff_index = base_tree.diff(tree, paths=f"{layer}/features")
            diff_index.sort(key=lambda d: (d.a_path, d.b_path))

            re_obj_path = re.compile(f'{re.escape(layer)}/features/[0-9a-f]{{4}}/(?P<fk>[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}})/')
            def _get_feature_key_a(diff):
                m = re_obj_path.match(diff.a_path)
                assert m, f"Diff object path doesn't match expected path pattern? '{diff.a_path}'"
                return m.group('fk')
            def _get_feature_key_b(diff):
                m = re_obj_path.match(diff.b_path)
                assert m, f"Diff object path doesn't match expected path pattern? '{diff.b_path}'"
                return m.group('fk')

            # deletes
            wip_features = []
            wip_idmap = []
            for feature_key, feature_diffs in itertools.groupby(diff_index.iter_change_type('D'), _get_feature_key_a):  # D=delete
                a_blobs = [diff.a_blob for diff in feature_diffs]
                feature = _feature_blobs_to_dict(a_blobs, geom_column_name)
                wip_features.append((feature[pk_field],))
                wip_idmap.append((table, feature_key))

            if wip_features:
                dbcur.executemany(sql_delete_feature, wip_features)
                assert dbcur.rowcount == len(wip_features), f"checkout delete: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"
                dbcur.executemany(sql_delete_id, wip_idmap)
                assert dbcur.rowcount == len(wip_features), f"checkout delete-id: expected Δ{len(wip_features)} changes, got {dbcur.rowcount}"

            # updates
            for feature_key, feature_diffs in itertools.groupby(diff_index.iter_change_type('M'), _get_feature_key_a):  # M=modified
                b_blobs = [diff.b_blob for diff in feature_diffs]
                b_feature = _feature_blobs_to_dict(b_blobs, geom_column_name)

                if b_feature:
                    sql_update_feature = f"""
                        UPDATE {sqlite_ident(table)}
                        SET {','.join([f'{sqlite_ident(k)}=?' for k in b_feature.keys()])}
                        WHERE {sqlite_ident(pk_field)}=(SELECT feature_id FROM __kxg_map WHERE table_name=? AND feature_key=?);
                    """
                    params = list(b_feature.values()) + [table, feature_key]
                    dbcur.execute(sql_update_feature, params)
                    assert dbcur.rowcount == 1, f"checkout update: expected Δ1, got {dbcur.rowcount}"

                    if 'fid' in b_feature:
                        # fid change
                        sql_update_id = f"UPDATE __kxg_map SET feature_id=? WHERE table_name=? AND feature_key=?;"
                        dbcur.execute(sql_update_id, (b_feature[pk_field], table, feature_key))
                        assert dbcur.rowcount == 1, f"checkout update-id: expected Δ1, got {dbcur.rowcount}"

            # adds/inserts
            wip_features = []
            wip_idmap = []
            for feature_key, feature_diffs in itertools.groupby(diff_index.iter_change_type('A'), _get_feature_key_b):  # A=add
                b_blobs = [diff.b_blob for diff in feature_diffs]
                feature = _feature_blobs_to_dict(b_blobs, geom_column_name)
                wip_features.append([feature[c] for c in col_names])
                wip_idmap.append((table, feature_key, feature[pk_field]))

            if wip_features:
                dbcur.executemany(sql_insert_feature, wip_features)
                dbcur.executemany(sql_insert_id, wip_idmap)

            # Update gpkg_contents
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
                    commit.committed_datetime.astimezone(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.%fZ'),  # GPKG Spec Req.15
                    table,
                )
            )
            assert dbcur.rowcount == 1, f"gpkg_contents update: expected 1Δ, got {dbcur.rowcount}"

            # update the tree id
            db.execute("UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';", (str(tree), table))

            repo.head.reset(commit=commit, working_tree=False, index=False)


def _repr_row(row, prefix=''):
    m = []
    for k in row.keys():
        if k.startswith("__"):
            continue

        v = row[k]

        if isinstance(v, bytes) and v[:2] == b'GP':
            g = ogr.CreateGeometryFromWkb(v[40:])  # FIXME
            v = f"{g.GetGeometryName()}(...)"
            del g

        v = "NULL" if v is None else v
        m.append("{prefix}{k:>40} = {v}".format(k=k, v=v, prefix=prefix))

    return "\n".join(m)


def _build_db_diff(repo, layer, db, tree=None):
    """ Generates a diff between a working copy DB and the underlying repository tree """
    table = layer
    dbcur = db.cursor()

    if not tree:
        dbcur.execute("SELECT value FROM __kxg_meta WHERE table_name=? AND key=?;", (table, 'tree'))
        tree = repo.tree(dbcur.fetchone()[0])
        tree.path = ''  # HACK https://github.com/gitpython-developers/GitPython/issues/759

    layer_tree = (tree / layer)
    meta_tree = (layer_tree / 'meta')

    meta_diff = {}
    for name, mv_new in _dump_gpkg_meta_info(db, layer):
        mv_old = json.load((meta_tree / name).data_stream)
        mv_new = json.loads(mv_new)
        if mv_old != mv_new:
            meta_diff[name] = (mv_old, mv_new)

    meta_geom = json.load((meta_tree / 'gpkg_geometry_columns').data_stream)

    candidates = {'I':[],'U':{},'D':{}}

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
        o = {k:row[k] for k in row.keys() if not k.startswith('__')}
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
        ftree = (features_tree / feature_key[:4] / feature_key)
        repo_obj = _feature_blobs_to_dict(ftree.blobs, meta_geom['column_name'])

        s_old = set(repo_obj.items())
        s_new = set(db_obj.items())

        if s_old ^ s_new:
            results['U'][feature_key] = (repo_obj, db_obj)

    return results


@cli.command()
@click.pass_context
def diff(ctx):
    repo_dir = ctx.obj['repo_dir']
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    repo_cfg = repo.config_reader('repository')
    if not repo_cfg.has_option('kx', 'workingcopy'):
        click.UsageError("No working-copy, use 'checkout'")

    fmt, working_copy, layer = repo_cfg.get('kx', 'workingcopy').split(':')
    assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

    db = _get_db(working_copy, isolation_level='DEFERRED')
    with db:
        _assert_db_tree_match(db, layer, repo.head.commit.tree)
        diff = _build_db_diff(repo, layer, db)

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


def _assert_db_tree_match(db, table, tree):
    dbcur = db.cursor()
    dbcur.execute("SELECT value FROM __kxg_meta WHERE table_name=? AND key=?;", (table, 'tree'))
    wc_tree_id = dbcur.fetchone()[0]
    assert (wc_tree_id == str(tree)), f"Working Copy is tree {wc_tree_id}; expecting {tree}"
    return wc_tree_id


@cli.command()
@click.pass_context
@click.option('--message', '-m', required=True)
def commit(ctx, message):
    repo_dir = ctx.obj['repo_dir']
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')
    commit = repo.head.commit
    tree = commit.tree

    repo_cfg = repo.config_reader('repository')
    if not repo_cfg.has_option('kx', 'workingcopy'):
        click.UsageError("No working-copy, use 'checkout'")

    fmt, working_copy, layer = repo_cfg.get('kx', 'workingcopy').split(':')
    assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

    table = layer

    db = _get_db(working_copy, isolation_level='DEFERRED')
    #db.execute("PRAGMA locking_mode = EXCLUSIVE;")
    with db:
        _assert_db_tree_match(db, table, tree)

        diff = _build_db_diff(repo, layer, db)
        if not any(diff.values()):
            print("No changes to commit")
            return

        dbcur = db.cursor()

        index_remove_entries = []
        index_add_entries = []

        for k, (obj_old, obj_new) in diff['META'].items():
            object_path = f"{layer}/meta/{k}"
            value = json.dumps(obj_new).encode('utf8')

            istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value)))
            file_mode = 0o100644
            entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, object_path))
            index_add_entries.append(entry)
            click.secho(f"Δ {object_path}", fg='yellow')

        for feature_key in diff['D'].keys():
            object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}"
            index_remove_entries.append(object_path)
            click.secho(f"- {object_path}", fg='red')

            dbcur.execute("DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?", (table, feature_key))
            assert dbcur.rowcount == 1, f"__kxg_map delete: expected 1Δ, got {dbcur.rowcount}"

        for obj in diff['I']:
            feature_key = str(uuid.uuid4())
            for k, value in obj.items():
                object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}/{k}"
                if not isinstance(value, bytes):  # blob
                    value = json.dumps(value).encode('utf8')

                istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value)))
                file_mode = 0o100644
                entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, object_path))
                index_add_entries.append(entry)
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

                    istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value)))
                    file_mode = 0o100644
                    entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, object_path))
                    index_add_entries.append(entry)
                    click.secho(f"Δ {object_path}", fg='yellow')
                else:
                    index_remove_entries.append(object_path)
                    click.secho(f"- {object_path}", fg='red')

        dbcur.execute("UPDATE __kxg_map SET state=0 WHERE table_name=? AND state != 0;", (table,))

        print("Updating index...")
        git_index = repo.index
        git_index.reset(commit, working_tree=False)
        if index_remove_entries:
            git_index.remove(index_remove_entries, working_tree=False, r=True, write=False)
        if index_add_entries:
            git_index.add(index_add_entries, write=False)
        git_index.write()

        print("Writing tree...")
        new_tree = git_index.write_tree()
        print(f"Tree sha: {new_tree}")

        dbcur.execute("UPDATE __kxg_meta SET value=? WHERE table_name=? AND key='tree';", (str(new_tree), table))
        assert dbcur.rowcount == 1, f"__kxg_meta update: expected 1Δ, got {dbcur.rowcount}"

        print("Committing...")
        new_commit = git_index.commit(message)
        print(f"Commit: {new_commit}")

        # update the ref (branch) to point to the current commit
        if not repo.head.is_detached:
            repo.head.ref.commit = new_commit


# straight process-replace commands

@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def log(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    os.execvp("git", ["git", "-C", repo_dir, "log"] + list(args))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def push(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    os.execvp("git", ["git", "-C", repo_dir, "push"] + list(args))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
))
@click.pass_context
@click.argument('args', nargs=-1, type=click.UNPROCESSED)
def fetch(ctx, args):
    repo_dir = ctx.obj['repo_dir'] or '.'
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    os.execvp("git", ["git", "-C", repo_dir, "fetch"] + list(args))


if __name__ == "__main__":
    cli()