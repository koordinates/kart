#!/usr/bin/env python3
import collections
import io
import json
import os
import sqlite3
import time
import uuid

import click
import git
import gitdb
from osgeo import gdal, ogr, osr


gdal.UseExceptions()

repo_params = {
    'odbt': git.GitCmdObjectDB,
}



@click.group()
@click.option('repo_dir', '--repo', type=click.Path(file_okay=False, dir_okay=True), default=os.curdir, metavar="PATH")
@click.pass_context
def cli(ctx, repo_dir):
    ctx.ensure_object(dict)
    ctx.obj['repo_dir'] = repo_dir


def sqlite_ident(identifier):
    escaped = identifier.replace("\"", "\"\"")
    return f'"{escaped}"'


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

    with sqlite3.connect(geopackage) as db:
        db.row_factory = sqlite3.Row
        dbcur = db.cursor()

        index_entries = []
        print("Writing meta bits...")
        value = json.dumps({
            "version": "0.0.1"
        })
        istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value.encode('utf8'))))
        file_mode = 0o100644
        entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, f"{table}/meta/version"))
        index_entries.append(entry)

        try:
            for gpkg_table in ('gpkg_contents', 'gpkg_geometry_columns'):
                dbcur.execute(f"SELECT * FROM {gpkg_table} WHERE table_name=?;", (table,))
                row = dbcur.fetchone()
                value = json.dumps(collections.OrderedDict(sorted(zip(row.keys(), row))))
                istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value.encode('utf8'))))
                file_mode = 0o100644
                entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, f"{table}/meta/{gpkg_table}"))
                index_entries.append(entry)
        except Exception:
            print(f"Error building meta/{gpkg_table}")
            raise

        QUERIES = {
            'sqlite_table_info': (f'PRAGMA table_info({sqlite_ident(table)});',),
            'gpkg_metadata_reference': ('''
                SELECT MR.*
                FROM gpkg_metadata_reference MR
                    INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
                WHERE
                    MR.table_name=?
                    AND MR.column_name IS NULL
                    AND MR.row_id_value IS NULL;
            ''', (table,)),
            'gpkg_metadata': ('''
                SELECT M.*
                FROM gpkg_metadata_reference MR
                    INNER JOIN gpkg_metadata M ON (MR.md_file_id = M.id)
                WHERE
                    MR.table_name=?
                    AND MR.column_name IS NULL
                    AND MR.row_id_value IS NULL;
            ''', (table,)),
            'gpkg_spatial_ref_sys': ('''
                SELECT DISTINCT SRS.*
                FROM gpkg_spatial_ref_sys SRS
                    LEFT OUTER JOIN gpkg_contents C ON (C.srs_id = SRS.srs_id)
                    LEFT OUTER JOIN gpkg_geometry_columns G ON (G.srs_id = SRS.srs_id)
                WHERE
                    (C.table_name=? OR G.table_name=?)
            ''', (table, table))
        }
        try:
            for filename, sql_cmd in QUERIES.items():
                dbcur.execute(*sql_cmd)
                value = json.dumps([collections.OrderedDict(sorted(zip(row.keys(), row))) for row in dbcur])
                istream = repo.odb.store(gitdb.IStream(git.Blob.type, len(value), io.BytesIO(value.encode('utf8'))))
                file_mode = 0o100644
                entry = git.BaseIndexEntry((file_mode, istream.binsha, 0, f"{table}/meta/{filename}"))
                index_entries.append(entry)
        except Exception:
            print(f"Error building meta/{filename}")
            raise

        repo.index.add(index_entries, write=False)

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
            #print(feature_id, object_path, field, value, entry)

            if i and i % 500 == 0:
                print(f"  {i+1} features... @{time.time()-t1:.1f}s")

        t2 = time.time()
        print("Adding to index...")
        repo.index.add(index_entries, write=False)
        t3 = time.time()

        print(f"Added {i+1} Features to index in {t3-t2:.1f}s")
        print(f"Overall rate: {((i+1)/(t3-t0)):.0f} features/s)")

        print("Writing tree...")
        tree_sha = repo.index.write_tree()
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
@click.argument('working-copy', type=click.Path(writable=True, dir_okay=False), required=False)
def checkout(ctx, working_copy, layer, fmt):
    repo_dir = ctx.obj['repo_dir']
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')

    repo_cfg = repo.config_reader('repository')
    if repo_cfg.has_option('kx', 'workingcopy'):
        if working_copy is not None:
            raise click.BadParameter(f"This repository already has a working copy at: {repo_cfg.get('kx', 'workingcopy')}", param_hint='WORKING_COPY')
        fmt, working_copy, layer = repo_cfg.get('kx', 'workingcopy').split(':')

        assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

        click.echo(f'Updating {working_copy}...')
        return _checkout_update(repo, working_copy, layer)

    # new working-copy path
    if not working_copy:
        raise click.BadParameter("No existing working copy, specify working-copy path", param_hint='working-copy')
    if not layer:
        raise click.BadParameter("No existing working copy, specify layer", param_hint='--layer')

    if not fmt:
        fmt = 'GPKG'

    click.echo(f'Checkout {layer} to {working_copy} as {fmt}...')

    _checkout_new(repo, working_copy, layer, fmt)

    repo_cfg = repo.config_writer()
    if not repo_cfg.has_section("kx"):
        repo_cfg.add_section("kx")
    repo_cfg.set("kx", "workingcopy", f"{fmt}:{working_copy}:{layer}")
    repo_cfg.write()
    del repo_cfg


def _feature_tree_to_dict(feature_tree, geom_column_name):
    o = {}
    for blob in feature_tree.blobs:
        if blob.name == geom_column_name:
            value = blob.data_stream.read()
            assert value[:2] == b'GP', "Not a standard GeoPackage geometry"
        else:
            value = json.load(blob.data_stream)
        o[blob.name] = value
    return o


OFTMap = {
    'INTEGER': ogr.OFTInteger,
    'MEDIUMINT': ogr.OFTInteger,
    'TEXT': ogr.OFTString,
    'REAL': ogr.OFTReal,
}

def _checkout_new(repo, working_copy, layer, fmt):
    if fmt != "GPKG":
        raise NotImplementedError(fmt)

    commit = repo.refs.master.commit
    click.echo(f'Commit: {commit}')

    layer_tree = commit.tree / layer
    meta_tree = layer_tree / 'meta'
    meta_info = json.load((meta_tree / 'gpkg_contents').data_stream)

    if meta_info['table_name'] != layer:
        assert False, f"Layer mismatch (table_name={meta_info['table_name']}; layer={layer}"
    table = layer

    meta_geom = json.load((meta_tree / 'gpkg_geometry_columns').data_stream)
    meta_cols = json.load((meta_tree / 'sqlite_table_info').data_stream)
    # meta_md = json.load((meta_tree / 'gpkg_metadata').data_stream)
    # meta_md_ref = json.load((meta_tree / 'gpkg_metadata_reference').data_stream)
    meta_srs = json.load((meta_tree / 'gpkg_spatial_ref_sys').data_stream)

    # GDAL: Create GeoPackage
    # GDAL: Add metadata/etc
    gdal_driver = gdal.GetDriverByName(fmt)
    gdal_ds = gdal_driver.Create(working_copy, 0, 0, 0, gdal.GDT_Unknown)
    del gdal_ds

    db = sqlite3.connect(working_copy, isolation_level='DEFERRED')
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA foreign_keys = ON;")
    db.execute("PRAGMA synchronous = OFF;")
    with db:
        for o in meta_srs:
            keys, values = zip(*o.items())
            sql = f"INSERT OR REPLACE INTO gpkg_spatial_ref_sys ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
            db.execute(sql, values)

        keys, values = zip(*meta_info.items())
        sql = f"INSERT INTO gpkg_contents ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
        db.execute(sql, values)

        keys, values = zip(*meta_geom.items())
        sql = f"INSERT INTO gpkg_geometry_columns ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
        db.execute(sql, values)

        db.execute("DELETE FROM gpkg_geometry_columns WHERE table_name='ogr_empty_table';")
        db.execute("DELETE FROM gpkg_contents WHERE table_name='ogr_empty_table';")
        db.execute("DROP TABLE ogr_empty_table;")

        # for o in meta_md:
        #     keys, values = zip(*o.items())
        #     sql = f"INSERT INTO gpkg_metadata ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
        #     db.execute(sql, values)

        # for o in meta_md_ref:
        #     keys, values = zip(*o.items())
        #     sql = f"INSERT INTO gpkg_metadata_reference ({','.join([sqlite_ident(k) for k in keys])}) VALUES ({','.join(['?']*len(keys))});"
        #     db.execute(sql, values)

        pk_field = 'fid'
        col_specs = []
        col_names = []
        for col in meta_cols:
            col_spec = f"{sqlite_ident(col['name'])} {col['type']}"
            if col['pk']:
                col_spec += " PRIMARY KEY"
                pk_field = col['name']
            if col['notnull']:
                col_spec += " NOT NULL"
            col_specs.append(col_spec)
            col_names.append(col['name'])
        db.execute(f"CREATE TABLE {sqlite_ident(table)} ({', '.join(col_specs)});")

        db.execute(f"CREATE TABLE __kxg_map (table_name TEXT NOT NULL, feature_key VARCHAR(36) NULL, feature_id INTEGER NOT NULL, state INTEGER NOT NULL DEFAULT 0);")

        click.echo("Creating features...")
        col_names = tuple(col_names)
        sql_insert_features = f"INSERT INTO {sqlite_ident(table)} ({','.join([sqlite_ident(k) for k in col_names])}) VALUES ({','.join(['?']*len(col_names))});"
        sql_insert_ids = "INSERT INTO __kxg_map (table_name, feature_key, feature_id, state) VALUES (?,?,?,0);"
        feat_count = 0
        t0 = time.time()

        wip_features = []
        wip_idmap = []
        for ftree_prefix in (layer_tree / 'features').trees:
            for ftree in ftree_prefix.trees:
                feature = _feature_tree_to_dict(ftree, meta_geom['column_name'])

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
        db.execute(f"""
            CREATE TRIGGER {sqlite_ident(f"__kxg_{table}_ins")}
               AFTER INSERT
               ON {sqlite_ident(table)}
            BEGIN
                INSERT INTO __kxg_map (table_name, feature_key, feature_id, state)
                    VALUES (?, NULL, NEW.fid, 1);
            END;
        """, (table,))
        db.execute(f"""
            CREATE TRIGGER {sqlite_ident(f"__kxg_{table}_upd")}
               AFTER UPDATE
               ON {sqlite_ident(table)}
            BEGIN
                UPDATE __kxg_map
                    SET state=1, feature_id=NEW.fid
                    WHERE table_name=?
                        AND feature_id=OLD.fid
                        AND state >= 0;
            END;
        """, (table,))
        db.execute(f"""
            CREATE TRIGGER {sqlite_ident(f"__kxg_{table}_del")}
               AFTER DELETE
               ON {sqlite_ident(table)}
            BEGIN
                UPDATE __kxg_map
                SET state=-1
                WHERE table_name=?
                    AND feature_id=OLD.fid;
            END;
        """, (table,))

    print(f"Added {feat_count} Features to GPKG in {t1-t0:.1f}s")
    print(f"Overall rate: {(feat_count/(t1-t0)):.0f} features/s)")

    gdal_ds = gdal.OpenEx(working_copy, gdal.OF_VECTOR | gdal.OF_UPDATE | gdal.OF_VERBOSE_ERROR, ["GPKG"])
    gdal_ds.ExecuteSQL(f'SELECT CreateSpatialIndex({sqlite_ident(table)}, {sqlite_ident(meta_geom["column_name"])});')
    print(f"Created spatial index in {time.time()-t1:.1f}s")
    del gdal_ds


def _checkout_update(repo, working_copy, layer):
    raise NotImplementedError()


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


def _build_feature_diff(repo, commit, layer, db):
    layer_tree = commit.tree / layer
    meta_tree = layer_tree / 'meta'
    meta_geom = json.load((meta_tree / 'gpkg_geometry_columns').data_stream)

    table = layer
    candidates = {'I':[],'U':{},'D':{}}

    dbcur = db.cursor()
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
        'I': candidates['I'],
        'D': candidates['D'],
        'U': {},
    }

    features_tree = (commit.tree / layer / 'features')
    for feature_key, db_obj in candidates['U'].items():
        ftree = (features_tree / feature_key[:4] / feature_key)
        repo_obj = _feature_tree_to_dict(ftree, meta_geom['column_name'])

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
    commit = repo.refs.master.commit

    repo_cfg = repo.config_reader('repository')
    if not repo_cfg.has_option('kx', 'workingcopy'):
        click.UsageError("No working-copy, use 'checkout'")

    fmt, working_copy, layer = repo_cfg.get('kx', 'workingcopy').split(':')
    assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

    db = sqlite3.connect(working_copy, isolation_level='DEFERRED')
    db.row_factory = sqlite3.Row
    with db:
        diff = _build_feature_diff(repo, commit, layer, db)

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

@cli.command()
@click.pass_context
@click.option('--message', '-m', required=True)
def commit(ctx, message):
    repo_dir = ctx.obj['repo_dir']
    repo = git.Repo(repo_dir, **repo_params)
    if not repo or not repo.bare:
        raise click.BadParameter("Not an existing bare repository?", param_hint='--repo')
    commit = repo.refs.master.commit

    repo_cfg = repo.config_reader('repository')
    if not repo_cfg.has_option('kx', 'workingcopy'):
        click.UsageError("No working-copy, use 'checkout'")

    fmt, working_copy, layer = repo_cfg.get('kx', 'workingcopy').split(':')
    assert os.path.isfile(working_copy), f"Working copy missing? {working_copy}"

    table = layer

    db = sqlite3.connect(working_copy, isolation_level='DEFERRED')
    db.row_factory = sqlite3.Row
    with db:
        db.execute("PRAGMA locking_mode=EXCLUSIVE;")

        diff = _build_feature_diff(repo, commit, layer, working_copy)
        if not any(diff.values()):
            print("No changes to commit")
            return

        index_remove_entries = []
        index_add_entries = []

        for feature_key in diff['D'].keys():
            object_path = f"{layer}/features/{feature_key[:4]}/{feature_key}"
            index_remove_entries.append(object_path)
            click.secho(f"- {object_path}", fg='red')

            db.execute("DELETE FROM __kxg_map WHERE table_name=? AND feature_key=?", (table, feature_key))

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

            db.execute("INSERT INTO __kxg_map (table_name, feature_key, fid, state) VALUES (?,?,?,0);", (table, feature_key, obj['fid']))
        db.execute("DELETE FROM __kxg_map WHERE table_name=? AND feature_key IS NULL;", (table,))

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
                    click.secho(f"+ {object_path}", fg='green')
                else:
                    index_remove_entries.append(object_path)
                    click.secho(f"- {object_path}", fg='red')

        db.execute("UPDATE __kxg_map SET state=0 WHERE table_name=? AND state != 0", (table,))

        print("Updating index...")
        repo.index.remove(index_remove_entries, working_tree=False, r=True, write=False)
        repo.index.add(index_add_entries, write=True)

        print("Writing tree...")
        tree_sha = repo.index.write_tree()
        print(f"Tree sha: {tree_sha}")

        print("Committing...")
        commit = repo.index.commit(message)
        print(f"Commit: {commit}")

if __name__ == "__main__":
    cli()