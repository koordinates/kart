#!/usr/bin/env python3

import argparse
import random
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pygit2


def main():
    parser = argparse.ArgumentParser(
        description='Create some history by evolving a working copy geopackage and committing repeatedly',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('commits', metavar='COMMITS', type=int, help="how many Sno commits to create. 0 will make changes without committing")
    parser.add_argument('tables', metavar='TABLE', nargs='*', help='GeoPackage tables to evolve (skip for all)')
    parser.add_argument('--scale', metavar='M', type=int, default=1, help='Multiply per-commit change counts by this')
    parser.add_argument('--inserts', metavar='N', type=int, default=3, help='count of changes that should be INSERTs (per table per commit)')
    parser.add_argument('--deletes', metavar='N', type=int, default=1, help='count of changes that should be DELETEs (per table per commit)')
    parser.add_argument('--updates', metavar='N', type=int, default=17, help='count of changes that should be UPDATEs (per table per commit)')
    parser.add_argument('--attributes', metavar='N', type=int, default=3, help='count of attribute changes per UPDATE')
    parser.add_argument('-C', '--repo', metavar='PATH', help='Repository path', default='.')
    parser.add_argument('--gpkg', metavar='PATH', help='GeoPackage path')
    parser.add_argument('--debug', action='store_true', help='Show SQL queries')

    options = parser.parse_args()

    repo = pygit2.Repository(options.repo)
    if options.commits and not repo:
        parser.error(f"No repository found at: '{options.repo}'. Specify path via --repo or use COMMITS=0")

    if options.gpkg:
        db_path = options.gpkg
        if not Path(db_path).exists():
            parser.error(f"GeoPackage {db_path} not found")
    elif repo:
        if 'sno.workingcopy.path' not in repo.config:
            parser.error(f"No working copy found, specify with --gpkg?")

        db_path = str(Path(repo.path) / repo.config['sno.workingcopy.path'])
    else:
        parser.error("If no repository, need to specify GeoPackage path with --gpkg")

    def debug(*content, fg='37', prefix=''):
        if not options.debug:
            return
        print(f"\x1b[1;{fg}m", prefix, *content, "\x1b[0;0m", file=sys.stderr)

    def db_debug(*content):
        if content[0].startswith('-- '):
            # ignore triggers
            return
        debug(*content, fg='34', prefix='🌀  ')

    db = sqlite3.connect(db_path)
    db.isolation_level = None  # disable stupid DBAPI behaviour
    db.row_factory = sqlite3.Row
    db.enable_load_extension(True)
    db.execute("SELECT load_extension('mod_spatialite');")
    db.execute("SELECT EnableGpkgMode();")
    if options.debug:
        db.set_trace_callback(db_debug)
    print(f"Connected to {db_path}")

    all_tables = [r[0] for r in db.execute("SELECT table_name FROM gpkg_contents;")]
    if options.tables:
        tables = options.tables
        assert set(tables) <= set(all_tables), f"Couldn't find some of those tables: {set(tables) - set(all_tables)}"
    else:
        tables = all_tables

    print(f"Tables:", *tables, sep="\n\t")

    print("Getting row counts...")
    row_counts = {}
    for table in tables:
        row_counts[table] = db.execute(f'SELECT COUNT(*) FROM "{table}";').fetchone()[0]
        print(f"\t{table:40}\t{row_counts[table]:9,}")

    print("Getting schema information...")
    col_info = {}
    pk_info = {}
    for table in tables:
        q = db.execute(f'PRAGMA table_info("{table}");')
        cols = {}
        for row in q:
            if row['pk']:
                pk_info[table] = row['name']
            else:
                cols[row['name']] = row['type']
        col_info[table] = cols

    def insert(table, n):
        cols = col_info[table]
        cols_expr = ', '.join([f'"{c}"' for c in cols])
        offset = random.randint(0, row_counts[table] - n -1)
        dbcur = db.cursor()
        sql = (
            f"""INSERT INTO "{table}" """
            f"""({cols_expr}) """
            f"""SELECT {cols_expr} FROM "{table}" LIMIT {n} OFFSET {offset}"""
        )
        debug(sql, fg=36, prefix="🌀📝")
        dbcur.execute(sql)
        assert dbcur.rowcount == n
        row_counts[table] += n

    def delete(table, n):
        offset = random.randint(0, row_counts[table] - n - 1)
        pk = pk_info[table]
        dbcur = db.cursor()
        sql = f"""DELETE FROM "{table}" WHERE "{pk}" IN (SELECT "{pk}" FROM "{table}" LIMIT {n} OFFSET {offset});"""
        debug(sql, fg=36, prefix="🌀📝")
        dbcur.execute(sql)
        assert dbcur.rowcount == n
        row_counts[table] -= n

    def update(table, n):
        cols = col_info[table]
        pk = pk_info[table]
        dbcur = db.cursor()
        for i in range(n):
            offset = random.randint(0, row_counts[table] - 1)
            f_old = dict(dbcur.execute(f"""SELECT * FROM "{table}" LIMIT 1 OFFSET {offset};""").fetchone())

            f_new = {}
            for j in range(options.attributes):
                c = list(cols.keys())[random.randint(0, len(cols)-1)]
                f_new[c] = evolve(cols[c], f_old[c])

            upd_expr = []
            params = []

            for c, (e, v) in f_new.items():
                if e is None:
                    upd_expr.append(f'"{c}"=?')
                else:
                    upd_expr.append(f'"{c}"={e}')
                params.append(v)

            if options.debug:
                d_old = {c: ('<>' if isinstance(v, bytes) else v) for c, v in f_old.items() if c in f_new}
                d_new = {c: (v if e is None else '?'.replace('?', ('<>' if isinstance(v, bytes) else str(v)))) for c, (e, v) in f_new.items()}
                debug(f"{pk}={f_old[pk]}:", d_old, "\n  -> ", d_new, prefix="🔶", fg=33)

            params.append(f_old[pk])
            sql = (
                f"""UPDATE "{table}" """
                f"""SET {", ".join(upd_expr)} """
                f"""WHERE "{pk}"=?;"""
            )
            debug(sql, params, fg=36, prefix="🌀📝")
            dbcur.execute(sql, params)
            assert dbcur.rowcount == 1

    def evolve(typ, old):
        if old is None:
            return (None, old)

        # https://www.sqlite.org/datatype3.html
        # 3.1. Determination Of Column Affinity

        if (typ is None) or re.search('BLOB|POINT|POLYGON|LINE|GEO', typ):
            # Geometry
            dx = random.random() * 4 - 2
            dy = random.random() * 4 - 2
            return (f"ST_Translate(?, {dx}, {dy}, 0)", old)

        elif re.search('INT', typ):
            return (None, old + random.randint(-1000, 1000))

        elif re.search('TEXT|CHAR|CLOB', typ):
            try:
                # Dates
                dt = datetime.fromisoformat(old)
                return (None, (dt + timedelta(days=random.randint(0, 30) - 15)).isoformat())
            except ValueError:
                # not a datetime
                s = list(old[:6])
                random.shuffle(s)
                return (None, ''.join(s) + old[6:])

        elif re.search('REAL|FLOA|DOUB', typ):
            return (None, old + (random.random() * 1000 - 500))

        elif re.search('BOOL', typ):
            return (None, old * -1)

        else:
            # NUMERIC
            return (None, old + 1)

    # Start the changes...
    print("Beginning...")
    num_inserts = options.inserts * options.scale
    num_updates = options.updates * options.scale
    num_deletes = options.deletes * options.scale

    t0 = time.time()
    try:
        for i in range(options.commits or 1):
            db.execute("BEGIN")

            for table in tables:
                update(table, num_updates)
                insert(table, num_inserts)
                delete(table, num_deletes)

            if options.debug:
                print("Aborting for --debug ...")
                db.execute("ROLLBACK")
                return
            else:
                db.execute("COMMIT")

            if not options.commits:
                print("GeoPackage changes made, skipping sno commit.")
                return
            else:
                subprocess.check_call([
                    'sno', 'commit',
                    '-m', f'history-creator changed things at {datetime.now():%H:%M:%S}'
                ])

            if i and (i % 10 == 0):
                print(f"\t{i+1} @ {(time.time()-t0):0.1f}s ...")
    finally:
        t1 = time.time()
        print(f"Completed {options.commits or 1} loops in {(t1-t0):.1f}s ({((options.commits or 1)/(t1-t0)):.1f}/s)")


if __name__ == "__main__":
    main()
