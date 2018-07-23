import contextlib
import json
import logging
import re
import sqlite3
import textwrap
import time
from collections import defaultdict
from datetime import datetime

import box
import dateutil.parser
import dateutil.tz
import requests
import urllib3
from requests.adapters import HTTPAdapter
from yaspin import yaspin
from yaspin.spinners import Spinners

import snowdrop


SYNC_CONFIG_TABLE = '.kx_sync'
SYNC_LAYERS_TABLE = '.kx_sync_layers'
WFS_TIMEOUT = 60
WFS_CHANGESET_PAGE_SIZE = 1000

class UserError(Exception):
    pass
class SyncError(Exception):
    pass


def check_geopackage(db, initialized=False):
    """ Check whether a SQLite DB is a GeoPackage """
    L = logging.getLogger('snowdrop.main.check_geopackage')

    cur = db.cursor()
    # this will raise a sqlite3.DatabaseError if it isn't a SQLite DB
    app_id = cur.execute("PRAGMA application_id").fetchone()[0]
    app_id_str = app_id.to_bytes(4, 'big').decode('utf8')
    L.debug("application_id=%s '%s'", hex(app_id), app_id_str)
    if app_id_str not in ('GP10', 'GP11', 'GPKG'):
        return False

    if initialized:
        # check whether the DB has already been initialised for sync
        cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN (?,?);", (SYNC_CONFIG_TABLE, SYNC_LAYERS_TABLE))
        if cur.fetchone()[0] != 2:
            L.debug("initialised=False %s", (SYNC_CONFIG_TABLE, SYNC_LAYERS_TABLE))
            return False

    return True


def init_db(db_path, api_key):
    L = logging.getLogger('snowdrop.main.init_db')

    # Validate we have a SQLite DB and a GeoPackage
    db_uri = f'file:{db_path}?mode=rw'
    try:
        # this doesn't actually check whether it's a SQLite DB...
        db = sqlite3.connect(db_uri, uri=True, isolation_level=None)

        if not check_geopackage(db):
            raise UserError(f"{db_path} doesn't appear to be a valid GeoPackage")
    except sqlite3.DatabaseError as e:
        raise UserError(f"{db_path} doesn't appear to be a valid GeoPackage") from e

    with db:
        db.row_factory = sqlite3.Row
        cur = db.cursor()
        cur.execute("BEGIN")

        # check whether the DB has already been initialised for sync
        cur.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name IN (?,?);", (SYNC_CONFIG_TABLE, SYNC_LAYERS_TABLE))
        if cur.fetchone()[0] > 0:
            raise UserError(f"{db_path} has already been configured for sync. Maybe you want 'kx-sync sync'?")

        # Create config Table
        cur.execute(f"""CREATE TABLE "{SYNC_CONFIG_TABLE}" (key TEXT NOT NULL PRIMARY KEY, value TEXT)""")
        # Create layers table
        cur.execute(f"""CREATE TABLE "{SYNC_LAYERS_TABLE}" (layer_id INTEGER NOT NULL PRIMARY KEY, table_name TEXT NOT NULL, version_id INTEGER, source_date TEXT NOT NULL)""")

        domain = None

        # find layers
        for row in cur.execute("SELECT table_name, description, last_change FROM gpkg_contents WHERE data_type = 'features'"):
            # Source: https://koordinates.com/layer/55-nz-titles/
            m = re.match(r'Source: (?P<url>(?P<site>https://(?P<domain>[^/]+)/)layer/(?P<layer_id>\d+)-[^/]+/)\n', row['description'])
            if not m:
                raise UserError(f"{db_path} isn't from a Koordinates site")

            if domain is None:
                domain = m['domain']
                L.info("%s is from %s", db_path, m.group('site'))

            layer_id = int(m['layer_id'])
            L.info("Found feature table: %s (%s)", row['table_name'], m.group('url'))

            layer_info = api_get_layer(domain, api_key, layer_id)

            publish_date = dateutil.parser.parse(layer_info.published_at)
            gpkg_date = dateutil.parser.parse(row['last_change'])
            if gpkg_date >= publish_date:
                version_id = layer_info.version.id
                source_date = publish_date
            else:
                version_id = None
                source_date = gpkg_date

            L.debug("Layer %s publish_date=%s gpkg_date=%s", layer_id, publish_date, gpkg_date)
            cur.execute(
                f"""INSERT INTO "{SYNC_LAYERS_TABLE}" (layer_id, table_name, version_id, source_date) VALUES (?,?,?,?)""", (
                layer_id,
                row['table_name'],
                version_id,
                source_date.isoformat(),
            ))
        L.debug("Configured layers table: %s", SYNC_LAYERS_TABLE)

        # populate settings table
        cur.executemany(
            f"""INSERT INTO "{SYNC_CONFIG_TABLE}" (key, value) VALUES (?,?)""",
            [
                ('domain', domain),
                ('api_key', api_key),
                ('version', snowdrop.__version__),
            ]
        )
        L.debug("Configured settings table: %s", SYNC_CONFIG_TABLE)


def api_find_layer_version(domain, api_key, layer_id, publish_date):
    L = logging.getLogger('snowdrop.main.api_find_layer_version')

    url = f"https://{domain}/services/api/v1/layers/{layer_id}/versions/"
    r = requests.get(url, headers={'Authorization': 'key {}'.format(api_key)}, timeout=30)
    r.raise_for_status()
    version_list = box.BoxList(r.json())

    L.debug("Finding version < %s for Layer %s", publish_date, layer_id)
    for lv in version_list:
        created_at = dateutil.parser.parse(lv.created_at)
        if created_at > publish_date:
            # if it was created after our date it can't have been published before it
            L.debug("Skipping %s: created_at=%s", lv.id, created_at)
            continue

        r = requests.get(lv.url, headers={'Authorization': 'key {}'.format(api_key)}, timeout=30)
        r.raise_for_status()
        layer = box.Box(r.json())
        published_at = dateutil.parser.parse(layer.published_at)

        L.debug("LayerVersion %s published_at=%s", layer.version.id, published_at)
        if published_at <= publish_date:
            return layer

    return None


def api_get_layer(domain, api_key, layer_id):
    url = f"https://{domain}/services/api/v1/layers/{layer_id}/"
    r = requests.get(url, headers={'Authorization': 'key {}'.format(api_key)}, timeout=30)
    r.raise_for_status()
    data = r.json()
    return box.Box(data)


def sync_db(db_path, date_to=None, verbosity=1):
    L = logging.getLogger('snowdrop.main.sync_db')

    # Validate we have a SQLite DB and a GeoPackage
    db_uri = f'file:{db_path}?mode=rw'
    try:
        # this doesn't actually check whether it's a SQLite DB...
        db = sqlite3.connect(db_uri, uri=True, isolation_level=None)
        db.row_factory = sqlite3.Row

        if not check_geopackage(db, initialized=True):
            raise UserError(f"{db_path} doesn't appear to be a valid initialised GeoPackage")
    except sqlite3.DatabaseError as e:
        raise UserError(f"{db_path} doesn't appear to be a valid GeoPackage") from e

    cur = db.cursor()
    try:
        db.enable_load_extension(True)
        db.execute("SELECT load_extension(?)", ('mod_spatialite',))
        cur.execute("SELECT EnableGpkgMode()")
    except sqlite3.Error as e:
        raise UserError(f"Spatialite Error") from e

    sync_conf = box.Box(dict(cur.execute(f"""SELECT key, value FROM "{SYNC_CONFIG_TABLE}";""").fetchall()))
    for layer_id, table_name, version_id, source_date in cur.execute(f"""SELECT layer_id, table_name, version_id, source_date FROM "{SYNC_LAYERS_TABLE}";"""):
        if verbosity == 1:
            progress = yaspin(Spinners.earth, text=f"{table_name}: Checking...")
        else:
            progress = None
            L.info("Checking %s...", table_name)

        with (progress or contextlib.suppress()):
            if date_to is None:
                layer_info = api_get_layer(sync_conf.domain, sync_conf.api_key, layer_id)
            else:
                layer_info = api_find_layer_version(sync_conf.domain, sync_conf.api_key, layer_id, publish_date=date_to)
                if not layer_info:
                    raise UserError("No data was published for Layer %s at %s", layer_id, date_to)

            source_date = dateutil.parser.parse(source_date)
            layer_info.published_at = dateutil.parser.parse(layer_info.published_at)
            target_date = date_to or datetime.now(dateutil.tz.UTC)

            L.debug("Layer %s: local=v%s (%s) remote=v%s (%s) target=%s",
                layer_id, version_id, source_date,
                layer_info.version.id, layer_info.published_at,
                target_date
            )
            if layer_info.version.id <= version_id:
                if progress:
                    progress.text = f"{table_name}: no update available"
                    progress.ok("✅ ")
                else:
                    L.info("No update available for %s, table_name")
                continue

            else:
                srs_id = cur.execute("SELECT srs_id FROM gpkg_contents WHERE table_name=?", (table_name,)).fetchone()[0]
                try:
                    with db:
                        cur.execute("BEGIN")
                        counts = sync_layer(db, sync_conf, layer_id, table_name, source_date, srs_id, layer_info, target_date, progress)

                        L.debug("Updating sync table: version_id=%s source_date=%s", layer_info.version.id, layer_info.published_at)
                        cur.execute(f"""UPDATE "{SYNC_LAYERS_TABLE}" SET version_id=:ver_id, source_date=:target_date WHERE layer_id=:layer_id""", {
                            'layer_id': layer_id,
                            'ver_id': layer_info.version.id,
                            'target_date': target_date.isoformat(),
                        })
                        if not cur.rowcount == 1:
                            L.error("SQL rowcount mismatch updating %s: %s != 1", SYNC_LAYERS_TABLE, cur.rowcount)
                            raise SyncError("Error updating layer sync information")

                    if progress:
                        progress.text = f"{table_name}: {counts['all']} changes synced (up to {target_date})"
                        progress.ok("✅ ")
                    else:
                        L.info("%s: %s changes synced (up to %s)", table_name, counts['all'], target_date)
                except:
                    if progress:
                        progress.text = f"{table_name}: Error"
                        progress.fail("💥 ")
                    raise


def escape_sqlite_identifier(identifier):
    return f'''"{identifier.replace('"', '""')}"'''


def sync_layer(db, conf, layer_id, table_name, source_date, srs_id, layer_info, target_date, progress):
    L = logging.getLogger(f'snowdrop.main.sync_layer.{layer_id}')

    t0 = time.time()
    is_vector = (layer_info.kind == 'vector')
    L.debug("is_vector: %s (%s)", is_vector, layer_info.kind)

    attr_fields = {f.name for f in layer_info.data.fields if f.type != 'geometry'}
    pk_fields = set(layer_info.data.primary_key_fields)
    geom_field = layer_info.data.geometry_field
    all_fields = attr_fields | ({geom_field} if is_vector else {})

    L.debug("attr_fields: %s", attr_fields)
    L.debug("geom_field: %s", geom_field)
    L.debug("pk_fields: %s", pk_fields)

    cur = db.cursor()
    cur.execute(f"PRAGMA table_info({table_name});")
    # FIXME: ignore 'fid' field since it's set to autoincrement PK
    db_cols = {row['name']: dict(zip(row.keys(), row)) for row in cur}
    L.debug("DB columns: %s", db_cols)
    db_fields = set(db_cols.keys())
    if 'fid' in db_cols:
        if db_cols['fid']['type'] == 'INTEGER' and db_cols['fid']['pk'] == 1 and db_cols['fid']['notnull'] == 1:
            L.debug("Ignoring OGR PK 'fid' DB field")
            db_fields.discard('fid')

    if db_fields != all_fields:
        L.error("Field mismatch DB +%s; WFS +%s", (db_fields - attr_fields), (attr_fields - db_fields))
        raise SyncError(f"Field mismatch between DB & Koordinates: {(db_fields ^ attr_fields)}")

    L.debug("DB rowcount: %s", cur.execute(f"SELECT count(*) FROM {escape_sqlite_identifier(table_name)}").fetchone()[0])

    # yuck
    sql_statements = {
        "INSERT": f"""
            INSERT INTO "{table_name}" ({', '.join([escape_sqlite_identifier(f) for f in attr_fields])})
            VALUES ({', '.join([':{}'.format(f) for f in attr_fields])});
            """,
        "UPDATE": f"""
            UPDATE "{table_name}"
            SET {', '.join(["{}=:{}".format(escape_sqlite_identifier(f), f) for f in attr_fields])}
            WHERE {' AND '.join(['{}=:{}'.format(escape_sqlite_identifier(f), f) for f in pk_fields])};
            """,
        "DELETE": f"""
            DELETE FROM "{table_name}" WHERE {' AND '.join(['{}=:{}'.format(escape_sqlite_identifier(f), f) for f in pk_fields])};
            """,
    }
    if is_vector:
        sql_statements.update({
            "INSERT": f"""
                INSERT INTO "{table_name}" ({escape_sqlite_identifier(geom_field)}, {', '.join([escape_sqlite_identifier(f) for f in attr_fields])})
                VALUES (
                    SetSRID(GeomFromGeoJSON(:{geom_field}),{srs_id}),
                    {', '.join([':{}'.format(f) for f in attr_fields])}
                );
                """,
            "UPDATE": f"""
                UPDATE "{table_name}"
                SET
                    {escape_sqlite_identifier(geom_field)}=SetSRID(GeomFromGeoJSON(:{geom_field}),{srs_id}),
                    {', '.join(["{}=:{}".format(escape_sqlite_identifier(f), f) for f in attr_fields])}
                WHERE
                    {' AND '.join(['{}=:{}'.format(escape_sqlite_identifier(f), f) for f in pk_fields])};
                """,
        })
    for k, v in sql_statements.items():
        L.debug("Template SQL statement for %s: %s", k, textwrap.dedent(v).rstrip())

    date_from = source_date.isoformat().replace('+00:00', 'Z')
    date_to = target_date.isoformat().replace('+00:00', 'Z')
    start = 0
    counts = defaultdict(int)
    L.debug("Changeset period: from=%s to=%s", date_from, date_to)
    L.debug("Changeset page size = %s", WFS_CHANGESET_PAGE_SIZE)

    #https://vm.dev.kx.gd/services;key=110fd69629de413f9c259fb905e2d86a/wfs/layer-55-changeset?SERVICE=WFS&REQUEST=GetCapabilities&viewparams=from:2017-12-21T14:35:45.509554Z;to:2018-01-10T21:54:28.450091Z

    with requests.Session() as session:
        session.headers = {'Authorization': f'key {conf.api_key}'}
        retries = urllib3.Retry(total=10, connect=0, backoff_factor=1, status_forcelist=[429, 502, 503, 504], raise_on_redirect=True)
        session.mount('https://', HTTPAdapter(max_retries=retries))

        if progress:
            progress.text = f"{table_name}: 0 changes..."

        while True:
            url = f"https://{conf.domain}/services/wfs/{layer_info.type}-{layer_id}-changeset?SERVICE=WFS&VERSION=2.0.0&REQUEST=GetFeature&typeNames={layer_info.type}-{layer_id}-changeset&viewparams=from:{date_from};to:{date_to}&outputFormat=json&count={WFS_CHANGESET_PAGE_SIZE}&startIndex={start}&srsName=EPSG:{srs_id}"
            L.debug("Fetching from %s: %s", start, url)
            r = session.get(url, timeout=WFS_TIMEOUT)
            L.debug("Result: %s %s in %ss", r.status_code, r.reason, r.elapsed.total_seconds())
            r.raise_for_status()
            t1 = time.time()

            try:
                data = r.json()
            except ValueError:
                L.debug("First 1KB of text:\n%s", repr(r.text[:1024]))
                if r.text.startswith('<ows:ExceptionReport '):
                    m = re.match(r'<ows:ExceptionReport .*?>\s*<ows:Exception exceptionCode="(?P<code>.*?)">\s*<ows:ExceptionText>(?P<text>.*?)</ows:ExceptionText>', r.text, re.DOTALL)
                    if m:
                        raise SyncError("WFS Error ({code}): {text}".format(**m.groupdict()))
                raise

            if data.get('type', None) != 'FeatureCollection':
                raise SyncError("WFS: Unexpected response format")

            L.debug("crs=%s totalFeatures=%s featureCount=%s", data['crs'], data['totalFeatures'], len(data['features']))

            if len(data['features']) == 0:
                L.debug("No features in response")
                break

            if start == 0:
                # check all this stuff once
                L.debug("Checking response CRS: layer=%s feature=%s", srs_id, data['crs'])
                if data['crs']['properties']['name'] != f'urn:ogc:def:crs:EPSG::{srs_id}':
                    raise SyncError(f"WFS response CRS mismatch: requested EPSG:{srs_id} but got {data['crs']}")

                f0 = data['features'][0]
                f_fields = set(f0['properties'].keys())
                f_fields.discard('__change__')
                L.debug("Checking Feature fields: layer=%s feature=%s", attr_fields, f_fields)
                if f_fields != attr_fields:
                    raise SyncError(f"WFS field mismatch: %s", (f_fields ^ attr_fields))

            for i, feature in enumerate(data['features']):
                # {
                #   "type": "Feature",
                #   "id": "layer-55-changeset.1",
                #   "geometry": {
                #     "type": "MultiPolygon",
                #     "coordinates": [[[[1483052.5633666886,5300209.8463025475],[1483167.711633922,5300270.112598528],[1483250.4147460386,5300213.157832153],[1483052.5633666886,5300209.8463025475]]],[[[1482408.2376226126,5300199.02304678],[1482491.6637041818,5300314.6892252825],[1482543.771555084,5300282.757257851],[1482573.6691695328,5300201.807609348],[1482408.2376226126,5300199.02304678]]]]
                #   },
                #   "geometry_name": "geom",
                #   "properties": {
                #     "__change__": "INSERT",
                #     "gid": 9317,
                #     "id": 1160086,
                #     "title_no": "WS5C/560",
                #     "status": "LIVE",
                #     "type": "Freehold",
                #     "land_distr": "Westland",
                #     "issue_date": "1988-03-11Z",
                #     "guarantee": "Guarantee",
                #     "estate_des": "Fee Simple, 1/1, Rural Section 3095, 850,953 m2",
                #     "number_own": "1",
                #     "spatial_ex": "F"
                #   }
                # }
                params = feature['properties'].copy()
                if 'geometry_name' in feature:
                    params[feature['geometry_name']] = json.dumps(feature['geometry'])
                change = params.pop('__change__')

                sql = sql_statements[change]
                if i == 0 and start == 0:
                    L.debug("Feature %s:\n  %s\n  params=%s", i, textwrap.dedent(sql).strip(), json.dumps(params))

                try:
                    cur.execute(sql, params)
                except sqlite3.Error as e:
                    L.debug("row %s: %s: %s %s", i, e, change, json.dumps(params), exc_info=True)
                    raise SyncError(f"{change} failed at row {i+start}") from e

                if cur.rowcount != 1:
                    pk_vals = {f:params[f] for f in pk_fields}
                    L.error("row %s: SQL rowcount mismatch: got %s for %s pk=%s", i, cur.rowcount, change, pk_vals)
                    raise SyncError(f"{change} failed for {pk_vals}")

                counts[change] += 1
                counts['all'] += 1

            L.debug("Applied %s changes in %ss", i+1, (time.time() - t1))
            if progress:
                progress.text = f"{table_name}: {counts['all']} changes..."

            start += len(data['features'])
            L.debug("Next start=%s", start)

        L.debug("No more features; Applied changes: %s", dict(counts))

        # check total feature count matches
        db_count = cur.execute(f"SELECT count(*) FROM {escape_sqlite_identifier(table_name)};").fetchone()[0]
        L.debug("Feature counts local=%s layer=%s (total_changes=%s)", db_count, layer_info.data.feature_count, db.total_changes)
        if db_count != layer_info.data.feature_count:
            raise SyncError("Table row count mismatch after applying changeset")

        L.debug("Sync took %ss", (time.time() - t0))
        return counts
