import logging
import sqlite3
import sys

import box
import click
import dateutil.parser
import dateutil.tz

from .main import init_db, sync_db, UserError, SyncError, check_geopackage, load_spatialite


L = logging.getLogger('snowdrop.cli')


def validate_geopackage(ctx, param, value):
    """ Validate we have a SQLite DB and a GeoPackage """
    db_uri = f'file:{value}?mode=ro'
    try:
        # connecting doesn't actually check whether it's a SQLite DB...
        with sqlite3.connect(db_uri, uri=True, isolation_level=None) as db:
            # Check we have a GeoPackage
            if not check_geopackage(db):
                raise click.BadParameter(f"{value} doesn't appear to be a valid GeoPackage")

            return value
    except sqlite3.DatabaseError as e:
        raise click.BadParameter(f"{value} doesn't appear to be a valid GeoPackage") from e


def validate_timestamp(ctx, param, value):
    """ Validate we have an ISO timestamp """
    if value is None:
        return value

    try:
        value = dateutil.parser.parse(value)

        if value.tzinfo is None:
            L.info(f"'{value}' has no timezone specified, assuming UTC.")
            value = value.replace(tzinfo=dateutil.tz.UTC)

        # convert to UTC
        return value.astimezone(dateutil.tz.UTC)
    except ValueError as e:
        raise click.BadParameter(f"{value} isn't a valid ISO timestamp") from e


def config_logging(ctx, param, value):
    if value == 2:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s:%(lineno)d %(levelname)s %(message)s')
    elif value == 0:
        logging.basicConfig(level=logging.ERROR, format='%(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(message)s')


def version(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return

    import sqlite3
    import snowdrop

    click.echo(f'kx-sync version: {snowdrop.__version__}', color=ctx.color)

    click.echo(f'Python {sys.version} [PyAPI v{sys.api_version}]', color=ctx.color)

    # Check SQLite/Spatialite versions
    try:
        # this doesn't actually check whether it's a SQLite DB...
        db = sqlite3.connect(':memory:')
        db.row_factory = sqlite3.Row
    except sqlite3.DatabaseError as e:
        raise "Starting SQLite" from e

    cur = db.cursor()

    r = cur.execute("SELECT sqlite_version()").fetchone()
    click.echo("SQLite version: %s" % r[0], color=ctx.color)

    try:
        load_spatialite(db)
    except sqlite3.Error as e:
        raise "Loading Spatialite" from e

    r = cur.execute("SELECT spatialite_version(), HasGeoPackage()").fetchone()
    click.echo("Spatialite version: %s\nSpatialite GeoPackage support? %s" % (r[0], bool(r[1])), color=ctx.color)

    ctx.exit()


@click.group()
@click.option('-v', '--verbosity', type=click.IntRange(0, 2, clamp=True), default=1, is_eager=True, callback=config_logging)
@click.option('-V', '--version', callback=version, is_eager=True, is_flag=True, help='Show the version and exit.', expose_value=False)
@click.pass_context
def cli(ctx, verbosity):
    ctx.obj = box.Box()

    ctx.obj.verbosity = verbosity


@cli.command()
@click.argument('gpkg', type=click.Path(exists=True, allow_dash=False, writable=True), callback=validate_geopackage)
@click.option('--api-key', prompt=True, hide_input=True, required=True)
@click.pass_context
def init(ctx, gpkg, api_key):
    """ Start using a GeoPackage downloaded from Kx """
    try:
        init_db(gpkg, api_key)
        click.echo(f"Sucessfully initialised {gpkg} for syncing")
    except UserError as e:
        L.debug("Error during init_db", exc_info=True)
        click.echo(e)
        sys.exit(1)


@cli.command()
@click.argument('gpkg', type=click.Path(exists=True, allow_dash=False, writable=True), callback=validate_geopackage)
@click.option('--to', callback=validate_timestamp)
@click.pass_context
def sync(ctx, to, gpkg):
    """ Sync with Kx """
    try:
        sync_db(gpkg, date_to=to, verbosity=ctx.obj.verbosity)
        click.echo(f"Sucessfully synced {gpkg}")
    except (UserError, SyncError) as e:
        L.debug("Error during sync_db", exc_info=True)
        click.echo(e)
        sys.exit(1)
