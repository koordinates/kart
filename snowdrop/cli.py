import logging
import sqlite3
import sys

import box
import click
import dateutil.parser
import dateutil.tz

from .main import check_geopackage, init_db, sync_db, UserError, SyncError


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


@click.group()
@click.option('-v', '--verbosity', type=click.IntRange(0, 2, clamp=True), default=1)
@click.pass_context
def cli(ctx, verbosity):
    ctx.obj = box.Box()

    if verbosity == 2:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s:%(lineno)d %(levelname)s %(message)s')
    elif verbosity == 1:
        logging.basicConfig(level=logging.INFO, format='%(message)s')
    elif verbosity == 1:
        logging.basicConfig(level=logging.ERROR, format='%(message)s')
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
