import logging
import re
import sys
import time

import click

from .exceptions import NotFound
from .output_util import dump_json_output
from kart.cli_util import KartCommand

L = logging.getLogger("kart.query")


@click.command("query", hidden=True, cls=KartCommand)
@click.pass_context
@click.argument("path")
@click.argument(
    "command",
    type=click.Choice(("get", "geo-nearest", "geo-intersects", "geo-count", "index")),
    required=True,
)
@click.argument("params", nargs=-1, required=False)
def query(ctx, path, command, params):
    """
    Find features in a Dataset

    WARNING: Spatial indexing is a proof of concept.
    Significantly, indexes don't update when the repo changes in any way.
    """
    repo = ctx.obj.repo
    dataset = repo.datasets()[path]

    if command == "index":
        USAGE = "index"

        t0 = time.monotonic()
        dataset.build_spatial_index(dataset.table_name)
        t1 = time.monotonic()
        L.debug("Indexed {dataset} in %0.3fs", t1 - t0)
        return

    try:
        dataset.get_spatial_index(dataset.table_name)
    except OSError:
        raise NotFound("No spatial index found. Run `kart query {path} index`")

    if command == "get":
        USAGE = "get PK"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        t0 = time.monotonic()
        results = dataset.get_feature(params[0])
        t1 = time.monotonic()

    elif command == "geo-nearest":
        USAGE = "geo-nearest X0,Y0[,X1,Y1] [LIMIT]"
        if len(params) < 1 or len(params) > 2:
            raise click.BadParameter(USAGE)
        elif len(params) > 1:
            limit = int(params[1])
        else:
            limit = 1

        coordinates = [float(c) for c in re.split(r"[ ,]", params[0])]
        if len(coordinates) not in (2, 4):
            raise click.BadParameter(USAGE)

        index = dataset.get_spatial_index(path)
        t0 = time.monotonic()
        results = [dataset.get_feature(pk) for pk in index.nearest(coordinates, limit)]
        t1 = time.monotonic()

    elif command == "geo-intersects":
        USAGE = "geo-intersects X0,Y0,X1,Y1"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        coordinates = [float(c) for c in re.split(r"[ ,]", params[0])]
        if len(coordinates) != 4:
            raise click.BadParameter(USAGE)

        index = dataset.get_spatial_index(path)
        t0 = time.monotonic()
        results = [dataset.get_feature(pk) for pk in index.intersection(coordinates)]
        t1 = time.monotonic()

    elif command == "geo-count":
        USAGE = "geo-count X0,Y0,X1,Y1"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        coordinates = [float(c) for c in re.split(r"[ ,]", params[0])]
        if len(coordinates) != 4:
            raise click.BadParameter(USAGE)

        index = dataset.get_spatial_index(path)
        t0 = time.monotonic()
        results = index.count(coordinates)
        t1 = time.monotonic()

    else:
        raise NotImplementedError(f"Unknown command: {command}")

    L.debug("Results in %0.3fs", t1 - t0)
    t2 = time.monotonic()
    dump_json_output(results, sys.stdout)
    L.debug("Output in %0.3fs", time.monotonic() - t2)
