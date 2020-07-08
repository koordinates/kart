import datetime
import json
import logging
import re
import sys
import time
import types

import click
from osgeo import ogr

from . import structure
from .exceptions import NotFound


L = logging.getLogger("sno.query")


def _json_encode_default(o):
    if isinstance(o, types.GeneratorType):
        return list(o)

    if isinstance(o, ogr.Geometry):
        return json.loads(o.ExportToJson())

    if isinstance(o, (datetime.date, datetime.datetime, datetime.time)):
        return o.isoformat()

    raise TypeError(f"Object of type {type(o)} is not JSON serializable")


@click.command("query", hidden=True)
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
    rs = structure.RepositoryStructure(repo)
    dataset = rs[path]

    if command == "index":
        USAGE = "index"

        t0 = time.monotonic()
        dataset.build_spatial_index(dataset.name)
        t1 = time.monotonic()
        L.debug("Indexed {dataset} in %0.3fs", t1 - t0)
        return

    try:
        dataset.get_spatial_index(dataset.name)
    except OSError:
        raise NotFound("No spatial index found. Run `sno query {path} index`")

    if command == "get":
        USAGE = "get PK"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        # need to get the type correct here, otherwise it won't be found after msgpack encoding
        if dataset.primary_key_type == "INTEGER":
            lookup = int(params[0])
        else:
            lookup = params[0]

        t0 = time.monotonic()
        results = dataset.get_feature(lookup)
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
    json_params = {
        "indent": 2 if sys.stdout.isatty() else None,
    }
    t2 = time.monotonic()
    json.dump(results, sys.stdout, default=_json_encode_default, **json_params)
    L.debug("Output in %0.3fs", time.monotonic() - t2)
