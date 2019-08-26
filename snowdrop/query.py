import datetime
import json
import logging
import os
import re
import sys
import time
import types

import click
import pygit2
from osgeo import ogr

from . import structure


L = logging.getLogger('snowdrop.query')


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
    type=click.Choice(("get", "geo-nearest", "geo-intersects", "geo-count")),
    required=True
)
@click.argument("params", nargs=-1, required=True)
def query(ctx, path, command, params):
    """
    Find features in a Dataset
    """
    repo = pygit2.Repository(ctx.obj["repo_dir"] or os.curdir)
    rs = structure.RepositoryStructure(repo)
    dataset = rs[path]

    if command == "get":
        USAGE = "get PK"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        # need to get the type correct here, otherwise it won't be found after msgpack encoding
        if dataset.primary_key_type == 'INTEGER':
            lookup = int(params[0])
        else:
            lookup = params[0]

        t0 = time.time()
        results = dataset.get_feature(lookup)[1]
        t1 = time.time()

    elif command == 'geo-nearest':
        USAGE = "geo-nearest X0,Y0[,X1,Y1] [LIMIT]"
        if len(params) < 1 or len(params) > 2:
            raise click.BadParameter(USAGE)
        elif len(params) > 1:
            limit = int(params[1])
        else:
            limit = 1

        coordinates = [float(c) for c in re.split(r'[ ,]', params[0])]
        if len(coordinates) not in (2, 4):
            raise click.BadParameter(USAGE)

        index = dataset.get_spatial_index(path)
        t0 = time.time()
        results = [dataset.get_feature(pk)[1] for pk in index.nearest(coordinates, limit)]
        t1 = time.time()

    elif command == 'geo-intersects':
        USAGE = "geo-intersects X0,Y0,X1,Y1"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        coordinates = [float(c) for c in re.split(r'[ ,]', params[0])]
        if len(coordinates) != 4:
            raise click.BadParameter(USAGE)

        index = dataset.get_spatial_index(path)
        t0 = time.time()
        results = [dataset.get_feature(pk)[1] for pk in index.intersection(coordinates)]
        t1 = time.time()

    elif command == 'geo-count':
        USAGE = "geo-count X0,Y0,X1,Y1"
        if len(params) != 1:
            raise click.BadParameter(USAGE)

        coordinates = [float(c) for c in re.split(r'[ ,]', params[0])]
        if len(coordinates) != 4:
            raise click.BadParameter(USAGE)

        index = dataset.get_spatial_index(path)
        t0 = time.time()
        results = index.count(coordinates)
        t1 = time.time()

    else:
        raise NotImplementedError(f"Unknown command: {command}")

    L.debug("Results in %0.3fs", t1-t0)
    json_params = {
        'indent': 2 if sys.stdout.isatty() else None,
    }
    t2 = time.time()
    json.dump(results, sys.stdout, default=_json_encode_default, **json_params)
    L.debug("Output in %0.3fs", time.time()-t2)
