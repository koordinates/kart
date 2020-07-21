import cProfile  # noqa
from http.server import (  # noqa
    HTTPServer,
    ThreadingHTTPServer,
    SimpleHTTPRequestHandler,
)
import re
import threading
import time
from urllib.parse import parse_qsl, urlparse


import click

from osgeo import ogr, osr

from sno.exceptions import NotFound
from sno.geometry import gpkg_geom_to_ogr
from sno.structure import RepositoryStructure
from sno.pyvendor.vector_tile_base import VectorTile, PolygonFeature

_PATH_SCHEME = re.compile(r'^/(?P<z>[\d-]+)/(?P<x>[\d-]+)/(?P<y>[\d-]+).pbf$')


MAP_SIZE = 40075016.685578
MAP_OFFSET = MAP_SIZE / 2


def spherical_mercator_bbox(z, ix, iy):
    num_tiles = 2 ** z
    resolution = MAP_SIZE / num_tiles
    x = ix * resolution - MAP_OFFSET
    y = MAP_OFFSET - iy * resolution
    return (x, y - resolution, x + resolution, y)


def SRS(srid):
    srs = osr.SpatialReference()
    srs.ImportFromEPSG(int(srid))
    srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return srs


def make_handler(rs, datasets):
    repo_path = rs.repo.path
    map_srs = SRS(3857)
    d2m_transforms = {}
    m2d_transforms = {}
    # libspatialindex doesn't appear to be thread safe.
    # sharing these across threads in a ThreadingHTTPServer causes segfaults.
    spatial_indexes = threading.local()

    for dataset in datasets:
        dataset_srid = dataset.get_meta_item('gpkg_spatial_ref_sys')[0]['srs_id']
        dataset_srs = SRS(dataset_srid)
        d2m_transforms[dataset.name] = osr.CoordinateTransformation(
            dataset_srs, map_srs
        )
        m2d_transforms[dataset.name] = osr.CoordinateTransformation(
            map_srs, dataset_srs
        )

    class VTilesRequestHandler(SimpleHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'
        DEFAULT_HEADERS = {
            'Access-Control-Allow-Origin': '*',
            'Content-Type': 'application/vnd.mapbox-vector-tile',
        }

        def response(self, code, body, **headers):
            # make things nicer. http.server is *weird*
            self.send_response_only(code, None)
            self.send_header('Server', self.version_string())
            self.send_header('Date', self.date_time_string())
            for k, v in self.DEFAULT_HEADERS.items():
                self.send_header(k, v)
            for k, v in headers.items():
                self.send_header(k.replace('_', '-'), v)
            self.end_headers()
            self.wfile.write(body)

            profile_bits = ''
            try:
                profile_bits = f"{self.num_features} features"
            except AttributeError:
                pass
            try:
                request_time = time.monotonic() - self.request_start_time
                profile_bits += f" / {request_time:.3f}s"
            except AttributeError:
                pass

            self.log_request(
                code, size=f'{len(body)}   [{profile_bits}]',
            )

        def scale_points_list(self, tile_bbox, points_list, tile_grid_size=4096):
            tile_width = tile_bbox[2] - tile_bbox[0]
            tile_height = tile_bbox[3] - tile_bbox[1]

            tile_res_x = tile_grid_size / tile_width
            tile_res_y = -tile_grid_size / tile_height
            return [
                [(x - tile_bbox[0]) * tile_res_x, (y - tile_bbox[3]) * tile_res_y]
                for (x, y) in points_list
            ]

        def add_feature(self, vlayer, ogr_geom, tile_bbox):
            """
            Add a feature to the given vector tile layer, in a tile with the given bbox.
            """
            gt = ogr_geom.GetGeometryType()
            if ogr.GT_Flatten(gt) != gt:
                raise NotImplementedError("Z/M not implemented")
            if gt == ogr.wkbPoint:
                feature = vlayer.add_point_feature()
                feature.add_points(
                    self.scale_points_list(tile_bbox, [ogr_geom.GetPoint_2D(0)])
                )
            elif gt == ogr.wkbMultiPoint:
                feature = vlayer.add_point_feature()
                for i in range(ogr_geom.GetGeometryCount()):
                    # get point
                    g = ogr_geom.GetGeometryRef(i)
                    feature.add_points(
                        self.scale_points_list(tile_bbox, [g.GetPoint_2D(0)])
                    )

            elif gt == ogr.wkbLineString:
                feature = vlayer.add_line_string_feature()
                # handle multilinestrings
                ogr_geom = ogr.ForceToMultiLineString(ogr_geom)
                for i in range(ogr_geom.GetGeometryCount()):
                    # get linestring
                    g = ogr_geom.GetGeometryRef(i)
                    feature.add_line_string(
                        self.scale_points_list(tile_bbox, g.GetPoints())
                    )

            elif gt in (ogr.wkbPolygon, ogr.wkbMultiPolygon):
                feature = vlayer.add_polygon_feature()
                # handle multipolygons
                ogr_geom = ogr.ForceToMultiPolygon(ogr_geom)
                for i in range(ogr_geom.GetGeometryCount()):
                    # get polygon
                    g = ogr_geom.GetGeometryRef(i)
                    for j in range(g.GetGeometryCount()):
                        # get ring
                        ring = g.GetGeometryRef(j)
                        points = self.scale_points_list(tile_bbox, ring.GetPoints())

                        # hack: vector_tiles_base apparently expects the exterior ring to be *anticlockwise*
                        # (and the interior rings to be *clockwise*. !!?)
                        # (that's the reverse of what the vector tiles spec and OGC both expect)
                        need_clockwise = j != 0
                        if (
                            PolygonFeature._is_ring_clockwise(None, points)
                            != need_clockwise
                        ):
                            points.reverse()
                        feature.add_ring(points)
            else:
                raise ValueError(f"Unknown geometry type: {gt}")
            return feature

        def make_vector_tile(self, z, x, y):
            print(f"starting vector tile {self.path}")
            minx, miny, maxx, maxy = spherical_mercator_bbox(z, x, y)
            tile_box_geom = ogr.CreateGeometryFromWkt(
                f'POLYGON(({minx} {miny},{minx} {maxy},{maxx} {maxy},{maxx} {miny},{minx} {miny}))'
            )

            vtile = VectorTile()
            if 'debug_bboxes' in self.GET:
                # add a box around the whole tile, so it's easy to see where the tile edges are
                vlayer = vtile.add_layer('debug', version=3)
                feature = vlayer.add_polygon_feature()
                feature.add_ring([[0, 0], [4096, 0], [4096, 4096], [0, 4096], [0, 0]])

            num_features = 0
            for dataset in datasets:
                tile_box_transformed = tile_box_geom.Clone()
                tile_box_transformed.Transform(m2d_transforms[dataset.name])
                # ogr has a weird envelope order...
                w, e, s, n = tile_box_transformed.GetEnvelope()

                vlayer = vtile.add_layer(dataset.name, version=3)
                try:
                    index = getattr(spatial_indexes, dataset.name)
                except AttributeError:
                    index = dataset.get_spatial_index(repo_path)
                    setattr(
                        spatial_indexes, dataset.name, index,
                    )

                pks = list(index.intersection((w, s, e, n)))
                num_features += len(pks)
                for pk in pks:
                    if 'include_attributes' in self.GET or 'get_feature' in self.GET:
                        f = dataset.get_feature((pk,))
                        geom = f.pop(dataset.geom_column_name)
                    else:
                        geom = dataset.get_geometry((pk,))

                    # reproject geom to spherical merc
                    if geom is None:
                        continue

                    geom = gpkg_geom_to_ogr(geom)
                    geom.Transform(d2m_transforms[dataset.name])

                    feature = self.add_feature(vlayer, geom, (minx, miny, maxx, maxy))
                    if 'include_attributes' in self.GET:
                        feature.attributes = f
                    feature.id = f"{dataset.name}::{pk}"
            self.num_features = num_features
            return vtile.serialize()

        def serve_tile(self, z, x, y):
            if 'cprofile' in self.GET:
                cProfile.runctx(
                    "self.vtile_data = self.make_vector_tile(int(z), int(x), int(y))",
                    globals(),
                    locals(),
                    sort='cumtime',
                )
                vtile_data = self.vtile_data
            else:
                vtile_data = self.make_vector_tile(int(z), int(x), int(y))
            return self.response(200, vtile_data, content_length=str(len(vtile_data)))

        def do_GET(self):
            self.request_start_time = time.monotonic()

            # oddly, `self.path` includes the querystring...
            self.url_parsed = urlparse(self.path)
            self.actual_path = self.url_parsed.path
            self.GET = dict(parse_qsl(self.url_parsed.query, keep_blank_values=True))

            m = _PATH_SCHEME.match(self.actual_path)
            if m:
                z, x, y = m.groups()
                return self.serve_tile(z, x, y)
            # serve assets, or 404s
            return super().do_GET()

    return VTilesRequestHandler


@click.group()
@click.pass_context
def vtiles(ctx):
    ...


@vtiles.command("serve")
@click.pass_context
@click.option('--port', default=8000, type=click.INT, help="Which port to listen on")
@click.option(
    '--host',
    default='0.0.0.0',
    help='Which IP to listen on. Defaults to all interfaces',
)
@click.argument("revision")
@click.argument(
    "tables", nargs=-1,
)
def serve(ctx, host, port, revision, tables):
    """
    Serves vector tiles from the given dataset(s)
    """
    rs = RepositoryStructure.lookup(ctx.obj.repo, revision)
    if rs.version != 2:
        raise NotImplementedError(f"Can only handle v2 repos, this one is {rs.version}")
    datasets = []
    if tables:
        for n in tables:
            try:
                ds = rs[n]
            except KeyError:
                raise NotFound(f"Not found: {n}")
            if not ds.has_geometry:
                raise click.UsageError(f"Dataset is aspatial: {n}")
            datasets.append(ds)
    else:
        datasets = list(ds for ds in rs if ds.has_geometry)

    for ds in datasets:
        index = ds.get_spatial_index(rs.repo.path)
        if index is None:
            click.echo(f"Building spatial index for {ds.name} @ {revision}")
            ds.build_spatial_index(rs.repo.path)
        else:
            click.echo(f"Found existing spatial index for {ds.name} @ {revision}")
            index = None

    server_address = (host, port)

    click.secho(f"Serving vector tiles on {host}:{port}", bold=True)

    handler = make_handler(rs, datasets)

    httpd = ThreadingHTTPServer(server_address, handler)
    # for debugging with pdb, use this
    # httpd = HTTPServer(server_address, handler)
    httpd.serve_forever()


@vtiles.command("decode")
@click.argument("file", type=click.File(mode='rb'))
def decode(file):
    vt = VectorTile(file.read())

    def output():
        for l in vt.layers:
            yield f"{l.name}\n"
            for f in l.features:
                yield click.style(f"    {f.id}\n", bold=True)
                yield click.style(
                    f"        {f.type:>20}: {f.get_geometry()}\n", fg='blue'
                )
                for k in f.attributes:
                    v = f.attributes[k]
                    yield (f"        {k:>20}: {v}\n")
                yield '\n'

    click.echo_via_pager(output())
