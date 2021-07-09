import functools

import click

from .geometry import Geometry


class SpatialFilter:
    @classmethod
    @functools.lru_cache()
    def from_spec(cls, spec):
        if not spec:
            return SpatialFilter.MATCH_ALL

        # Could also accept WKT here if that is useful.
        filter_geometry = Geometry.from_hex_wkb(spec).normalise()
        return SpatialFilter(filter_geometry)

    def __init__(self, filter_geometry, match_all=False):
        self.filter_geometry = filter_geometry
        if filter_geometry:
            self.filter_env = filter_geometry.envelope_2d_as_ogr()
            self.filter_ogr = filter_geometry.to_ogr()

        self.match_all = match_all

    def __contains__(self, feature_geometry):
        if self.match_all or self.filter_geometry is None or feature_geometry is None:
            return True

        err = None
        feature_env = None
        feature_ogr = None

        # Quick check - envelope intersects envelope?
        if self.filter_env is not None:
            try:
                feature_env = feature_geometry.envelope_2d_as_ogr()
                # Envelope might be missing (for POINT geometries, or, for unknown reasons)
                if feature_env is None:
                    feature_env = feature_ogr = feature_geometry.to_ogr()

                if not self.filter_env.Intersects(feature_env):
                    # Geometries definitely don't intersect if envelopes don't intersect.
                    return False
            except Exception as e:
                err = e

        # Slow check - geometry intersects geometry?
        try:
            if feature_ogr is None:
                feature_ogr = feature_geometry.to_ogr()
            return self.filter_ogr.Intersects(feature_ogr)
        except Exception as e:
            err = e

        # If we fail to apply the spatial filter - perhaps the geometry is corrupt? - we assume it matches.
        click.echo(f"Error applying spatial filter to geometry:\n{err}")
        return True


SpatialFilter.MATCH_ALL = SpatialFilter(None)
