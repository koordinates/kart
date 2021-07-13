import collections
import functools
import logging

import click

from .geometry import Geometry

L = logging.getLogger("kart.spatial_filters")


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
        if filter_geometry is not None:
            self.filter_ogr = filter_geometry.to_ogr()
            self.filter_env = self.filter_ogr.GetEnvelope()
        else:
            self.filter_ogr = None
            self.filter_env = None

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
                # Don't call envelope() with calculate_if_missing=True - calculating the envelope
                # involves loading it into OGR, which we don't want to do twice (see below).
                feature_env = feature_geometry.envelope(only_2d=True)

                # Envelope might be missing (for POINT geometries, or, for unknown reasons).
                # In this case, we use OGR to calculate it, but we keep the OGR geometry too.
                if feature_env is None:
                    feature_ogr = feature_geometry.to_ogr()
                    feature_env = feature_ogr.GetEnvelope()

                if not bbox_intersects_fast(self.filter_env, feature_env):
                    # Geometries definitely don't intersect if envelopes don't intersect.
                    return False
            except Exception as e:
                L.warn(e)
                err = e

        # Slow check - geometry intersects geometry?
        try:
            if feature_ogr is None:
                feature_ogr = feature_geometry.to_ogr()
            return self.filter_ogr.Intersects(feature_ogr)
        except Exception as e:
            L.warn(e)
            err = e

        # If we fail to apply the spatial filter - perhaps the geometry is corrupt? - we assume it matches.
        click.echo(f"Error applying spatial filter to geometry:\n{err}", err=True)
        return True


SpatialFilter.MATCH_ALL = SpatialFilter(None, match_all=True)


def _range_overlaps(range1_tuple, range2_tuple):
    (a1, a2) = range1_tuple
    (b1, b2) = range2_tuple
    if a1 > a2 or b1 > b2:
        raise ValueError(
            "I was passed a range that didn't make sense: (%r, %r), (%r, %r)"
            % (a1, a2, b1, b2)
        )
    if b1 < a1:
        # `b` starts to the left of `a`, so they intersect if `b` finishes to the right of where `a` starts.
        return b2 > a1
    elif a1 < b1:
        # `a` starts to the left of `b`, so they intersect if `a` finishes to the right of where `b` starts.
        return a2 > b1
    else:
        # They both have the same left edge, so they must intersect unless one of them is zero-width.
        return b2 != b1 and a2 != a1


def bbox_intersects_fast(a, b):
    """
    Takes two four-tuples representing extents, and returns whether their interiors intersect.
    Also handles multiboxes (two tuples each containing zero-or-more four-tuples)
    Intended for use when doing intersections of loads of mathematically-created boxes, avoiding
    the substantial overhead of creating geometry objects and then calling .intersects() on them.
    Don't use this if you've already got GEOSGeometry objects - calling .extent on each and then calling
    this is unlikely to help with performance.
    Don't call this for global projections unless you're certain that the boxes inhabit the same world
    (since we don't/can't wrap boxes across the antimeridian here)
    """
    if (not len(a)) or isinstance(a[0], collections.Iterable):
        # a is a multi-box
        return any(bbox_intersects_fast(a_item, b) for a_item in a)
    if (not len(b)) or isinstance(b[0], collections.Iterable):
        # b is a multi-box
        return any(bbox_intersects_fast(a, b_item) for b_item in b)
    return _range_overlaps((a[0], a[2]), (b[0], b[2])) and _range_overlaps(
        (a[1], a[3]), (b[1], b[3])
    )
