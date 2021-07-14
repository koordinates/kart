import collections
import functools
import logging

import click

from .exceptions import InvalidOperation, NotYetImplemented
from .geometry import Geometry, make_crs

L = logging.getLogger("kart.spatial_filters")


class SpatialFilter:
    @classmethod
    @functools.lru_cache()
    def from_spec(cls, geometry_wkt, crs_spec):
        if not geometry_wkt:
            return SpatialFilter.MATCH_ALL

        filter_geometry_ogr = Geometry.from_wkt(geometry_wkt).to_ogr()
        if crs_spec is not None:
            try:
                crs = make_crs(crs_spec)
            except RuntimeError as e:
                raise click.BadParameter(
                    f"Invalid or unknown coordinate reference system configured in spatial filter: {crs_spec!r} ({e})"
                )
        else:
            crs = None

        return SpatialFilter(filter_geometry_ogr, crs)

    def __init__(self, filter_geometry_ogr, crs, match_all=False):
        """
        Create a new spatial filter.
        filter_geometry_ogr - The shape of the spatial filter. An OGR Geometry object.
        crs - The CRS used to interpret the spatial filter. An OGR SpatialReference object.
        match_all - if True, this filter is the default match-everything filter.
        """
        self.filter_ogr = filter_geometry_ogr
        if self.filter_ogr is not None:
            self.filter_env = self.filter_ogr.GetEnvelope()
        else:
            self.filter_env = None

        self.crs = crs
        self.match_all = match_all

    def __contains__(self, feature_geometry):
        if self.match_all or self.filter_ogr is None or feature_geometry is None:
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
                raise
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

    def transform_for_dataset(self, dataset):
        if self.match_all or self.filter_ogr is None or self.crs is None:
            return self

        ds_path = dataset.path
        ds_crs_defs = dataset.crs_definitions()
        if not ds_crs_defs:
            return self
        if len(ds_crs_defs) > 1:
            raise NotYetImplemented(
                f"Sorry, spatial filtering dataset {ds_path!r} with multiple CRS is not yet supported"
            )
        ds_crs_def = list(ds_crs_defs.values())[0]
        return self.transform_for_crs_def(ds_crs_def, ds_path)

    def transform_for_crs_def(self, crs_def, ds_path=None):
        if self.match_all or self.filter_ogr is None or self.crs is None:
            return self

        from osgeo import osr
        from kart.geometry import make_crs

        try:
            new_crs = make_crs(crs_def)
            transform = osr.CoordinateTransformation(self.crs, new_crs)
            new_filter_ogr = self.filter_ogr.Clone()
            new_filter_ogr.Transform(transform)
            return SpatialFilter(new_filter_ogr, new_crs)

        except RuntimeError as e:
            crs_desc = f"CRS for {ds_path!r}" if ds_path else f"CRS:\n {crs_def!r}"
            raise InvalidOperation(
                f"Can't reproject spatial filter into {crs_desc}:\n{e}"
            )


SpatialFilter.MATCH_ALL = SpatialFilter(None, None, match_all=True)


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
    Given two bounding boxes in the form (min-x, max-x, min-y, max-y) - returns True if the bounding boxes overlap.
    """
    return _range_overlaps((a[0], a[1]), (b[0], b[1])) and _range_overlaps(
        (a[2], a[3]), (b[2], b[3])
    )
