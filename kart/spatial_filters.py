import functools
import logging

import click

from .exceptions import InvalidOperation, NotYetImplemented
from .geometry import Geometry, make_crs

L = logging.getLogger("kart.spatial_filters")


# TODO(https://github.com/koordinates/kart/issues/456) - need to handle the following issues:
# - make sure long polygon edges are segmented into short lines before reprojecting, so that the
# geographical location of the middle of the polygon's edge doesn't change
# - handle anti-meridians appropriately, particularly the case where the spatial filter crosses the anti-meridian
# - handle the case where the spatial filter cannot or can only partially be projected to the target CRS


class SpatialFilter:
    """
    Responsible for deciding whether a feature or feature-geometry does or does not match the user's specified area.
    A spatial filter has a particular CRS, and so should be applied to geometries with a matching CRS.
    A spatial filter can only be used on entire features if it is configured with the name of the geometry column.
    Each spatial filter is immutable object. To get a spatial filter for a particular CRS or dataset,
    call SpatialFilter.transform_for_dataset or SpatialFilter.transform_for_crs
    """

    @classmethod
    @functools.lru_cache()
    def from_spec(cls, geometry_wkt, crs_spec):
        if not geometry_wkt:
            return SpatialFilter.MATCH_ALL
        if not crs_spec:
            raise ValueError("SpatialFilter requires a CRS")

        # TODO - use PreparedGeometry.
        filter_geometry_ogr = Geometry.from_wkt(geometry_wkt).to_ogr()
        try:
            crs = make_crs(crs_spec)
        except RuntimeError as e:
            raise click.BadParameter(
                f"Invalid or unknown coordinate reference system configured in spatial filter: {crs_spec!r} ({e})"
            )

        return SpatialFilter(filter_geometry_ogr, crs)

    def __init__(
        self, filter_geometry_ogr, crs, geom_column_name=None, match_all=False
    ):
        """
        Create a new spatial filter.
        filter_geometry_ogr - The shape of the spatial filter. An OGR Geometry object.
        crs - The CRS used to interpret the spatial filter. An OGR SpatialReference object.
        match_all - if True, this filter is the default match-everything filter.
        """
        self.match_all = match_all

        if match_all:
            self.filter_ogr = self.filter_env = self.crs = None
            self.geom_column_name = None
        else:
            self.filter_ogr = filter_geometry_ogr
            self.filter_env = self.filter_ogr.GetEnvelope()
            self.crs = crs
            self.geom_column_name = geom_column_name

    def matches(self, feature):
        """
        Returns True if the given feature geometry matches this spatial filter.
        The feature to be tested is assumed to be using the same CRS as this spatial filter,
        otherwise the intersection test makes no sense.
        To get a spatial filter for a particular CRS, see transfrom_for_dataset / transform_for_crs.

        feature_geometry - either a feature dict (in which case self.geom_column_name must be set)
            or a geometry.Geometry object.
        """
        if self.match_all or feature is None:
            return True

        feature_geometry = feature[self.geom_column_name]

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
        """Transform this spatial filter so that it matches the CRS of the given dataset."""
        if self.match_all:
            return SpatialFilter.MATCH_ALL

        geom_column_name = dataset.geom_column_name
        if not geom_column_name:
            return SpatialFilter.MATCH_ALL
        result = self.with_geom_column_name(geom_column_name)

        ds_path = dataset.path
        ds_crs_defs = dataset.crs_definitions()
        if not ds_crs_defs:
            return self
        if len(ds_crs_defs) > 1:
            raise NotYetImplemented(
                f"Sorry, spatial filtering dataset {ds_path!r} with multiple CRS is not yet supported"
            )
        ds_crs_def = list(ds_crs_defs.values())[0]
        return result.transform_for_crs(ds_crs_def, ds_path)

    def with_geom_column_name(self, geom_column_name):
        if self.match_all:
            return SpatialFilter.MATCH_ALL
        return SpatialFilter(self.filter_ogr, self.crs, geom_column_name)

    def transform_for_crs(self, new_crs, ds_path=None):
        """
        Transform this spatial filter so that it matches the given CRS.
        The CRS should be a name eg EPSG:4326, or a full CRS definition, or an osgeo.osr.SpatialReference
        """
        if self.match_all:
            return SpatialFilter.MATCH_ALL

        from osgeo import osr
        from kart.geometry import make_crs

        try:
            crs_spec = str(new_crs)
            if isinstance(new_crs, str):
                new_crs = make_crs(new_crs)
            transform = osr.CoordinateTransformation(self.crs, new_crs)
            new_filter_ogr = self.filter_ogr.Clone()
            new_filter_ogr.Transform(transform)
            return SpatialFilter(new_filter_ogr, new_crs, self.geom_column_name)

        except RuntimeError as e:
            crs_desc = f"CRS for {ds_path!r}" if ds_path else f"CRS:\n {crs_spec!r}"
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
