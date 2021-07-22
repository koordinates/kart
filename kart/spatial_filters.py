import functools
import logging

import click

from .crs_util import make_crs, CoordinateReferenceString
from .exceptions import CrsError
from .geometry import GeometryString, geometry_from_string

L = logging.getLogger("kart.spatial_filters")


# TODO(https://github.com/koordinates/kart/issues/456) - need to handle the following issues:
# - make sure long polygon edges are segmented into short lines before reprojecting, so that the
# geographical location of the middle of the polygon's edge doesn't change
# - handle anti-meridians appropriately, particularly the case where the spatial filter crosses the anti-meridian
# - handle the case where the spatial filter cannot or can only partially be projected to the target CRS


def spatial_filter_options():
    """
    A decorator that can be added to a command to add "--spatial-filter" and "--spatial-filter-crs" options.
    These can then be converted to a spatial filter using:
    SpatialFilter.from_cli_opts(spatial_filter, spatial_filter_crs)
    """

    def decorator(f):
        options = (
            click.option(
                "--spatial-filter",
                type=GeometryString(encoding="utf-8"),
                help=(
                    "Specify a spatial filter geometry to restrict this repository for working on features that "
                    "intersect that geometry - features outside this area are not shown. Both the user and "
                    "computer can benefit by not thinking about features outside the area of interest. The "
                    "geometry should be supplied using WKT or hex-encoded WKB, and should be in EPSG:4326 "
                    "unless otherwise specified using --spatial-filter-crs."
                ),
            ),
            click.option(
                "--spatial-filter-crs",
                type=CoordinateReferenceString(encoding="utf-8", keep_as_string=True),
                default=SpatialFilter.DEFAULT_CRS_SPEC,
                help=(
                    "The coordinate reference system that the --spatial-filter geometry is using. Can be a short name "
                    "such as EPSG:4326, or a full WKT definition."
                ),
            ),
        )

        for opt in options:
            f = opt(f)
        return f

    return decorator


class SpatialFilter:
    """
    Responsible for deciding whether a feature or feature-geometry does or does not match the user's specified area.
    A spatial filter has a particular CRS, and so should be applied to geometries with a matching CRS.
    A spatial filter can only be used on entire features if it is configured with the name of the geometry column.
    Each spatial filter is immutable object. To get a spatial filter for a particular CRS or dataset,
    call SpatialFilter.transform_for_dataset or SpatialFilter.transform_for_crs
    """

    @property
    def is_original(self):
        # Overridden by OriginalSpatialFilter.
        return False

    DEFAULT_CRS_SPEC = "EPSG:4326"

    @classmethod
    def from_repo_config(cls, repo):
        from kart.repo import KartConfigKeys

        geometry_spec = repo.get_config_str(KartConfigKeys.KART_SPATIALFILTER_GEOMETRY)
        crs_spec = repo.get_config_str(KartConfigKeys.KART_SPATIALFILTER_CRS)
        return SpatialFilter.from_config_values(geometry_spec, crs_spec)

    @classmethod
    @functools.lru_cache()
    def from_config_values(cls, geometry_spec, crs_spec):
        if not geometry_spec:
            return OriginalSpatialFilter._MATCH_ALL
        if not crs_spec:
            crs_spec = cls.DEFAULT_CRS_SPEC

        geometry = geometry_from_string(geometry_spec, context="spatial filter")

        return OriginalSpatialFilter(geometry, crs_spec)

    @classmethod
    def from_cli_opts(cls, geometry, crs_spec):
        if geometry is None:
            return OriginalSpatialFilter._MATCH_ALL
        if not crs_spec:
            crs_spec = cls.DEFAULT_CRS_SPEC

        return OriginalSpatialFilter(geometry, crs_spec)

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


class OriginalSpatialFilter(SpatialFilter):
    """
    The OriginalSpatialFilter is a spatial filter that the user specified, with a particular geometry and
    a particular CRS. Unlike its parent class, the SpatialFilter, the OriginalSpatialFilter can be
    transformed to have a new CRS - but the result is a normal SpatialFilter, not an "Original".

    Normal SpatialFilters cannot be transformed - since transformation may be lossy, transforming a non-original
    SpatialFilter may lead to extra data loss which could be avoided by only ever transforming the original.
    That is why only OriginalSpatialFilter supports transformation.

    The OriginalSpatialFilter also keeps the CRS specification, in the original form supplied by the user,
    and it can be written to the config (the specification will be written to the config in its original form).
    """

    @property
    def is_original(self):
        return True

    def __init__(self, geometry, crs_spec, match_all=None):
        self.match_all = match_all

        if match_all:
            super().__init__(None, None, match_all=True)
            self.geometry = None
            self.crs_spec = None
        else:
            super().__init__(geometry.to_ogr(), make_crs(crs_spec))
            self.geometry = geometry
            self.crs_spec = crs_spec

    def write_config(self, repo):
        from kart.repo import KartConfigKeys

        geom_key = KartConfigKeys.KART_SPATIALFILTER_GEOMETRY
        crs_key = KartConfigKeys.KART_SPATIALFILTER_CRS

        if self.match_all:
            repo.del_config(geom_key)
            repo.del_config(crs_key)
        else:
            repo.config[geom_key] = self.geometry.to_wkt()
            repo.config[crs_key] = self.crs_spec

    def transform_for_dataset(self, dataset):
        """Transform this spatial filter so that it matches the CRS (and geometry column name) of the given dataset."""
        if self.match_all:
            return SpatialFilter._MATCH_ALL

        if not dataset.geom_column_name:
            return SpatialFilter._MATCH_ALL

        ds_path = dataset.path
        ds_crs_defs = dataset.crs_definitions()
        if not ds_crs_defs:
            return self
        if len(ds_crs_defs) > 1:
            raise CrsError(
                f"Sorry, spatial filtering dataset {ds_path!r} with multiple CRS is not yet supported"
            )
        ds_crs_def = list(ds_crs_defs.values())[0]

        return self.transform_for_schema_and_crs(dataset.schema, ds_crs_def, ds_path)

    def transform_for_schema_and_crs(self, schema, crs, ds_path=None):
        """
        Similar to transform_for_dataset above, but can also be used without a dataset object - for example,
        to apply the spatial filter to a working copy table which might not exactly match any dataset.

        schema - the dataset (or table) schema.
        new_crs - the crs definition of the dataset or table.
            The CRS should be a name eg EPSG:4326, or a full CRS definition, or an osgeo.osr.SpatialReference.
        """
        if self.match_all:
            return SpatialFilter._MATCH_ALL

        geometry_columns = schema.geometry_columns
        if not geometry_columns:
            return SpatialFilter._MATCH_ALL
        new_geom_column_name = geometry_columns[0].name

        from osgeo import osr

        try:
            crs_spec = str(crs)
            if isinstance(crs, str):
                crs = make_crs(crs)
            transform = osr.CoordinateTransformation(self.crs, crs)
            new_filter_ogr = self.filter_ogr.Clone()
            new_filter_ogr.Transform(transform)
            return SpatialFilter(new_filter_ogr, crs, new_geom_column_name)

        except RuntimeError as e:
            crs_desc = f"CRS for {ds_path!r}" if ds_path else f"CRS:\n {crs_spec!r}"
            raise CrsError(f"Can't reproject spatial filter into {crs_desc}:\n{e}")


# A SpatialFilter object that matches everything.
SpatialFilter._MATCH_ALL = SpatialFilter(None, None, match_all=True)

# An OriginalSpatialFilter object that matches everything, and, which has the "transform_for_*" methods:
OriginalSpatialFilter._MATCH_ALL = OriginalSpatialFilter(None, None, match_all=True)

# Code outside this package can use "SpatialFilter.MATCH_ALL" as a default if the user hasn't specified anything else.
# We actually map this to OriginalSpatialFilter._MATCH_ALL in case it still needs to be transformed.

SpatialFilter.MATCH_ALL = OriginalSpatialFilter._MATCH_ALL


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
