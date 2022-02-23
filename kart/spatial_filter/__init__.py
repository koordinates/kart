import functools
import logging
import os
import re
import sys
from enum import Enum, auto

import click

from kart.cli_util import StringFromFile, add_help_subcommand
from kart.crs_util import make_crs
from kart.exceptions import (
    NO_SPATIAL_FILTER,
    CrsError,
    GeometryError,
    NotFound,
    NotYetImplemented,
)
from kart.geometry import GeometryType, geometry_from_string
from kart.output_util import dump_json_output
from kart.promisor_utils import object_is_promised
from kart.repo import KartRepoState
from kart.serialise_util import hexhash

L = logging.getLogger("kart.spatial_filter")


# TODO(https://github.com/koordinates/kart/issues/456) - need to handle the following issues:
# - make sure long polygon edges are segmented into short lines before reprojecting, so that the
# geographical location of the middle of the polygon's edge doesn't change
# - handle anti-meridians appropriately, particularly the case where the spatial filter crosses the anti-meridian
# - handle the case where the spatial filter cannot or can only partially be projected to the target CRS


def spatial_filter_help_text(allow_reference=True):
    result = (
        "Specify a spatial filter geometry to restrict this repository for working on features that intersect that"
        "geometry - features outside this area are not shown. Both the user and computer can benefit by not thinking "
        "about features outside the area of interest. It should consist of the CRS name, followed by a semicolon, "
        "followed by a valid Polygon or Multipolygon encoded using WKT or hex-encoded WKB. For example: "
        "EPSG:4326;POLYGON((...)) or EPSG:4269;01030000...\n"
        "Alternatively you may reference a file that contains the data, which should contain either the CRS name "
        "or the entire CRS definition in WKT, followed by a blank line, followed by a valid Polygon or Multipolygon "
        "encoded using WKT or hex-encoded WKB. To reference a file on your filesystem, set this flag to an @ symbol "
        "followed by the file path. For example: @myfile.txt"
    )
    if allow_reference:
        result += (
            "\nTo reference a file that has been checked into this repository, set this flag to its object ID, "
            "or to a git reference that resolves to that object. By convention, references to spatial filters "
            "are kept in a filters subfolder - ie refs/filters/myfilter - and in which case, the refs/filters/ "
            "prefix can be omitted, and this flag can be simply set to the name of the reference."
        )
    return result


@add_help_subcommand
@click.group(hidden=True)
@click.pass_context
def spatial_filter(ctx, **kwargs):
    """
    Extra commands for working with spatial filters. Generally, kart clone and kart checkout are sufficient.
    """


@spatial_filter.command()
@click.option(
    "--envelope",
    "do_envelope",
    is_flag=True,
    default=False,
    help=(
        "Output only the spatial filter's envelope in WGS 84, in the following format: lng_w,lat_s,lng_e,lat_n)"
    ),
)
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.argument("reference")
@click.pass_context
def resolve(ctx, reference, do_envelope, output_format):
    """
    Outputs the spatial filter definition stored in the repo at the given reference.
    """

    do_json = output_format == "json"

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)
    ref_spec = ReferenceSpatialFilterSpec(reference)

    if do_envelope:
        envelope = ref_spec.resolve(repo).envelope_wgs84()
        if do_json:
            dump_json_output(envelope, sys.stdout)
        else:
            click.echo("{0},{1},{2},{3}".format(*envelope))
    else:
        data = ref_spec.resolved_data(repo)
        if do_json:
            from kart.status import spatial_filter_status_to_json

            data = spatial_filter_status_to_json(data)
            dump_json_output(data, sys.stdout)
        else:
            click.echo(data["crs"])
            click.echo()
            click.echo(data["geometry"])


@spatial_filter.command()
@click.option(
    "--clear-existing",
    is_flag=True,
    default=False,
    help="Clear existing index before re-indexing",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Don't do any indexing, instead just output what would be indexed.",
)
@click.option(
    "--debug",
    hidden=True,
    help="Don't do any indexing, instead just calculate the envelope for this feature / encode or decode this envelope.",
)
@click.argument(
    "commits",
    nargs=-1,
)
@click.pass_context
def index(ctx, clear_existing, dry_run, debug, commits):
    """
    Maintains the index needed to perform a spatially-filtered clone using this repo as the server.
    Indexes all features added by the supplied commits and their ancestors.
    If no commits are supplied, indexes all features in all commits.
    """
    from .index import (
        debug_index,
        resolve_all_commit_refs,
        resolve_commits,
        update_spatial_filter_index,
    )

    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    if debug:
        debug_index(repo, debug)
        return

    if not commits:
        commits = resolve_all_commit_refs(repo)
    else:
        commits = resolve_commits(repo, commits)

    update_spatial_filter_index(
        repo,
        commits,
        verbosity=ctx.obj.verbosity + 1,
        clear_existing=clear_existing,
        dry_run=dry_run,
    )


class SpatialFilterString(StringFromFile):
    """Click option to specify a SpatialFilter."""

    def __init__(self, *args, allow_reference=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.allow_reference = allow_reference

    def convert(self, value, param, ctx):
        if not value or value.upper() == "NONE":
            return ResolvedSpatialFilterSpec(None, None, match_all=True)

        try:
            parts = value.split(";", maxsplit=1)
            if len(parts) == 2 and re.fullmatch(
                r"[A-Za-z0-9]{2,10}:[0-9]{1,10}", parts[0]
            ):
                # Inline CRS and geometry definition.
                return ResolvedSpatialFilterSpec(*parts)

            if value.startswith("@"):
                contents = super().convert(value, param, ctx)
                parts = ReferenceSpatialFilterSpec.split_file(contents)
                return ResolvedSpatialFilterSpec(*parts)

            else:
                if not os.environ.get("X_KART_SPATIAL_FILTER_REFERENCE"):
                    raise NotYetImplemented(
                        "Sorry, loading spatial references by reference is not yet supported"
                    )
                if not self.allow_reference:
                    self.fail(
                        "Invalid spatial filter definition - "
                        "should be in the form CRS_AUTHORITY:CRS_ID;GEOMETRY or @FILENAME"
                    )
                # Can't parse this spec in any further detail without the repo - which may not even exist yet:
                # As is the case for eg: kart clone SOURCE --spatial-filter=REFNAME
                return ReferenceSpatialFilterSpec(value)
        except (CrsError, GeometryError) as e:
            self.fail(str(e))


class SpatialFilterSpec:
    """
    A user-provided specification for a spatial filter.
    This is different to the OriginalSpatialFilter (see below) in that it may not yet be resolved or even resolvable
    to a specific geometry and CRS - if the user asks to partially clone a repo by using the spatial filter at
    refs/filters/xyz, then this will be represented as a ReferenceSpatialFilterSpec until such time as we actually
    know what that filter is (note that it is not available locally when we begin the clone).
    """

    def __init__(self):
        from kart.repo import KartConfigKeys

        self.GEOM_KEY = KartConfigKeys.KART_SPATIALFILTER_GEOMETRY
        self.CRS_KEY = KartConfigKeys.KART_SPATIALFILTER_CRS
        self.REF_KEY = KartConfigKeys.KART_SPATIALFILTER_REFERENCE
        self.OID_KEY = KartConfigKeys.KART_SPATIALFILTER_OBJECTID

    def write_config(self, repo, update_remote=None):
        """Writes this user-provied spatial-filter specification to the given repository's config."""
        raise NotImplementedError()

    def resolve(self, repo):
        """
        Returns an equivalent ResolvedSpatialFilterSpec that directly contains the geometry and CRS
        (as opposed to a ReferenceSpatialFilterSpec that contains a reference to some other object
        that in turn contains the geometry and CRS).
        """
        raise NotImplementedError()

    def envelope_wgs84(self):
        """Returns the envelope of the spatial filter geometry as a tuple (lng_w, lat_s, lng_e, lat_n), using WGS 84."""
        raise NotImplementedError()

    def partial_clone_filter_spec(self, specify_in_full=True):
        """Approximates this spatial-filter so it can be parsed by git's custom spatial filter extension."""
        raise NotImplementedError()


class ResolvedSpatialFilterSpec(SpatialFilterSpec):
    """A user-provided specification for a spatial filter where the user has supplied the values directly."""

    def __init__(self, crs_spec, geometry_spec, match_all=False):
        super().__init__()
        self.match_all = match_all
        if not self.match_all:
            self.crs_spec = crs_spec
            self.geometry_spec = geometry_spec
            self.crs = make_crs(crs_spec)
            self.geometry = geometry_from_string(
                geometry_spec,
                allowed_types=(GeometryType.POLYGON, GeometryType.MULTIPOLYGON),
                allow_empty=False,
                context="spatial filter",
            )

    def resolve(self, repo):
        return self

    def write_config(self, repo, update_remote=None):
        if self.match_all:
            self.delete_all_config(repo, update_remote=update_remote)
        else:
            repo.config[self.GEOM_KEY] = self.geometry.to_wkt()
            repo.config[self.CRS_KEY] = self.crs_spec
            repo.del_config(self.REF_KEY)
            repo.del_config(self.OID_KEY)
            if update_remote:
                repo.config[
                    f"remote.{update_remote}.partialclonefilter"
                ] = self.partial_clone_filter_spec(specify_in_full=False)

    def delete_all_config(self, repo, update_remote=None):
        for key in (self.GEOM_KEY, self.CRS_KEY, self.REF_KEY, self.OID_KEY):
            repo.del_config(key)
        if update_remote:
            repo.del_config(f"remote.{update_remote}.partialclonefilter")

    def matches_working_copy(self, repo):
        working_copy = repo.working_copy
        return (
            working_copy is None
            or working_copy.get_spatial_filter_hash() == self.hexhash
        )

    @property
    def hexhash(self):
        if self.match_all:
            return None
        return hexhash(self.crs_spec.strip(), self.geometry.to_wkb())

    def envelope_wgs84(self):
        from osgeo import osr

        try:
            transform = osr.CoordinateTransformation(self.crs, make_crs("EPSG:4326"))
            geom_ogr = self.geometry.to_ogr()
            geom_ogr.Transform(transform)
            w, e, s, n = geom_ogr.GetEnvelope()
            return w, s, e, n

        except RuntimeError as e:
            raise CrsError(f"Can't reproject spatial filter into EPSG:4326:\n{e}")

    def partial_clone_filter_spec(self, specify_in_full=True):
        if self.match_all:
            result = None
        else:
            envelope = self.envelope_wgs84()
            result = "extension:spatial={0},{1},{2},{3}".format(*envelope)
        if specify_in_full:
            result = f"--filter={result}" if result else "--no-filter"
        return result

    def is_within_envelope(self, envelope_wgs84):
        assert len(envelope_wgs84) == 4
        if self.match_all:
            return False
        w, s, e, n = self.envelope_wgs84()
        w_, s_, e_, n_ = envelope_wgs84
        return w >= w_ and s >= s_ and e <= e_ and n <= n_


class ReferenceSpatialFilterSpec(SpatialFilterSpec):
    """
    A user-provided specification for a spatial filter where the user has supplied the values indirectly -
    we need to load an object at a particular object ID or reference to load the spatial filter definition.
    """

    def __init__(self, ref_or_oid):
        super().__init__()
        self.ref_or_oid = ref_or_oid

    @functools.lru_cache(maxsize=1)
    def _resolve_ref_oid_blob(self, repo):
        """
        Returns a tuple (ref, oid, blob), where ref, oid and blob are the reference, object ID and blob
        indicated by self.ref_or_oid (but ref will be None if self.ref_or_oid is an object ID).
        """

        ref = None
        oid = None
        obj = None
        try:
            oid = self.ref_or_oid
            obj = repo[oid]
        except (KeyError, ValueError):
            pass

        if obj is None:
            ref = self.ref_or_oid
            if not ref.startswith("refs/"):
                ref = f"refs/filters/{ref}"

            if ref in repo.references:
                oid = str(repo.references[ref].resolve().target)
            try:
                obj = repo[oid]
            except (KeyError, ValueError):
                pass

        if obj is None or obj.type_str != "blob":
            ref_desc = " or ".join(set([oid, ref]))
            raise NotFound(
                f"No spatial filter object was found in the repository at {ref_desc}",
                exit_code=NO_SPATIAL_FILTER,
            )

        return ref, oid, obj

    def resolved_data(self, repo):
        ref, oid, obj = self._resolve_ref_oid_blob(repo)
        crs, geometry = self.split_file(obj.data.decode("utf-8"))
        return {
            "reference": ref,
            "objectId": oid,
            "geometry": geometry,
            "crs": crs,
        }

    def resolve(self, repo):
        data = self.resolved_data(repo)
        return ResolvedSpatialFilterSpec(data["crs"], data["geometry"])

    def write_config(self, repo):
        ref, oid, blob = self._resolve_ref_oid_blob(repo)
        if ref is None:
            # Found an object by object ID - objects are immutable including their OIDs, so storing the OID
            # is just storing a pointer to an immutable CRS and geometry. It makes more sense to resolve
            # this OID to its geometry and CRS, and store those in the config directly.
            self.resolve(repo).write_config(repo)

        else:
            # Found a reference. The reference is mutable, so we store it (and the object it points to).
            repo.config[self.REF_KEY] = ref
            repo.config[self.OID_KEY] = oid
            repo.del_config(self.GEOM_KEY)
            repo.del_config(self.CRS_KEY)

    def matches_working_copy(self, repo):
        return self.resolve(repo).matches_working_copy(repo)

    def partial_clone_filter_spec(self, specify_in_full=True):
        result = f"extension:spatial={self.ref_or_oid}"
        if specify_in_full:
            result = f"--filter={result}" if result else "--no-filter"
        return result

    @classmethod
    def split_file(cls, contents):
        parts = re.split(r"\r?\n\r?\n", contents, maxsplit=1)
        if len(parts) != 2:
            raise click.UsageError(
                "Spatial filter file must contain the CRS, then an empty line, then the geometry."
            )
        return [p.strip() for p in parts]


class MatchResult(Enum):
    """Stores a bit more detail about whether a feature matches or doesn't match the spatial filter."""

    # The given feature...
    # Does not exist: (This result is used in when matching feature deltas where we make a distinction as to whether
    # the old value or the new value matches the spatial filter (or both, or neither). When categorising these deltas,
    # we also keep the possibility that the old or new value doesn't exist at all (as happens for inserts and deletes)
    # separate from the non-matching and matching categories)
    NONEXISTENT = auto()
    # Isn't present locally, but is promised. From this we infer it doesn't match the repo's current spatial filter:
    PROMISED = auto()
    # Exists and is present and does NOT match the given spatial filter:
    NON_MATCHING = auto()
    # Exists and is present and DOES match the given spatial filter.
    # This includes the case where a feature matches the "match-all" spatial-filter, which matches everything,
    # and it includes the case where a feature has no geometry (and so matches all spatial filters).
    MATCHING = auto()

    def __bool__(self):
        return self is self.MATCHING


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

    @classmethod
    def load_repo_config(cls, repo):
        from kart.repo import KartConfigKeys

        geometry_spec = repo.get_config_str(KartConfigKeys.KART_SPATIALFILTER_GEOMETRY)
        crs_spec = repo.get_config_str(KartConfigKeys.KART_SPATIALFILTER_CRS)
        if geometry_spec:
            if not crs_spec:
                raise NotFound(
                    "Spatial filter CRS is missing from config",
                    exit_code=NO_SPATIAL_FILTER,
                )
            return {"geometry": geometry_spec, "crs": crs_spec}

        ref_spec = repo.get_config_str(KartConfigKeys.KART_SPATIALFILTER_REFERENCE)
        oid_spec = repo.get_config_str(KartConfigKeys.KART_SPATIALFILTER_OBJECTID)
        if ref_spec:
            if not oid_spec:
                raise NotFound(
                    "Spatial filter object ID is missing from config",
                    exit_code=NO_SPATIAL_FILTER,
                )

            if ref_spec not in repo.references:
                click.echo(
                    f"The current spatial filter has been deleted from {ref_spec} - to unapply this filter, run: "
                    f"kart checkout --spatial-filter=",
                    err=True,
                )
            elif str(repo.references[ref_spec].resolve().target) != oid_spec:
                # TODO -  Improve handling of changed spatial filter - maybe reapply it automatically if WC is clean.
                click.echo(
                    f"The spatial filter at {ref_spec} has changed since it was applied - to apply the new filter, "
                    f"run: kart checkout --spatial-filter={ref_spec}",
                    err=True,
                )

            contents = repo[oid_spec].data.decode("utf-8")
            crs_spec, geometry_spec = ReferenceSpatialFilterSpec.split_file(contents)
            return {
                "reference": ref_spec,
                "objectId": oid_spec,
                "geometry": geometry_spec,
                "crs": crs_spec,
            }

        return None

    @classmethod
    def from_repo_config(cls, repo):
        config = cls.load_repo_config(repo)
        if config:
            return SpatialFilter.from_spec(config["crs"], config["geometry"])

        return SpatialFilter.MATCH_ALL

    @classmethod
    @functools.lru_cache()
    def from_spec(cls, crs_spec, geometry_spec):
        return OriginalSpatialFilter(crs_spec, geometry_spec)

    def __init__(
        self, crs, filter_geometry_ogr, geom_column_name=None, match_all=False
    ):
        """
        Create a new spatial filter.
        filter_geometry_ogr - The shape of the spatial filter. An OGR Geometry object.
        crs - The CRS used to interpret the spatial filter. An OGR SpatialReference object.
        match_all - if True, this filter is the default match-everything filter.
        """
        self.match_all = match_all

        if match_all:
            self.crs = None
            self.filter_ogr = None
            self.filter_prep = None
            self.filter_env = None
            self.geom_column_name = None
        else:
            self.crs = crs
            self.filter_ogr = filter_geometry_ogr
            self.filter_prep = filter_geometry_ogr.CreatePreparedGeometry()
            self.filter_env = self.filter_ogr.GetEnvelope()
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
        if feature is None:
            return MatchResult.NONEXISTENT
        if self.match_all:
            return MatchResult.MATCHING

        feature_geometry = feature[self.geom_column_name]
        if feature_geometry is None:
            return MatchResult.MATCHING

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
                    return MatchResult.NON_MATCHING
            except Exception as e:
                raise
                L.warn(e)
                err = e

        # Slow check - geometry intersects geometry?
        try:
            if feature_ogr is None:
                feature_ogr = feature_geometry.to_ogr()
            intersects = self.filter_prep.Intersects(feature_ogr)
            return MatchResult.MATCHING if intersects else MatchResult.NON_MATCHING
        except Exception as e:
            L.warn(e)
            err = e

        # If we fail to apply the spatial filter - perhaps the geometry is corrupt? - we assume it matches.
        click.echo(f"Error applying spatial filter to geometry:\n{err}", err=True)
        return MatchResult.MATCHING

    def matches_delta_value(self, delta_key_value):
        # Returns a MatchResult describing whether the feature contained by the given Delta KeyValue matches this
        # spatial filter. The feature may need to be lazily loaded, or it may turn out not to be present in this repo,
        # in which case IS_PROMISED is returned.
        if delta_key_value is None:
            return MatchResult.NONEXISTENT
        try:
            value = delta_key_value.get_lazy_value()
            return self.matches(value)
        except KeyError as e:
            if self.feature_is_prefiltered(e):
                return MatchResult.PROMISED
            raise

    def feature_is_prefiltered(self, feature_error):
        # If no spatial filter is active, features should not be missing due to the spatial filter.
        return (not self.match_all) and object_is_promised(feature_error)


class OriginalSpatialFilter(SpatialFilter):
    """
    The OriginalSpatialFilter is a spatial filter that the user specified, with a particular geometry and
    a particular CRS. Unlike its parent class, the SpatialFilter, the OriginalSpatialFilter can be
    transformed to have a new CRS - but the result is a normal SpatialFilter, not an "Original".

    Normal SpatialFilters cannot be transformed - since transformation may be lossy, transforming a non-original
    SpatialFilter may lead to extra data loss which could be avoided by only ever transforming the original.
    That is why only OriginalSpatialFilter supports transformation.
    """

    def __init__(self, crs_spec, geometry_spec, match_all=False):
        if match_all:
            super().__init__(None, None, match_all=True)
            self.hexhash = None
        else:
            ctx = "spatial filter"
            geometry = geometry_from_string(geometry_spec, context=ctx)
            crs = make_crs(crs_spec, context=ctx)
            super().__init__(crs, geometry.to_ogr())
            self.hexhash = hexhash(crs_spec.strip(), geometry.to_wkb())

    @property
    def is_original(self):
        return True

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
            return SpatialFilter(crs, new_filter_ogr, new_geom_column_name)

        except RuntimeError as e:
            crs_desc = f"CRS for {ds_path!r}" if ds_path else f"CRS:\n {crs_spec!r}"
            raise CrsError(f"Can't reproject spatial filter into {crs_desc}:\n{e}")

    def matches_working_copy(self, repo):
        working_copy = repo.working_copy
        return (
            working_copy is None
            or working_copy.get_spatial_filter_hash() == self.hexhash
        )


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
