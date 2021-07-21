import itertools
import logging
from pathlib import Path
import re
import sys

import click

from .diff_structs import RepoDiff, DatasetDiff, WORKING_COPY_EDIT
from .exceptions import (
    CrsError,
    InvalidOperation,
    NotFound,
    NO_WORKING_COPY,
)
from .key_filters import RepoKeyFilter
from .spatial_filters import SpatialFilter


L = logging.getLogger("kart.diff_writer")


class BaseDiffWriter:
    """
    A base class for writing diff output. Can handle any/all of the following, depending on the needs
    of the implementation class:
    - parsing commit spec eg abcd^^...abcd
    - setting up filters, finding repo datasets that are included by filters
    - creating a diff object for the entire repo, or dataset by dataset (preferred when possible)
    - finding geometry transforms needed for each dataset
    - exiting with the right code
    """

    @classmethod
    def get_diff_writer_class(cls, output_format):
        if output_format == "quiet":
            from .quiet_diff_writer import QuietDiffWriter

            return QuietDiffWriter
        elif output_format == "text":
            from .text_diff_writer import TextDiffWriter

            return TextDiffWriter
        elif output_format == "json":
            from .json_diff_writers import JsonDiffWriter

            return JsonDiffWriter
        elif output_format == "json-lines":
            from .json_diff_writers import JsonLinesDiffWriter

            return JsonLinesDiffWriter
        elif output_format == "geojson":
            from .json_diff_writers import GeojsonDiffWriter

            return GeojsonDiffWriter
        elif output_format == "html":
            from .html_diff_writer import HtmlDiffWriter

            return HtmlDiffWriter

        raise click.BadParameter(
            f"Unrecognized output format: {output_format}", param_hint="output_format"
        )

    def __init__(
        self,
        repo,
        commit_spec,
        user_key_filters,
        output_path="-",
        *,
        json_style="pretty",
        target_crs=None,
    ):
        self.repo = repo
        self.commit_spec = commit_spec
        self.base_rs, self.target_rs, self.working_copy = self._parse_diff_commit_spec(
            repo, commit_spec
        )

        self.user_key_filters = user_key_filters
        self.repo_key_filter = RepoKeyFilter.build_from_user_patterns(user_key_filters)

        self.spatial_filter = repo.spatial_filter

        base_ds_paths = {ds.path for ds in self.base_rs.datasets}
        target_ds_paths = {ds.path for ds in self.target_rs.datasets}
        all_ds_paths = base_ds_paths | target_ds_paths

        if not self.repo_key_filter.match_all:
            all_ds_paths = all_ds_paths & self.repo_key_filter.keys()

        self.all_ds_paths = sorted(list(all_ds_paths))

        self.output_path = self._check_output_path(
            repo, self._normalize_output_path(output_path)
        )

        self.json_style = json_style
        self.target_crs = target_crs

        self.commit = None

    def include_target_commit_as_header(self):
        """
        For show / create-patch commands, which show the diff C^...C but also include a header
        with all the info for commit C.
        """
        self.commit = self.target_rs.commit

    @classmethod
    def _normalize_output_path(cls, output_path):
        if not output_path or output_path == "-":
            return output_path
        if isinstance(output_path, str):
            output_path = Path(output_path)
        if isinstance(output_path, Path):
            output_path = output_path.expanduser()
        return output_path

    @classmethod
    def _check_output_path(cls, repo, output_path):
        """Make sure the given output_path is valid for this implementation (ie, are directories supported)."""
        return output_path

    @classmethod
    def _parse_diff_commit_spec(cls, repo, commit_spec):
        # Parse <commit> or <commit>...<commit>
        commit_spec = commit_spec or "HEAD"
        commit_parts = re.split(r"(\.{2,3})", commit_spec)

        if len(commit_parts) == 3:
            # Two commits specified - base and target. We diff base<>target.
            base_rs = repo.structure(commit_parts[0] or "HEAD")
            target_rs = repo.structure(commit_parts[2] or "HEAD")
            if commit_parts[1] == "..":
                # A   C    A...C is A<>C
                #  \ /     A..C  is B<>C
                #   B      (git log semantics)
                base_rs = cls._get_common_ancestor(repo, base_rs, target_rs)
            working_copy = None
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = repo.structure(commit_parts[0])
            target_rs = repo.structure("HEAD")
            working_copy = repo.working_copy
            if not working_copy:
                raise NotFound("No working copy", exit_code=NO_WORKING_COPY)
            working_copy.assert_db_tree_match(target_rs.tree)
        return base_rs, target_rs, working_copy

    @classmethod
    def _get_common_ancestor(cls, repo, rs1, rs2):
        for rs in rs1, rs2:
            if not rs.commit:
                raise click.UsageError(
                    f"The .. operator works on commits, not trees - {rs.id} is a tree. (Perhaps try the ... operator)"
                )
        ancestor_id = repo.merge_base(rs1.id, rs2.id)
        if not ancestor_id:
            raise InvalidOperation(
                "The .. operator tries to find the common ancestor, but no common ancestor was found. Perhaps try the ... operator."
            )
        return repo.structure(ancestor_id)

    @classmethod
    def _all_feature_keys(cls, old_feature, new_feature):
        # Returns a sensible order for outputting all fields from two features which may have different fields.
        return itertools.chain(
            old_feature.keys(),
            (k for k in new_feature.keys() if k not in old_feature),
        )

    def write_header(self):
        """For writing any header that is not part of the diff itself eg version info, commit info."""
        pass

    def write_diff(self):
        """Default implementation for writing a diff. Subclasses can override."""
        self.write_header()
        self.has_changes = False
        for ds_path in self.all_ds_paths:
            self.has_changes |= self.write_ds_diff_for_path(ds_path)

    def write_ds_diff_for_path(self, ds_path):
        """Default implementation for writing the diff for a particular dataset. Subclasses can override."""
        ds_diff = self.get_dataset_diff(ds_path)
        has_changes = bool(ds_diff)
        self.write_ds_diff(ds_path, ds_diff)
        return has_changes

    def write_ds_diff(self, ds_path, ds_diff):
        """For outputting ds_diff, the diff of a particular dataset."""
        raise NotImplementedError()

    def get_repo_diff(self, prune=True):
        """Returns the RepoDiff object for the entire repo."""
        diff = RepoDiff()
        for ds_path in self.all_ds_paths:
            diff[ds_path] = self.get_dataset_diff(ds_path, prune=False)
        if prune:
            diff.prune()
        return diff

    def get_dataset_diff(self, dataset_path, prune=True):
        """
        Returns the DatasetDiff object for the dataset at path dataset_path.

        Note that this diff is not yet spatial filtered. It is a dict, not a generator,
        and may contain feature values that have not yet been loaded. Spatial filtering
        cannot be applied to it while it remains a dict, since this would involve loading
        all the features up front, which breaks diff streaming.
        To apply the spatial filter to it, call self.filtered_ds_feature_deltas(ds_path, ds_diff)
        which will return a generator that filters features as it loads and outputs them,
        which can be used to output streaming diffs.
        """

        diff = DatasetDiff()
        ds_filter = self.repo_key_filter[dataset_path]

        if self.base_rs != self.target_rs:
            # diff += base_rs<>target_rs
            base_ds = self.base_rs.datasets.get(dataset_path)
            target_ds = self.target_rs.datasets.get(dataset_path)

            params = {}
            if not base_ds:
                base_ds, target_ds = target_ds, base_ds
                params["reverse"] = True

            diff_cc = base_ds.diff(target_ds, ds_filter=ds_filter, **params)
            L.debug("commit<>commit diff (%s): %s", dataset_path, repr(diff_cc))
            diff += diff_cc

        if self.working_copy:
            # diff += target_rs<>working_copy
            target_ds = self.target_rs.datasets.get(dataset_path)
            diff_wc = self.working_copy.diff_db_to_tree(target_ds, ds_filter=ds_filter)
            L.debug(
                "commit<>working_copy diff (%s): %s",
                dataset_path,
                repr(diff_wc),
            )
            diff += diff_wc

        if prune:
            diff.prune()
        return diff

    def _unfiltered_ds_feature_deltas(self, ds_path, ds_diff):
        if "feature" not in ds_diff:
            return

        yield from ds_diff["feature"].sorted_items()

    def filtered_ds_feature_deltas(self, ds_path, ds_diff):
        """
        Yields the key, delta for only those feature-deltas from the given dataset diff that match
        self.spatial_filter. Note that feature-deltas are always considered to match the spatial-filter
        if they are marked as working-copy edits, since working-copy edits are always relevant to the user
        even if they are outside the spatial filter.
        """
        # NOTE: This function has to load every feature if it is to do any filtering at all.
        # This stops lazy-loading of features for streaming diffs from providing any benefit.
        # TODO: Write better streaming alternatives for the more streamable output types, ie text and json-lines.
        if "feature" not in ds_diff:
            return

        if self.spatial_filter.match_all:
            yield from self._unfiltered_ds_feature_deltas(ds_path, ds_diff)
            return

        old_spatial_filter, new_spatial_filter = self.get_spatial_filters(
            ds_path, ds_diff
        )
        if old_spatial_filter.match_all and new_spatial_filter.match_all:
            # This can happen if neither the old nor the new version of the dataset have
            # any geometry - all of their features are guaranteed to match the spatial filter,
            # so no point doing any filtering here.
            yield from self._unfiltered_ds_feature_deltas(ds_path, ds_diff)
            return

        nonmatching_feature_count = 0
        for key, delta in self._unfiltered_ds_feature_deltas(ds_path, ds_diff):
            matches = bool(delta.flags & WORKING_COPY_EDIT)
            matches = matches or bool(
                delta.old_value and old_spatial_filter.matches(delta.old_value)
            )
            matches = matches or bool(
                delta.new_value and new_spatial_filter.matches(delta.new_value)
            )
            if matches:
                yield key, delta
            else:
                nonmatching_feature_count += 1

        self.report_nonmatching_features(ds_path, nonmatching_feature_count)

    def report_nonmatching_features(self, ds_path, nonmatching_feature_count):
        # Subclasses can override to warn about features that didn't match the spatial filter.
        pass

    def _get_old_and_new_schema(self, ds_path, ds_diff):
        from kart.schema import Schema

        old_schema = new_schema = None
        schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
        if schema_delta and schema_delta.old_value:
            old_schema = Schema.from_column_dicts(schema_delta.old_value)
        if schema_delta and schema_delta.new_value:
            new_schema = Schema.from_column_dicts(schema_delta.new_value)
        if old_schema or new_schema:
            return old_schema, new_schema

        # No diff - old and new schemas are the same.
        ds = self.base_rs.datasets.get(ds_path) or self.target_rs.datasets.get(ds_path)
        schema = ds.schema
        return schema, schema

    def _get_old_and_new_crs(self, ds_path, ds_diff, context=None):
        from kart.crs_util import make_crs

        # If the CRS is changing during the diff, we extract the two CRS from the diff.
        if "meta" in ds_diff:
            meta_diff = ds_diff["meta"]
            old_crs_defs = [
                v.old_value
                for k, v in meta_diff.items()
                if k.startswith("crs/") and v.old is not None
            ]
            new_crs_defs = [
                v.new_value
                for k, v in meta_diff.items()
                if k.startswith("crs/") and v.new is not None
            ]
            if len(old_crs_defs) > 1 or len(new_crs_defs) > 1:
                self._raise_multi_crs_error(ds_path, context=context)
            old_crs, new_crs = None, None
            if old_crs_defs:
                old_crs = make_crs(old_crs_defs[0], context=ds_path)
            if new_crs_defs:
                new_crs = make_crs(new_crs_defs[0], context=ds_path)
            if old_crs_defs or new_crs_defs:
                return old_crs, new_crs

        # No diff - old and new CRS are the same.
        ds = self.base_rs.datasets.get(ds_path) or self.target_rs.datasets.get(ds_path)
        crs_defs = list(ds.crs_definitions().values())
        if not crs_defs:
            return None, None
        if len(crs_defs) > 1:
            self._raise_multi_crs_error(ds_path, context=context)
        crs = make_crs(crs_defs[0], context=ds_path)
        return crs, crs

    def _raise_multi_crs_error(ds_path, context=None):
        message = (
            f"Sorry, multiple CRS definitions at {ds_path!r} are not yet supported"
        )
        if context:
            message += f" for {message}"
        raise CrsError(message)

    def get_geometry_transforms(self, ds_path, ds_diff, context=None):
        """
        Returns old_transform, new_transform for the dataset at a particular path -
        where old_transform is the transform that should be applied to old, pre-diff values,
        and new_transform is the transform that should be applied to new, post-diff values,
        in order that all geometry values output are now in self.target_crs
        """
        if self.target_crs is None:
            return None, None

        def _get_transform(source_crs):
            if source_crs is None:
                return None

            from osgeo import osr

            try:
                return osr.CoordinateTransformation(source_crs, self.target_crs)
            except RuntimeError as e:
                raise CrsError(
                    f"Can't reproject dataset {ds_path!r} into target CRS: {e}"
                )

        old_crs, new_crs = self._get_old_and_new_crs(
            ds_path, ds_diff, context="reprojection"
        )
        return (_get_transform(old_crs), _get_transform(new_crs))

    def get_spatial_filters(self, ds_path, ds_diff):
        """
        Returns old_spatial_filter, new_spatial filter for the datast at a particular path -
        where old_spatial_filter is the filter that should be applied to old, pre-diff values,
        and new_spatial_filter is the transform that should be applied to new, post-diff values,
        so that the spatial filter's CRS and geometry column name match the dataset.
        """
        old_schema, new_schema = self._get_old_and_new_schema(ds_path, ds_diff)
        old_crs, new_crs = self._get_old_and_new_crs(
            ds_path, ds_diff, context="spatial filtering"
        )
        sf = self.spatial_filter
        old_spatial_filter = (
            sf.transform_for_schema_and_crs(old_schema, old_crs, ds_path)
            if old_schema
            else SpatialFilter.MATCH_ALL
        )
        new_spatial_filter = (
            sf.transform_for_schema_and_crs(new_schema, new_crs, ds_path)
            if new_schema
            else SpatialFilter.MATCH_ALL
        )
        return old_spatial_filter, new_spatial_filter

    def exit_with_code(self):
        """Exit with code 1 if the diff already written had changes, otherwise exit with code 0."""
        if not hasattr(self, "has_changes"):
            raise RuntimeError(
                "write_diff must be called first to populate has_changes"
            )
        if self.has_changes:
            sys.exit(1)
        else:
            sys.exit(0)
