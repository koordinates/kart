import functools
import itertools
import logging
import re
import sys
from pathlib import Path
from typing import Generator

import click

from kart import diff_util
from kart.diff_format import DiffFormat
from kart.diff_structs import (
    FILES_KEY,
    WORKING_COPY_EDIT,
    BINARY_FILE,
    Delta,
    DatasetDiff,
)
from kart.exceptions import CrsError, InvalidOperation
from kart.key_filters import RepoKeyFilter
from kart import list_of_conflicts
from kart.promisor_utils import FetchPromisedBlobsProcess, object_is_promised
from kart.repo import KartRepoState
from kart.spatial_filter import SpatialFilter
from kart.serialise_util import b64encode_str
from kart.tile import ALL_TILE_DATASET_TYPES


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

    # This must be set to True when we need to keep track of which diff deltas were inside or outside the spatial
    # filter, and for which reason(s) - when it is False, we don't check all the possible reasons, we stop as soon
    # as we know the delta does or does not match the spatial filter.
    record_spatial_filter_stats = False

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
        commit_spec="",
        user_key_filters=(),
        output_path="-",
        *,
        json_style="pretty",
        delta_filter=None,
        target_crs=None,
        # used by json-lines diffs only
        diff_estimate_accuracy=None,
        # used by html diff only
        html_template=None,
        sort_keys=True,
    ):
        self.repo = repo
        self.commit_spec = commit_spec
        (
            self.base_rs,
            self.target_rs,
            self.include_wc_diff,
        ) = self.parse_diff_commit_spec(repo, commit_spec)

        self.user_key_filters = user_key_filters
        self.repo_key_filter = RepoKeyFilter.build_from_user_patterns(user_key_filters)
        self.html_template = html_template

        self.spatial_filter = repo.spatial_filter

        self.all_ds_paths = diff_util.get_all_ds_paths(
            self.base_rs, self.target_rs, self.repo_key_filter
        )
        self.workdir_diff_cache = self.repo.working_copy.workdir_diff_cache()

        self.spatial_filter_conflicts = None
        if (
            not self.spatial_filter.match_all
            and self.base_rs == self.target_rs
            and self.include_wc_diff
        ):
            # When generating a WC diff with a spatial filter active, we need to keep track of PK conflicts:
            self.record_spatial_filter_stats = True
            self.spatial_filter_conflicts = RepoKeyFilter()

        self.list_of_conflicts_warnings = []
        self.linked_dataset_changes = set()

        self.output_path = self._check_output_path(
            repo, self._normalize_output_path(output_path)
        )

        self.json_style = json_style
        self.target_crs = target_crs

        self.commit = None
        self.do_convert_to_dataset_format = None
        self.do_full_file_diffs = False
        self.sort_keys = sort_keys

    def include_target_commit_as_header(self):
        """
        For show / create-patch commands, which show the diff C^...C but also include a header
        with all the info for commit C.
        """
        self.commit = self.target_rs.commit

    def convert_to_dataset_format(self, do_convert_to_dataset_format):
        self.do_convert_to_dataset_format = do_convert_to_dataset_format

    def full_file_diffs(self, do_full_file_diffs=True):
        self.do_full_file_diffs = do_full_file_diffs

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
    def parse_diff_commit_spec(cls, repo, commit_spec):
        # Parse <commit> or <commit>...<commit>
        commit_spec = commit_spec or "HEAD"
        commit_parts = re.split(r"(\.{2,3})", commit_spec)

        if len(commit_parts) == 3:
            # Two commits specified - base and target. We diff base<>target.
            base_rs = repo.structure(commit_parts[0] or "HEAD", allow_unborn_head=False)
            target_rs = repo.structure(
                commit_parts[2] or "HEAD", allow_unborn_head=False
            )
            if commit_parts[1] == "..":
                # A   C    A...C is A<>C
                #  \ /     A..C  is B<>C
                #   B      (git log semantics)
                base_rs = cls._get_common_ancestor(repo, base_rs, target_rs)
            include_wc_diff = False
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = repo.structure(commit_parts[0])
            if repo.state == KartRepoState.MERGING:
                # During a merge, we transparently base the working copy off of the current merge-state
                # as stored in MERGED_TREE, rather than HEAD, so that's what we need to use as the target
                # of a working-copy diff (instead of HEAD).
                target_rs = repo.structure("MERGED_TREE")
            else:
                target_rs = repo.structure("HEAD", allow_unborn_head=False)

            repo.working_copy.assert_exists("Cannot generate working copy diff")
            repo.working_copy.assert_matches_tree(target_rs.tree)
            include_wc_diff = True
        return base_rs, target_rs, include_wc_diff

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
    def _all_dict_keys(cls, old_dict, new_dict):
        # Returns a sensible order for outputting all fields from two features which may have different fields.
        return itertools.chain(
            old_dict.keys(),
            (k for k in new_dict.keys() if k not in old_dict),
        )

    def write_header(self):
        """
        For writing any header that is not part of the diff itself eg version info, commit info.
        Not used for those JSON diffs where this info can't be written separately (it has to be part of the same JSON
        root object and output by the same call to json.dump)
        """
        pass

    def write_warnings_footer(self):
        """For writing any footer that is not part of the diff itself. Generally just writes warnings to stderr."""
        self.write_spatial_filter_conflicts_warning_footer()
        self.write_list_of_conflicts_warning_footer()
        self.write_linked_dataset_changes_warning_footer()

    def write_spatial_filter_conflicts_warning_footer(self):
        """
        Warns about spatial-filter conflicts - deltas that appear to be inserts but would actually overwrite
        items that already exist in a hard-to-see place: outside the spatial filter.
        These warnings are grouped since there could be many primary_keys we need to warn about, each with the same warning.
        """

        sf_conflicts = self.spatial_filter_conflicts
        if not sf_conflicts:
            return

        conflict_item_types = {}
        for ds_path, ds_key_filter in sf_conflicts.items():
            item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
            if ds_key_filter.get(item_type):
                conflict_item_types.setdefault(item_type, []).append(ds_path)

        for item_type, ds_paths in conflict_item_types.items():
            conflicting_property = (
                "primary key value" if item_type == "feature" else "name"
            )
            click.secho(
                f"Warning: Some {conflicting_property}s of newly-inserted {item_type}s in the working copy conflict with "
                f"other {item_type}s outside the spatial filter - if committed, they would overwrite those {item_type}s.",
                bold=True,
                err=True,
            )
            for ds_path in ds_paths:
                ds_key_filter in sf_conflicts[ds_path]
                conflict_list = ds_key_filter.get(item_type)
                if conflict_list:
                    if len(conflict_list) <= 100:
                        conflict_list = ", ".join(str(c) for c in conflict_list)
                    else:
                        conflict_list = (
                            ", ".join(str(c) for c in conflict_list[0:50])
                            + f", (... {len(conflict_list) - 50} more)"
                        )
                    click.echo(
                        f"  In dataset {ds_path} the conflicting {conflicting_property}s are: {conflict_list}",
                        err=True,
                    )
            click.echo(
                f"  To continue, change the {conflicting_property}s of those {item_type}s, or specify --allow-spatial-filter-conflicts.",
                err=True,
            )

    def write_list_of_conflicts_warning_footer(self):
        """
        Warns about ListOfConflicts - a delta where we're not sure what the new value should be because there are two different
        new values to choose from. For instance, the user has managed to provide two different CRSs in one dataset.
        """
        if not self.list_of_conflicts_warnings:
            return
        # Pretty vague since ListOfConflicts can describe a lot of different types of errors.
        click.secho(
            "Warning: Not all changes are committable as-is.", bold=True, err=True
        )
        for warning in self.list_of_conflicts_warnings:
            click.echo(f"  {warning}", err=True)

    def write_linked_dataset_changes_warning_footer(self):
        if not self.linked_dataset_changes:
            return
        click.secho(
            "Warning: changes to linked datasets cannot be committed.",
            bold=True,
            err=True,
        )
        click.echo(
            "To update a linked dataset, re-import from the source with both --link and --replace-existing.\n"
            "To discard these changes, use `kart reset --discard-changes`.",
            err=True,
        )
        click.echo("Linked datasets with uncommitted changes:", err=True)
        for ds_path in sorted(self.linked_dataset_changes):
            click.echo(f"  {ds_path}", err=True)

    def write_diff(self, diff_format=DiffFormat.FULL):
        """Default implementation for writing a diff. Subclasses can override."""
        # Entered when -o is text
        self.write_header()

        # If the diff format is NONE, skip getting the diff
        if diff_format == DiffFormat.NONE:
            self.write_warnings_footer()
            return
        # If the diff format is NO_DATA_CHANGES, check if there is a diff and print True or False
        elif diff_format == DiffFormat.NO_DATA_CHANGES:
            self.has_changes = False
            for ds_path in self.all_ds_paths:
                self.has_changes |= self.write_ds_diff_for_path(ds_path, diff_format)

            self.write_warnings_footer()
            return

        # Else, print the entire diff
        self.has_changes = False
        for ds_path in self.all_ds_paths:
            self.has_changes |= self.write_ds_diff_for_path(
                ds_path, diff_format=DiffFormat.FULL
            )
        self.has_changes |= self.write_file_diff(self.get_file_diff())
        self.write_warnings_footer()

    def write_ds_diff_for_path(self, ds_path, diff_format=DiffFormat.FULL):
        """Default implementation for writing the diff for a particular dataset. Subclasses can override."""
        ds_diff = self.get_dataset_diff(ds_path, diff_format=diff_format)
        has_changes = bool(ds_diff)
        list_of_conflicts.extract_error_messages_from_dataset_diff(
            ds_path, ds_diff, self.list_of_conflicts_warnings
        )
        if self.include_wc_diff:
            self._check_for_linked_dataset_changes(ds_path, ds_diff)
        self.write_ds_diff(ds_path, ds_diff, diff_format=diff_format)
        return has_changes

    def write_ds_diff(self):
        """For outputting ds_diff, the diff of a particular dataset."""
        raise NotImplementedError()

    def write_file_diff(self, file_diff):
        """For outputting file_diff - all the files that have changed, without reference to any dataset."""
        raise NotImplementedError()

    BASE64_PREFIX = "base64:"
    # Not having a text-prefix looks nicer but is slightly ambiguous in certain circumstances.
    # Subclasses can override if ambiguity would be bad.
    TEXT_PREFIX = ""

    def _full_file_delta(self, delta, skip_binary_files=False):
        def get_blob(half_delta):
            return self.repo[half_delta.value] if half_delta else None

        def is_binary(blob):
            return blob.is_binary if blob else False

        old_blob = get_blob(delta.old)
        new_blob = get_blob(delta.new)
        delta_is_binary = is_binary(old_blob) or is_binary(new_blob)

        if delta_is_binary:
            delta.flags |= BINARY_FILE
            if skip_binary_files:
                return delta

            blob_to_text = lambda blob: self.BASE64_PREFIX + b64encode_str(
                memoryview(blob)
            )
        else:
            blob_to_text = lambda blob: self.TEXT_PREFIX + str(
                memoryview(blob), "utf-8"
            )

        old_half_delta = (delta.old_key, blob_to_text(old_blob)) if delta.old else None
        new_half_delta = (delta.new_key, blob_to_text(new_blob)) if delta.new else None

        result = Delta(old_half_delta, new_half_delta)
        result.flags = delta.flags
        return result

    def get_repo_diff(self, include_files=True, diff_format=DiffFormat.FULL):
        """
        Generates a RepoDiff containing an entry for every dataset in the repo (that matches self.repo_key_filter).
        """
        repo_diff = diff_util.get_repo_diff(
            self.base_rs,
            self.target_rs,
            include_wc_diff=self.include_wc_diff,
            workdir_diff_cache=self.workdir_diff_cache,
            repo_key_filter=self.repo_key_filter,
            convert_to_dataset_format=self.do_convert_to_dataset_format,
            include_files=include_files,
            diff_format=diff_format,
        )
        list_of_conflicts.extract_error_messages_from_repo_diff(
            repo_diff, self.list_of_conflicts_warnings
        )
        if self.include_wc_diff:
            for ds_path, ds_diff in repo_diff.items():
                self._check_for_linked_dataset_changes(ds_path, ds_diff)
        return repo_diff

    def get_dataset_diff(self, ds_path, diff_format=DiffFormat.FULL):
        """
        Returns the DatasetDiff object for the dataset at path dataset_path.

        Note that this diff is not yet spatial filtered. It is a dict, not a generator,
        and may contain feature values that have not yet been loaded. Spatial filtering
        cannot be applied to it while it remains a dict, since this would involve loading
        all the features up front, which breaks diff streaming.
        To apply the spatial filter to it, call self.filtered_dataset_deltas(ds_path, ds_diff)
        which will return a generator that filters features as it loads and outputs them,
        which can be used to output streaming diffs.
        """
        return diff_util.get_dataset_diff(
            ds_path,
            self.base_rs.datasets(),
            self.target_rs.datasets(),
            include_wc_diff=self.include_wc_diff,
            workdir_diff_cache=self.workdir_diff_cache,
            ds_filter=self.repo_key_filter[ds_path],
            convert_to_dataset_format=self.do_convert_to_dataset_format,
            diff_format=diff_format,
        )

    def get_file_diff(self):
        """Returns the DatasetDiff object for the deltas that do not belong to any dataset."""
        return diff_util.get_file_diff(
            self.base_rs,
            self.target_rs,
            include_wc_diff=False,
            repo_key_filter=self.repo_key_filter,
        )

    def iter_deltadiff_items(self, deltas):
        if self.sort_keys:
            return deltas.resolve().sorted_items()
        return deltas.items()

    def filtered_dataset_deltas(self, ds_path, ds_diff):
        """
        Yields the key, delta for only those deltas from the given dataset diff that match
        self.spatial_filter. Note that deltas are always considered to match the spatial-filter
        if they are marked as working-copy edits, since working-copy edits are always relevant to the user
        even if they are outside the spatial filter.
        """
        item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
        if not item_type or item_type not in ds_diff:
            return

        unfiltered_deltas = self.iter_deltadiff_items(ds_diff[item_type])

        if self.spatial_filter.match_all:
            yield from unfiltered_deltas
            return

        old_spatial_filter, new_spatial_filter = self.get_spatial_filters(
            ds_path, ds_diff
        )
        if old_spatial_filter.match_all and new_spatial_filter.match_all:
            # This can happen if neither the old nor the new version of the dataset have
            # any geometry - all of their features are guaranteed to match the spatial filter,
            # so no point doing any filtering here.
            yield from unfiltered_deltas
            return

        do_yield = False

        # Avoid testing geometries against the spatial filter unless
        # a) we still haven't decided if this delta should be output or not or
        # b) record_spatial_filter_stats is set, so we need to check everything.
        def lazy_eval(callable):
            if do_yield and not self.record_spatial_filter_stats:
                return None
            return callable()

        delta_fetcher = self._get_delta_fetcher(ds_path)

        for key, delta in unfiltered_deltas:
            do_yield = bool(delta.flags & WORKING_COPY_EDIT)
            omr = lazy_eval(lambda: old_spatial_filter.matches_delta_value(delta.old))
            do_yield |= bool(omr)
            nmr = lazy_eval(lambda: new_spatial_filter.matches_delta_value(delta.new))
            do_yield |= bool(nmr)
            if omr is not None and nmr is not None:
                self.record_spatial_filter_stat(
                    ds_path, item_type, key, delta, omr, nmr
                )
            if do_yield:
                if delta_fetcher.ensure_delta_is_ready_or_start_fetch(key, delta):
                    yield key, delta

        yield from delta_fetcher.finish_fetching_deltas()

    def record_spatial_filter_stats_for_dataset(self, ds_path, ds_diff):
        """
        Goes through the given dataset-diff and checks which features / tiles match the spatial filter by calling
        record_spatial_filter_stat on each one.
        This is only necessary if the filtered deltas are not being output - the stats are recorded automatically
        as the deltas are filtered and output. In fact, this function works just by iterating over them without
        outputting them, which causes the stats to be recorded in the same way.
        """
        for _ in self.filtered_dataset_deltas(ds_path, ds_diff):
            pass

    def record_spatial_filter_stat(
        self, ds_path, item_type, key, delta, old_match_result, new_match_result
    ):
        """
        Records which / how many features were inside / outside the spatial filter for which reasons.
        These records are used by write_warnings_footer to show warnings to the user.
        """
        if self.spatial_filter_conflicts is not None:
            if not old_match_result and delta.old is not None and delta.new is not None:
                self.spatial_filter_conflicts.recursive_set(
                    [ds_path, item_type, key], True
                )

    def _get_delta_fetcher(self, ds_path):
        dataset = self._get_old_or_new_dataset(ds_path)

        if dataset.DATASET_TYPE == "table":
            # Table datasets can have missing ODB blobs that we need to fetch during a diff.
            return FeatureDeltaFetcher(self, ds_path)
        else:
            # Point-cloud datasets should not have missing data - all ODB blobs should be present.
            # The tile LFS blobs themselves may be missing, but these are not needed to generate a diff.
            return NullDeltaFetcher(ds_path, dataset.DATASET_TYPE)

    def _get_old_and_new_schema(self, ds_path, ds_diff):
        old_schema = new_schema = None
        schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
        if schema_delta and schema_delta.old_value:
            old_schema = schema_delta.old_value
        if schema_delta and schema_delta.new_value:
            new_schema = schema_delta.new_value
        if old_schema or new_schema:
            return old_schema, new_schema

        # No diff - old and new schemas are the same.
        dataset = self._get_old_or_new_dataset(ds_path)
        schema = dataset.schema
        return schema, schema

    def get_old_and_new_crs(self, ds_path, ds_diff, context=None):
        dataset = self._get_old_or_new_dataset(ds_path)
        if dataset.DATASET_TYPE == "table":
            return self._get_old_and_new_table_crs(ds_path, ds_diff, context=context)
        elif dataset.DATASET_TYPE in ALL_TILE_DATASET_TYPES:
            return self._get_old_and_new_tile_crs(ds_path, ds_diff, context=context)
        raise RuntimeError(
            f"Can't load old and new CRS for dataset of type {dataset.DATASET_TYPE}"
        )

    def _get_old_and_new_table_crs(self, ds_path, ds_diff, context=None):
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
        dataset = self._get_old_or_new_dataset(ds_path)
        crs_defs = list(dataset.crs_definitions().values())
        if not crs_defs:
            return None, None
        if len(crs_defs) > 1:
            self._raise_multi_crs_error(ds_path, context=context)
        crs = make_crs(crs_defs[0], context=ds_path)
        return crs, crs

    def _get_old_and_new_tile_crs(self, ds_path, ds_diff, context=None):
        from kart.crs_util import make_crs

        # If the CRS is changing during the diff, we extract the two CRS from the diff.
        crs_delta = ds_diff.recursive_get(["meta", "crs.wkt"])
        if crs_delta:
            old_crs_def = crs_delta.old_value
            old_crs = make_crs(old_crs_def, context=ds_path) if old_crs_def else None
            new_crs_def = crs_delta.new_value
            new_crs = make_crs(new_crs_def, context=ds_path) if new_crs_def else None
            return old_crs, new_crs

        # No diff - old and new CRS are the same.
        dataset = self._get_old_or_new_dataset(ds_path)
        crs_def = dataset.get_meta_item("crs.wkt")
        crs = make_crs(crs_def, context=ds_path) if crs_def else None
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

        dataset = self._get_old_or_new_dataset(ds_path)
        if dataset.DATASET_TYPE != "table":
            # So far, table datasets are the only ones which have deltas transformed to the target CRS.
            # TODO - support transformed output for tile-based datasets too.
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

        old_crs, new_crs = self.get_old_and_new_crs(
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
        dataset = self._get_old_or_new_dataset(ds_path)
        if dataset.DATASET_TYPE == "table":
            return self._get_table_spatial_filters(ds_path, ds_diff)
        elif dataset.DATASET_TYPE in ALL_TILE_DATASET_TYPES:
            return self._get_tile_spatial_filters(ds_path, ds_diff)
        raise RuntimeError(
            f"Spatial filtering is not supported for dataset of type {dataset.DATASET_TYPE}"
        )

    def _get_table_spatial_filters(self, ds_path, ds_diff):
        old_schema, new_schema = self._get_old_and_new_schema(ds_path, ds_diff)
        old_crs, new_crs = self._get_old_and_new_table_crs(
            ds_path, ds_diff, context="spatial filtering"
        )
        sf = self.spatial_filter
        old_spatial_filter = (
            sf.transform_for_table_schema_and_crs(old_schema, old_crs, ds_path)
            if old_schema
            else SpatialFilter.MATCH_ALL
        )
        new_spatial_filter = (
            sf.transform_for_table_schema_and_crs(new_schema, new_crs, ds_path)
            if new_schema
            else SpatialFilter.MATCH_ALL
        )
        return old_spatial_filter, new_spatial_filter

    def _get_tile_spatial_filters(self, ds_path, ds_diff):
        old_crs, new_crs = self._get_old_and_new_tile_crs(
            ds_path, ds_diff, context="spatial filtering"
        )
        sf = self.spatial_filter
        old_spatial_filter = (
            sf.transform_for_tile_crs(old_crs, ds_path)
            if old_crs
            else SpatialFilter.MATCH_ALL
        )
        new_spatial_filter = (
            sf.transform_for_tile_crs(new_crs, ds_path)
            if new_crs
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

    @functools.lru_cache()
    def _get_old_or_new_dataset(self, ds_path):
        """
        Returns the dataset at ds_path. Could be the old version or the new version of the dataset,
        so, useful for accessing things that won't change (its path, its type), or for accessing
        things that haven't changed (ie, check the diff first to make sure it hasn't changed).
        """
        if ds_path == FILES_KEY:
            return None
        dataset = self.base_rs.datasets().get(ds_path)
        if not dataset:
            dataset = self.target_rs.datasets().get(ds_path)
        return dataset

    def _check_for_linked_dataset_changes(self, ds_path, ds_diff):
        dataset = self._get_old_or_new_dataset(ds_path)
        if dataset and dataset.get_meta_item("linked-storage.json") and ds_diff:
            self.linked_dataset_changes.add(ds_path)

    def filtered_dataset_deltas_as_geojson(
        self, ds_path: str, ds_diff: DatasetDiff
    ) -> Generator[dict, None, None]:
        from kart.tabular.feature_output import feature_as_geojson

        if "feature" not in ds_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)

        for key, delta in self.filtered_dataset_deltas(ds_path, ds_diff):
            if delta.old:
                change_type = "U-" if delta.new else "D"
                yield feature_as_geojson(
                    delta.old_value,
                    delta.old_key,
                    ds_path,
                    change_type,
                    old_transform,
                )
            if delta.new:
                change_type = "U+" if delta.old else "I"
                yield feature_as_geojson(
                    delta.new_value,
                    delta.new_key,
                    ds_path,
                    change_type,
                    new_transform,
                )


class BaseDeltaFetcher:
    def _is_delta_value_ready(self, delta_key_value):
        if delta_key_value is None:
            return True
        try:
            delta_key_value.get_lazy_value()
            return True
        except KeyError as e:
            if object_is_promised(e):
                return False
            raise


class FeatureDeltaFetcher(BaseDeltaFetcher):
    """
    Given a diff Delta, either reports that it is available immediately, or kicks off a fetch so that it will be
    available soon, and adds it to the list of buffered deltas. This lets the diff writer above first output the deltas
    that are available immediately, and then block until the other deltas have been fetched, and finally output those.
    """

    def __init__(self, diff_writer, ds_path):
        self.diff_writer = diff_writer
        self.ds_path = ds_path
        self.buffered_deltas = []

    def ensure_delta_is_ready_or_start_fetch(self, key, delta):
        """
        If the delta is locally available, simply returns True.
        Otherwise, kicks off a fetch operation so that the Delta will be available soon, and adds the Delta
        to a buffer of deltas to be retried later. After doing any other useful work while waiting for the
        fetch to , all the deltas that were fetched can be generated by calling finish_fetching_deltas.
        """

        old_value_ready = self._is_delta_value_ready(delta.old)
        new_value_ready = self._is_delta_value_ready(delta.new)
        if old_value_ready and new_value_ready:
            return True

        self.buffered_deltas.append((key, delta))
        if not old_value_ready:
            self._start_fetch(delta.old)
        if not new_value_ready:
            self._start_fetch(delta.new)
        return False

    @property
    def fetch_process(self):
        if not hasattr(self, "_fetch_process"):
            self._fetch_process = FetchPromisedBlobsProcess(self.diff_writer.repo)
        return self._fetch_process

    def _start_fetch(self, delta_key_value):
        blob = delta_key_value.value.args[0]
        self.fetch_process.fetch(blob.id.hex)

    def finish_fetching_deltas(self):
        """Blocks until all the deltas that were requested finish fetching, then yields them all."""

        if not hasattr(self, "_fetch_process"):
            # We didn't start fetching any features - nothing to do here.
            return

        # Notify the user about the fetch at this point since this is the point at which the diff
        # output will stop until the fetch completes.
        click.echo(
            f"Fetching missing but required features in {self.ds_path}", err=True
        )

        self._fetch_process.finish()
        yield from self.buffered_deltas


class NullDeltaFetcher(BaseDeltaFetcher):
    """
    Given a diff Delta, checks to make sure that it is immediately available. If it is not, outputs an error message.
    """

    def __init__(self, ds_path, ds_type):
        self.ds_path = ds_path
        self.ds_type = ds_type

    def ensure_delta_is_ready_or_start_fetch(self, key, delta):
        """
        If the delta is locally available, simply returns True.
        If the delta is not locally available, raises an error.
        """

        old_value_ready = self._is_delta_value_ready(delta.old)
        new_value_ready = self._is_delta_value_ready(delta.new)
        if old_value_ready and new_value_ready:
            return True

        raise RuntimeError(
            f"Dataset {self.ds_path} has missing+promised blobs - this is not expected for a {self.ds_type} dataset"
        )

    def finish_fetching_deltas(self):
        # Nothing to do here.
        yield from ()
