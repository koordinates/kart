import itertools
import logging
import re
import sys
from pathlib import Path

import click

from . import diff_util
from .diff_structs import WORKING_COPY_EDIT
from .exceptions import CrsError, InvalidOperation
from .key_filters import RepoKeyFilter
from .promisor_utils import FetchPromisedBlobsProcess, object_is_promised
from .spatial_filter import SpatialFilter

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
        target_crs=None,
        # used by json-lines diffs only
        diff_estimate_accuracy=None,
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

        self.spatial_filter = repo.spatial_filter

        self.all_ds_paths = diff_util.get_all_ds_paths(
            self.base_rs, self.target_rs, self.repo_key_filter
        )
        self.wc_diff_context = diff_util.WCDiffContext(repo, self.all_ds_paths)

        self.spatial_filter_pk_conflicts = None
        if (
            not self.spatial_filter.match_all
            and self.base_rs == self.target_rs
            and self.include_wc_diff
        ):
            # When generating a WC diff with a spatial filter active, we need to keep track of PK conflicts:
            self.record_spatial_filter_stats = True
            self.spatial_filter_pk_conflicts = RepoKeyFilter()

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
    def parse_diff_commit_spec(cls, repo, commit_spec):
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
            include_wc_diff = False
        else:
            # When one commit is specified, it is base, and we diff base<>working_copy.
            # When no commits are specified, base is HEAD, and we do the same.
            # We diff base<>working_copy by diffing base<>target + target<>working_copy,
            # and target is set to HEAD.
            base_rs = repo.structure(commit_parts[0])
            target_rs = repo.structure("HEAD")
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
        pk_conflicts = self.spatial_filter_pk_conflicts
        if pk_conflicts:
            click.secho(
                "Warning: Some primary keys of newly-inserted features in the working copy conflict with other features "
                "outside the spatial filter - if committed, they would overwrite those features.",
                bold=True,
                err=True,
            )
            for ds_path, ds_key_filter in pk_conflicts.items():
                # So far we only support pk conflicts in vector features:
                pk_list = ds_key_filter.get("feature")
                if pk_list:
                    if len(pk_list) <= 100:
                        pk_list = ", ".join(str(pk) for pk in pk_list)
                    else:
                        pk_list = (
                            ", ".join(str(pk) for pk in pk_list[0:50])
                            + f", (... {len(pk_list) - 50} more)"
                        )
                    click.echo(
                        f"  In dataset {ds_path} the conflicting primary key values are: {pk_list}",
                        err=True,
                    )
            click.echo(
                "  To continue, change the primary key values of those features.",
                err=True,
            )

    def write_diff(self):
        """Default implementation for writing a diff. Subclasses can override."""
        self.write_header()
        self.has_changes = False
        for ds_path in self.all_ds_paths:
            self.has_changes |= self.write_ds_diff_for_path(ds_path)
        self.write_warnings_footer()

    def write_ds_diff_for_path(self, ds_path):
        """Default implementation for writing the diff for a particular dataset. Subclasses can override."""
        ds_diff = self.get_dataset_diff(ds_path)
        has_changes = bool(ds_diff)
        self.write_ds_diff(ds_path, ds_diff)
        return has_changes

    def write_ds_diff(self, ds_path, ds_diff):
        """For outputting ds_diff, the diff of a particular dataset."""
        raise NotImplementedError()

    def get_repo_diff(self):
        """
        Generates a RepoDiff containing an entry for every dataset in the repo (that matches self.repo_key_filter).
        """
        return diff_util.get_repo_diff(
            self.base_rs,
            self.target_rs,
            include_wc_diff=self.include_wc_diff,
            wc_diff_context=self.wc_diff_context,
            repo_key_filter=self.repo_key_filter,
        )

    def get_dataset_diff(self, ds_path):
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
        return diff_util.get_dataset_diff(
            ds_path,
            self.base_rs.datasets(),
            self.target_rs.datasets(),
            include_wc_diff=self.include_wc_diff,
            wc_diff_context=self.wc_diff_context,
            ds_filter=self.repo_key_filter[ds_path],
        )

    def _unfiltered_ds_feature_deltas(self, ds_path, ds_diff):
        if "feature" not in ds_diff:
            return

        yield from ds_diff["feature"].sorted_items()

    def record_spatial_filter_stats_for_dataset(self, ds_path, ds_diff):
        """
        Goes through the given dataset-diff and checks which features match the spatial filter by calling
        record_spatial_filter_stat on each one.
        No need to call this if filtered_ds_feature_deltas is called, which does this as a side effect.
        """
        for _ in self.filtered_ds_feature_deltas(ds_path, ds_diff):
            pass

    def filtered_ds_feature_deltas(self, ds_path, ds_diff):
        """
        Yields the key, delta for only those feature-deltas from the given dataset diff that match
        self.spatial_filter. Note that feature-deltas are always considered to match the spatial-filter
        if they are marked as working-copy edits, since working-copy edits are always relevant to the user
        even if they are outside the spatial filter.
        """
        if "feature" not in ds_diff:
            return

        unfiltered_deltas = self._unfiltered_ds_feature_deltas(ds_path, ds_diff)

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

        delta_fetcher = DeltaFetcher(self, ds_path)

        for key, delta in unfiltered_deltas:
            do_yield = bool(delta.flags & WORKING_COPY_EDIT)
            omr = lazy_eval(lambda: old_spatial_filter.matches_delta_value(delta.old))
            do_yield |= bool(omr)
            nmr = lazy_eval(lambda: new_spatial_filter.matches_delta_value(delta.new))
            do_yield |= bool(nmr)
            if omr is not None and nmr is not None:
                self.record_spatial_filter_stat(ds_path, key, delta, omr, nmr)
            if do_yield:
                if delta_fetcher.ensure_delta_is_ready_or_start_fetch(key, delta):
                    yield key, delta

        yield from delta_fetcher.finish_fetching_deltas()

    def record_spatial_filter_stat(
        self, ds_path, key, delta, old_match_result, new_match_result
    ):
        """
        Records which / how many features were inside / outside the spatial filter for which reasons.
        These records are used by write_warnings_footer to show warnings to the user.
        """
        if self.spatial_filter_pk_conflicts is not None:
            if not old_match_result and delta.old is not None and delta.new is not None:
                self.spatial_filter_pk_conflicts.recursive_set(
                    [ds_path, "feature", key], True
                )

    def _get_old_and_new_schema(self, ds_path, ds_diff):
        from kart.tabular.schema import Schema

        old_schema = new_schema = None
        schema_delta = ds_diff.recursive_get(["meta", "schema.json"])
        if schema_delta and schema_delta.old_value:
            old_schema = Schema.from_column_dicts(schema_delta.old_value)
        if schema_delta and schema_delta.new_value:
            new_schema = Schema.from_column_dicts(schema_delta.new_value)
        if old_schema or new_schema:
            return old_schema, new_schema

        # No diff - old and new schemas are the same.
        ds = self.base_rs.datasets().get(ds_path) or self.target_rs.datasets().get(
            ds_path
        )
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
        ds = self.base_rs.datasets().get(ds_path) or self.target_rs.datasets().get(
            ds_path
        )
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


class DeltaFetcher:
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
