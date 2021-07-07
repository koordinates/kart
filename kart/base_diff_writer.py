import itertools
import logging
import re
import sys

import click

from .diff_structs import RepoDiff, DatasetDiff
from .exceptions import InvalidOperation, NotFound, NotYetImplemented, NO_WORKING_COPY
from .key_filters import RepoKeyFilter


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

        base_ds_paths = {ds.path for ds in self.base_rs.datasets}
        target_ds_paths = {ds.path for ds in self.target_rs.datasets}
        all_ds_paths = base_ds_paths | target_ds_paths

        if not self.repo_key_filter.match_all:
            all_ds_paths = all_ds_paths & self.repo_key_filter.keys()

        self.all_ds_paths = sorted(list(all_ds_paths))

        self.output_path = self._check_output_path(repo, output_path)

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
        """Returns the DatasetDiff object for the dataset at path dataset_path."""

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

    def get_geometry_transforms(self, ds_path, ds_diff):
        """
        Returns old_transform, new_transform for the dataset at a particular path -
        where old_transform is the transform that should be applied to old, pre-diff values,
        and new_transform is the transform that should be applied to new, post-diff values,
        in order that all geometry values output are now in self.target_crs
        """

        if self.target_crs is None:
            return None, None

        # Check if the CRS is changing during the diff - in which case,
        # we need to reproject old and new geometries differently.
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
                raise NotYetImplemented(
                    f"Sorry, reprojecting dataset {ds_path!r} with multiple CRS into target CRS is not yet supported"
                )
            old_transform, new_transform = None, None
            if old_crs_defs:
                old_transform = self._make_transform(old_crs_defs[0], ds_path)
            if new_crs_defs:
                new_transform = self._make_transform(new_crs_defs[0], ds_path)
            if old_crs_defs or new_crs_defs:
                return old_transform, new_transform

        # No diff case - old and new transform are the same.
        ds = self.base_rs.datasets.get(ds_path) or self.target_rs.datasets.get(ds_path)
        transform = ds.get_geometry_transform(self.target_crs)
        return transform, transform

    def _make_transform(self, crs_def, ds_path):
        from osgeo import osr
        from kart.geometry import make_crs

        try:
            return osr.CoordinateTransformation(make_crs(crs_def), self.target_crs)
        except RuntimeError as e:
            raise InvalidOperation(
                f"Can't reproject dataset {ds_path!r} into target CRS: {e}"
            )

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
