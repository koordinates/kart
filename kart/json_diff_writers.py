from datetime import datetime, timezone, timedelta
import json
from pathlib import Path

import click

from .base_diff_writer import BaseDiffWriter
from .diff_structs import DatasetDiff, Delta
from .diff_output import json_row, geojson_row
from .log import commit_obj_to_json
from .output_util import dump_json_output, resolve_output_path
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz


class JsonDiffWriter(BaseDiffWriter):
    """
    Writes JSON diffs, with geometry encoded using hexwkb.
    Of all the diff-writers, JSON diffs are the most descriptive - nothing is left out (which is why the PatchWriter
    is just a slight variation of the JSON diff writer).
    As the output is a single JSON object, json.dumps interface requires that entire RepoDiff object representing the diff is
    generated first, and then dumped. This means the diff can be slow to start - the situation is improved somewhat by
    the fact that Delta's can be lazily evaluated, so at least individual blobs needn't be read until each delta is output.
    JsonLinesDiffWriter is faster to start for multi-dataset repos since it generates diffs repo by repo.

    The basic diff structure is as follows - for meta items:
      {"kart.diff/v1+hexwkb": {dataset-path: {"meta": {meta-item-name: {"-/+": old/new-value}}}}}
    And for features:
      {"kart.diff/v1+hexwkb": {dataset-path: {"feature": [{"-/+": old/new-value}, ...]}}}

    For kart show, there is another top level key alongside "kart.diff/v1+hexwkb" - that is "kart.show/v1+hexwkb",
    which contains information about the commit object.
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )
        return output_path

    def add_json_header(self, obj):
        if self.commit is not None:
            obj["kart.show/v1"] = commit_obj_to_json(self.commit)

    def write_diff(self):
        # TODO - optimise - no need to generate the entire repo diff before starting output.
        # (This is not quite as bad as it looks, since parts of the diff object are lazily generated.)
        repo_diff = self.get_repo_diff()
        self.has_changes = bool(repo_diff)

        for ds_path, ds_diff in repo_diff.items():
            ds_diff.ds_path = ds_path

        output_obj = {}
        self.add_json_header(output_obj)
        output_obj["kart.diff/v1+hexwkb"] = repo_diff

        dump_json_output(
            output_obj,
            self.output_path,
            json_style=self.json_style,
            encoder_kwargs={"default": self.default},
        )

    def default(self, obj):
        # Part of JsonEncoder interface - adapt objects that couldn't otherwise be encoded.
        if isinstance(obj, DatasetDiff):
            ds_path, ds_diff = obj.ds_path, obj
            self._old_transform, self._new_transform = self.get_geometry_transforms(
                ds_path, ds_diff
            )
            return None  # Handled by ExtendedJsonEncoder

        if isinstance(obj, Delta):
            return self.encode_delta(obj)
        return None

    def encode_delta(self, delta):
        result = {}
        if delta.old:
            result["-"] = json_row(delta.old_value, delta.old_key, self._old_transform)
        if delta.new:
            result["+"] = json_row(delta.new_value, delta.new_key, self._new_transform)
        return result


class PatchWriter(JsonDiffWriter):
    """
    PatchWriter is the same as JsonDiffWriter except for how the commit object is serialised -
    - it only has information that will be kept when the patch is reapplied (ie, authorName, but not committerName).
    - it is at the key "kart.patch/v1" instead of "kart.show/v1"
    """

    def add_json_header(self, obj):
        if self.commit is not None:
            author = self.commit.author
            author_time = datetime.fromtimestamp(author.time, timezone.utc)
            author_time_offset = timedelta(minutes=author.offset)

            obj["kart.patch/v1"] = {
                "authorName": author.name,
                "authorEmail": author.email,
                "authorTime": datetime_to_iso8601_utc(author_time),
                "authorTimeOffset": timedelta_to_iso8601_tz(author_time_offset),
                "message": self.commit.message,
            }


class JsonLinesDiffWriter(BaseDiffWriter):
    """
    Writes diffs using JSON-lines, which means, diff output can begin as soon as the first delta is known  Python's json
    library makes it very difficult to begin to json.dumps a dictionary without knowing how many entries it must have,
    which is a problem for the JsonDiffWriter - the top level dict which has one key per dataset-which-has-changes therefore
    requires we at least generate a list of all datasets which have changes before outputting anything. JSON-lines solves this.
    Similarly, it is also easier for clients to parse one line at a time and make use of it - most JSON decoding libraries
    will not make it easy to use information from a partially parsed top-level object.

    The messages that are streamed by the JsonLines diff-writer take the following form:
    Header:
      {"type": "version", "version": "kart.diff/v2", "outputFormat": "JSONL+hexwkb"}
    Commit (for kart show command):
      {"type": "commit", "value": {commit-info}}
    Meta info which hasn't changed - only output for schema.json:
      {"type": "metaInfo", "dataset": dataset-path, "key": "schema.json", "value" {schema-json}}
    Meta into which has changed:
      {"type": "meta", "dataset": dataset-path, "key": "schema.json", "change": {"-/+": old/new-value}}
    Feature which has changed:
      {"type": "feature", "dataset": dataset-path, "change": {"-/+": old/new-value}}
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )
        return output_path

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fp = resolve_output_path(self.output_path)
        self.separators = (",", ":") if self.json_style == "extracompact" else None

    def dump(self, obj):
        json.dump(obj, self.fp, separators=self.separators)
        self.fp.write("\n")

    def write_header(self):
        self.dump(
            {
                "type": "version",
                "version": "kart.diff/v2",
                "outputFormat": "JSONL+hexwkb",
            }
        )
        if self.commit:
            self.dump({"type": "commit", "value": commit_obj_to_json(self.commit)})

    def write_ds_diff(self, ds_path, ds_diff):
        if "schema.json" not in ds_diff.get("meta", {}):
            dataset = self.base_rs.datasets.get(ds_path) or self.target_rs.datasets.get(
                ds_path
            )
            self.dump(
                {
                    "type": "metaInfo",
                    "dataset": ds_path,
                    "key": "schema.json",
                    "value": dataset.schema.to_column_dicts(),
                }
            )

        self.write_meta_deltas(ds_path, ds_diff)
        self.write_feature_deltas(ds_path, ds_diff)

    def write_meta_deltas(self, ds_path, ds_diff):
        obj = {"type": "meta", "dataset": ds_path, "key": None, "change": None}
        for key, delta in sorted(ds_diff.get("meta", {}).items()):
            obj["key"] = key
            obj["change"] = delta.to_plus_minus_dict()
            self.dump(obj)

    def write_feature_deltas(self, ds_path, ds_diff):
        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)
        obj = {"type": "feature", "dataset": ds_path, "change": None}
        for key, delta in sorted(ds_diff.get("feature", {}).items()):
            change = {}
            if delta.old:
                change["-"] = json_row(delta.old_value, delta.old_key, old_transform)
            if delta.new:
                change["+"] = json_row(delta.new_value, delta.new_key, new_transform)
            obj["change"] = change
            self.dump(obj)


class GeojsonDiffWriter(BaseDiffWriter):
    """
    Writes all feature deltas as a single GeoJSON FeatureCollection of GeoJSON features.
    Meta deltas aren't output at all.
    The name of each feature in the collection indicates whether it is the old or new version of the feature,
    or if it was inserted or deleted. For example:
    U-::123  - old version of feature 123
    U+::123  - new version of features 123
    D::123   - feature 123 as it was before it was deleted
    I::123   - features 123 as it is after it was inserted
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        # DONOTSUBMIT - check path type, handle directories
        return output_path

    def write_diff(self):
        output_obj = {
            "type": "FeatureCollection",
            "features": self.all_repo_feature_deltas(),
        }

        dump_json_output(
            output_obj,
            self.output_path,
            json_style=self.json_style,
        )

    def all_repo_feature_deltas(self):
        has_changes = False
        for ds_path in self.all_ds_paths:
            ds_diff = self.get_dataset_diff(ds_path)
            has_changes |= bool(ds_diff)
            self._warn_about_any_meta_diffs(ds_path, ds_diff)
            yield from self.all_ds_feature_deltas(ds_path, ds_diff)
        self.has_changes = has_changes

    def _warn_about_any_meta_diffs(self, ds_path, ds_diff):
        if "meta" in ds_diff:
            meta_changes = ", ".join(ds_diff["meta"].keys())
            click.echo(
                f"Warning: {ds_path} meta changes aren't included in GeoJSON output: {meta_changes}",
                err=True,
            )

    def all_ds_feature_deltas(self, ds_path, ds_diff):
        feature_diff = ds_diff.get("feature")
        if not feature_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)

        deltas = (value for key, value in sorted(feature_diff.items()))
        for delta in deltas:
            if delta.old:
                change_type = "U-" if delta.new else "D"
                yield geojson_row(
                    delta.old_value, delta.old_key, change_type, old_transform
                )
            if delta.new:
                change_type = "U+" if delta.old else "I"
                yield geojson_row(
                    delta.new_value, delta.new_key, change_type, new_transform
                )
