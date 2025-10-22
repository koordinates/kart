import logging
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator

import click

from typing import Union

from kart.diff_format import DiffFormat
from .base_diff_writer import BaseDiffWriter
from .diff_estimation import (
    ThreadTerminated,
    estimate_diff_feature_counts,
    terminate_estimate_thread,
)
from kart.diff_structs import FILES_KEY, BINARY_FILE, DatasetDiff
from kart.key_filters import DeltaFilter
from kart.log import commit_obj_to_json
from kart.output_util import (
    dump_json_output,
    resolve_output_path,
    msgspec_json_encoder,
)

from kart.tabular.feature_output import feature_as_geojson, feature_as_json
from kart.timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz

L = logging.getLogger(__name__)


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

    def __init__(self, *args, delta_filter=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.delta_filter = delta_filter

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

    def write_diff(self, diff_format=DiffFormat.FULL):
        # TODO - optimise - no need to generate the entire repo diff before starting output.
        # (This is not quite as bad as it looks, since parts of the diff object are lazily generated.)
        repo_diff = self.get_repo_diff(diff_format=diff_format)
        self.has_changes = bool(repo_diff)
        for ds_path, ds_diff in repo_diff.items():
            ds_diff.ds_path = ds_path

        output_obj = {}
        self.add_json_header(output_obj)
        if diff_format != DiffFormat.NONE:
            output_obj["kart.diff/v1+hexwkb"] = repo_diff

        dump_json_output(
            output_obj,
            self.output_path,
            json_style=self.json_style,
            encoder_kwargs={"default": self.default},
        )
        self.write_warnings_footer()

    def _postprocess_simple_delta(self, delta):
        return delta.to_plus_minus_dict(self.delta_filter)

    def _postprocess_attachment_delta(self, delta):
        if self.do_full_file_diffs:
            delta = self._full_file_delta(delta)
        return delta.to_plus_minus_dict(self.delta_filter)

    def default(self, obj):
        # Part of JsonEncoder interface - adapt objects that couldn't otherwise be encoded.
        if isinstance(obj, DatasetDiff):
            ds_path, ds_diff = obj.ds_path, obj
            if ds_path == FILES_KEY:
                return {
                    key: self._postprocess_attachment_delta(value)
                    for key, value in self.iter_deltadiff_items(ds_diff[FILES_KEY])
                }

            result = {}
            if "meta" in ds_diff:
                result["meta"] = {
                    key: self._postprocess_simple_delta(value)
                    for key, value in ds_diff["meta"].items()
                }
            if "data_changes" in ds_diff:
                result["data_changes"] = ds_diff["data_changes"]
            item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
            if item_type and item_type in ds_diff:
                result[item_type] = self.filtered_dataset_deltas_as_json(
                    ds_path, ds_diff
                )
            return result

        return None

    def filtered_dataset_deltas_as_json(self, ds_path, ds_diff):
        item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
        if not item_type or item_type not in ds_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)

        for key, delta in self.filtered_dataset_deltas(ds_path, ds_diff):
            yield self.delta_as_json(delta, old_transform, new_transform)

    def delta_as_json(self, delta, old_transform, new_transform):
        result = {}
        for json_key, feature in delta.to_plus_minus_dict(self.delta_filter).items():
            pk_value = delta.old_key if "-" in json_key else delta.new_key
            transform = old_transform if "-" in json_key else new_transform
            result[json_key] = (
                feature_as_json(feature, pk_value, transform) if feature else None
            )
        return result


class PatchWriter(JsonDiffWriter):
    """
    PatchWriter is the same as JsonDiffWriter except for how the commit object is serialised -
    - it only has information that will be kept when the patch is reapplied (ie, authorName, but not committerName).
    - it is at the key "kart.patch/v1" instead of "kart.show/v1"

    PatchWriter always uses the "advanced" delta format with unambiguous keys:
    - "++" for inserts
    - "--" for deletes
    - "+" and "-" for updates
    """

    # Avoid any ambiguity in file-patches.
    TEXT_PREFIX = "text:"

    def __init__(self, *args, target_crs_str=None, **kwargs):
        # Don't set delta_filter in __init__ - we'll handle it per-context
        # (features use advanced format, metadata uses simple format)
        super().__init__(*args, **kwargs)
        self.target_crs_str = target_crs_str
        self.nonmatching_item_counts = {p: 0 for p in self.all_ds_paths}

    def add_json_header(self, obj):
        if self.commit is not None:
            author = self.commit.author
            author_time = datetime.fromtimestamp(author.time, timezone.utc)
            author_time_offset = timedelta(minutes=author.offset)

            try:
                original_parent = self.commit.parent_ids[0].hex
            except IndexError:
                original_parent = None

            obj["kart.patch/v1"] = {
                "authorName": author.name,
                "authorEmail": author.email,
                "authorTime": datetime_to_iso8601_utc(author_time),
                "authorTimeOffset": timedelta_to_iso8601_tz(author_time_offset),
                "message": self.commit.message,
                "base": original_parent,
            }
            if not original_parent:
                del obj["kart.patch/v1"]["base"]

            if self.target_crs_str is not None:
                obj["kart.patch/v1"]["crs"] = self.target_crs_str

    def delta_as_json(self, delta, old_transform, new_transform):
        """
        Override to use advanced format (++/--/-/+) for features and to exclude '-' keys
        for updates when target_crs is set.
        Per the docs, reprojected patches must not include both '-' and '+' for updates.
        """
        from .key_filters import DeltaFilter

        result = {}
        # Always use advanced format for features (++/-- for insert/delete, -/+ for update)
        plus_minus_dict = delta.to_plus_minus_dict(DeltaFilter.MATCH_ALL)

        # If target_crs is set and this is an update (has both old and new), exclude the '-' key
        if (
            self.target_crs is not None
            and "-" in plus_minus_dict
            and "+" in plus_minus_dict
        ):
            # This is an update with CRS transformation - only include the '+' key
            pk_value = delta.new_key
            transform = new_transform
            result["+"] = (
                feature_as_json(plus_minus_dict["+"], pk_value, transform)
                if plus_minus_dict["+"]
                else None
            )
        else:
            # Normal behavior: inserts (++), deletes (--), or updates (+/-)
            for json_key, feature in plus_minus_dict.items():
                pk_value = delta.old_key if "-" in json_key else delta.new_key
                transform = old_transform if "-" in json_key else new_transform
                result[json_key] = (
                    feature_as_json(feature, pk_value, transform) if feature else None
                )
        return result

    def record_spatial_filter_stat(
        self, ds_path, item_type, key, delta, old_match_result, new_match_result
    ):
        """
        Records which / how many features were inside / outside the spatial filter for which reasons.
        These records are used by write_warnings_footer to show warnings to the user.
        """
        if not old_match_result and not new_match_result:
            self.nonmatching_item_counts[ds_path] += 1

    def write_warnings_footer(self):
        super().write_warnings_footer()
        if any(self.nonmatching_item_counts.values()):
            click.secho(
                "Warning: The generated patch does not contain the entire commit: ",
                bold=True,
                err=True,
            )
            for ds_path, count in self.nonmatching_item_counts.items():
                if not count:
                    continue
                item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
                click.echo(
                    f"  In dataset {ds_path} there are {count} changed {item_type}s not included due to spatial filter",
                    err=True,
                )


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

    def __init__(self, *args, diff_estimate_accuracy=None, delta_filter=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fp = resolve_output_path(self.output_path)

        self._diff_estimate_accuracy = diff_estimate_accuracy
        self.delta_filter = delta_filter
        self._output_lock = threading.RLock()
        # https://jcristharif.com/msgspec/perf-tips.html#reusing-an-output-buffer
        self._output_buffer = bytearray()

    def dump(self, obj):
        with self._output_lock:
            # https://jcristharif.com/msgspec/perf-tips.html#line-delimited-json
            msgspec_json_encoder.encode_into(obj, self._output_buffer)
            self._output_buffer.extend(b"\n")
            self.fp.buffer.write(self._output_buffer)

    def write_header(self):
        self.dump(
            {
                "type": "version",
                "version": "kart.diff/v2",
                "outputFormat": "JSONL+hexwkb",
            }
        )
        if self._diff_estimate_accuracy is not None:
            t = threading.Thread(
                target=self._calculate_and_feature_count_estimate,
            )
            t.start()
        if self.commit:
            self.dump({"type": "commit", "value": commit_obj_to_json(self.commit)})

    def _calculate_and_feature_count_estimate(self):
        """
        Runs in a separate thread. Calculates the diff estimate for this diff, and inserts it
        into the JSON-Lines stream when it is calculated. Doesn't otherwise block the main thread.
        """
        try:
            est = estimate_diff_feature_counts(
                self.repo,
                self.base_rs.tree,
                self.target_rs.tree,
                include_wc_diff=self.include_wc_diff,
                accuracy=self._diff_estimate_accuracy,
            )
        except ThreadTerminated:
            return
        else:
            self.dump(
                {
                    "type": "featureCountEstimate",
                    "accuracy": self._diff_estimate_accuracy,
                    "datasets": est,
                }
            )

    def write_ds_diff(self, ds_path, ds_diff, diff_format=DiffFormat.FULL):
        dataset = self._get_old_or_new_dataset(ds_path)
        schema_json_delta = ds_diff.recursive_get(["meta", "schema.json"])

        # The diffs generated by our datasets aren't explicit about datasets being added and removed.
        # It can be inferred by checking if the schema.json meta-item has been added or removed.
        # In JSONL diffs, we then turn that into an explicit message about the dataset.
        if schema_json_delta is not None and schema_json_delta.type in (
            "insert",
            "delete",
        ):
            # Dataset is being added or removed - output that explicitly:
            message_type = "dataset"  # A dataset diff.
            key = "+" if schema_json_delta.type == "insert" else "-"
        else:
            # Dataset is not being added or removed, but we still output info about the dataset as useful context.
            message_type = "datasetInfo"  # Not a diff - some info about a dataset.
            key = "value"

        self.dump(
            {
                "type": message_type,
                "path": ds_path,
                key: {"type": dataset.DATASET_TYPE, "version": dataset.VERSION},
            }
        )

        if schema_json_delta is None:
            # Dataset's schema is not being changed, but we still output it as useful context.
            self.dump(
                {
                    "type": "metaInfo",
                    "dataset": ds_path,
                    "key": "schema.json",
                    "value": dataset.get_meta_item("schema.json"),
                }
            )

        self.write_meta_deltas(ds_path, ds_diff)
        if diff_format == DiffFormat.FULL.value:
            self.write_filtered_dataset_deltas(ds_path, ds_diff)
        elif diff_format == DiffFormat.NO_DATA_CHANGES.value:
            self.dump(
                {
                    "type": "dataChanges",
                    "dataset": ds_path,
                    "value": ds_diff["data_changes"],
                }
            )

    def write_meta_deltas(self, ds_path, ds_diff):
        if "meta" not in ds_diff:
            return

        obj = {"type": "meta", "dataset": ds_path, "key": None, "change": None}
        for key, delta in self.iter_deltadiff_items(ds_diff["meta"]):
            obj["key"] = key
            obj["change"] = delta.to_plus_minus_dict()
            self.dump(obj)

    def write_filtered_dataset_deltas(self, ds_path, ds_diff):
        item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
        if not item_type or item_type not in ds_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)

        obj = {"type": item_type, "dataset": ds_path, "change": None}

        for key, delta in self.filtered_dataset_deltas(ds_path, ds_diff):
            obj["type"] = item_type
            obj["change"] = self.delta_as_json(delta, old_transform, new_transform)
            self.dump(obj)

    def delta_as_json(self, delta, old_transform, new_transform):
        result = {}
        for json_key, feature in delta.to_plus_minus_dict(self.delta_filter).items():
            pk_value = delta.old_key if "-" in json_key else delta.new_key
            transform = old_transform if "-" in json_key else new_transform
            result[json_key] = (
                feature_as_json(feature, pk_value, transform) if feature else None
            )
        return result

    def write_file_diff(self, file_diff):
        obj = {"type": "file", "path": None, "binary": False, "change": None}
        if not self.do_full_file_diffs:
            obj.pop("binary")

        for key, delta in self.iter_deltadiff_items(file_diff):
            obj["path"] = key
            if self.do_full_file_diffs:
                delta = self._full_file_delta(delta)
                obj["binary"] = bool(delta.flags & BINARY_FILE)
            obj["change"] = delta.to_plus_minus_dict()
            self.dump(obj)

        return bool(file_diff)

    def write_warnings_footer(self):
        # If there's an estimate thread running (see write_header()), ask it to terminate
        terminate_estimate_thread.set()
        super().write_warnings_footer()


class GeojsonDiffWriter(BaseDiffWriter):
    """Writes all feature deltas as a single GeoJSON FeatureCollection of GeoJSON features.

    The name of each feature in the collection indicates whether it is the old or new version of the feature,
    or if it was inserted or deleted.

    Example:

        dataset:feature:123:U-  - old version of feature 123
        dataset:feature:123:U+  - new version of features 123
        dataset:feature:123:D   - feature 123 as it was before it was deleted
        dataset:feature:123:I   - features 123 as it is after it was inserted

    Note:

        Meta deltas aren't output at all.
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path):
            if output_path.is_file():
                raise click.BadParameter(
                    "Output path should be a directory for GeoJSON format.",
                    param_hint="--output",
                )
            if not output_path.exists():
                output_path.mkdir()
            else:
                geojson_paths = list(output_path.glob("*.geojson"))
                if geojson_paths:
                    L.debug(
                        "Cleaning %d *.geojson files from %s ...",
                        len(geojson_paths),
                        output_path,
                    )
                for p in geojson_paths:
                    p.unlink()
        else:
            output_path = "-"
        return output_path

    def write_diff(self, diff_format=DiffFormat.FULL):
        if diff_format != DiffFormat.FULL.value:
            raise click.UsageError("GeoJSON format only supports full diffs")
        repo_diff = self.get_repo_diff(include_files=False, diff_format=diff_format)

        self.has_changes = bool(repo_diff)
        if len(repo_diff) > 1 and not isinstance(self.output_path, Path):
            raise click.BadParameter(
                "Need to specify a directory via --output for GeoJSON with more than one dataset",
                param_hint="--output",
            )
        for ds_path, ds_diff in repo_diff.items():
            self._warn_about_any_non_feature_diffs(ds_path, ds_diff)
            output_obj = {
                "type": "FeatureCollection",
                "features": self.filtered_dataset_deltas_as_geojson(ds_path, ds_diff),
            }

            if self.output_path == "-":
                ds_output_path = "-"
            else:
                ds_output_filename = str(ds_path).replace("/", "__") + ".geojson"
                ds_output_path = self.output_path / ds_output_filename
            dump_json_output(
                output_obj,
                ds_output_path,
                json_style=self.json_style,
            )
        self.write_warnings_footer()

    def _warn_about_any_non_feature_diffs(
        self, ds_path: str, ds_diff: DatasetDiff
    ) -> None:
        if "meta" in ds_diff:
            meta_changes = ", ".join(ds_diff["meta"].keys())
            click.echo(
                f"Warning: {ds_path} meta changes aren't included in GeoJSON output: {meta_changes}",
                err=True,
            )
        if "tile" in ds_diff:
            count = len(ds_diff["tile"])
            click.echo(
                f"Warning: {count} tile changes in {ds_path} aren't included in GeoJSON output",
                err=True,
            )
