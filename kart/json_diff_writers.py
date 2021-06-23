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
