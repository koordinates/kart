import json
from pathlib import Path
import string
import sys
import webbrowser

import click

from .base_diff_writer import BaseDiffWriter
from .feature_output import feature_as_geojson
from .output_util import ExtendedJsonEncoder, resolve_output_path


class HtmlDiffWriter(BaseDiffWriter):
    """
    Writes a file usually called DIFF.html (the default name), which contains both a GeoJSON viewer, and the diff itself
    in GeoJSON. Automatically opens the created file using webbrowser if the created file is not stdout.
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )
        return output_path or repo.workdir_path / "DIFF.html"

    def write_diff(self):
        with open(
            Path(__file__).resolve().with_name("diff-view.html"), "r", encoding="utf8"
        ) as ft:
            template = string.Template(ft.read())

        repo_diff = self.get_repo_diff()
        self.has_changes = bool(repo_diff)

        if self.commit:
            commit_spec_desc = self.commit.short_id
        else:
            commit_spec_desc = f"{self.base_rs.short_id} ... {self.target_rs.short_id if self.target_rs else 'working-copy'}"

        title = f"{self.repo.workdir_path.stem}: {commit_spec_desc}"

        all_datasets_geojson = {
            ds_path: {
                "type": "FeatureCollection",
                "features": self.all_ds_feature_deltas(ds_path, ds_diff),
            }
            for ds_path, ds_diff in repo_diff.items()
        }

        fo = resolve_output_path(self.output_path)
        fo.write(
            template.substitute(
                {
                    "title": title,
                    "geojson_data": json.dumps(
                        all_datasets_geojson, cls=ExtendedJsonEncoder
                    ),
                }
            )
        )

        if fo != sys.stdout:
            fo.close()
            webbrowser.open_new(f"file://{self.output_path.resolve()}")

    def all_ds_feature_deltas(self, ds_path, ds_diff):
        feature_diff = ds_diff.get("feature")
        if not feature_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)

        deltas = feature_diff.values()
        for delta in deltas:
            if delta.old:
                change_type = "U-" if delta.new else "D"
                yield feature_as_geojson(
                    delta.old_value, delta.old_key, change_type, old_transform
                )
            if delta.new:
                change_type = "U+" if delta.old else "I"
                yield feature_as_geojson(
                    delta.new_value, delta.new_key, change_type, new_transform
                )
