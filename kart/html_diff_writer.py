import html
import json
import string
import sys
import os
import webbrowser
from pathlib import Path

import click

import kart
from kart.crs_util import make_crs
from kart.diff_format import DiffFormat
from .base_diff_writer import BaseDiffWriter
from .json_diff_writers import GeojsonDiffWriter
from .output_util import ExtendedJsonEncoder, resolve_output_path


class HtmlDiffWriter(BaseDiffWriter):
    """
    Writes a file usually called DIFF.html (the default name), which contains both a GeoJSON viewer, and the diff itself
    in GeoJSON. Automatically opens the created file using webbrowser if the created file is not stdout.
    """

    def __init__(self, *args, target_crs=None, **kwargs):
        if target_crs is None:
            target_crs = make_crs("EPSG:4326")
        super().__init__(*args, target_crs=target_crs, **kwargs)

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )
        return output_path or repo.workdir_path / "DIFF.html"

    def write_diff(self, diff_format=DiffFormat.FULL):
        template_path = self.html_template or (
            Path(kart.package_data_path) / "diff-view.html"
        )
        if not os.path.exists(template_path):
            raise click.UsageError("Html template not found")
        if diff_format != DiffFormat.FULL:
            raise click.UsageError("Html format only supports full diffs")
        with open(template_path, "r", encoding="utf8") as ft:
            template = string.Template(ft.read())

        repo_diff = self.get_repo_diff(diff_format=diff_format)
        self.has_changes = bool(repo_diff)

        if self.commit:
            commit_spec_desc = self.commit.short_id
        else:
            commit_spec_desc = f"{self.base_rs.short_id} ... {self.target_rs.short_id if self.target_rs else 'working-copy'}"

        title = f"{self.repo.workdir_path.stem}: {commit_spec_desc}"

        all_datasets_geojson = {
            ds_path: {
                "type": "FeatureCollection",
                "features": self.filtered_dataset_deltas_as_geojson(ds_path, ds_diff),
            }
            for ds_path, ds_diff in repo_diff.items()
            if ds_path != "<files>"
        }

        fo = resolve_output_path(self.output_path)
        fo.write(self.substitute_into_template(template, title, all_datasets_geojson))

        if fo != sys.stdout:
            fo.close()
            webbrowser.open_new(f"file://{self.output_path.resolve()}")

        self.write_warnings_footer()

    @classmethod
    def substitute_into_template(cls, template, title, all_datasets_geojson):
        return template.substitute(
            {
                "title": html.escape(title),
                "geojson_data": json.dumps(
                    all_datasets_geojson, cls=ExtendedJsonEncoder
                )
                .replace("/", r"\x2f")
                .replace("<", r"\x3c")
                .replace(">", r"\x3e"),
            }
        )
