from __future__ import annotations
import logging
import sys
import click
import pygit2

from typing import Union, Type, List
from pathlib import Path

from .conflicts_util import (
    _CONFLICT_PLACEHOLDER,
    set_value_at_dict_path,
    summarise_conflicts,
    conflicts_json_as_text,
)
from .crs_util import CoordinateReferenceString
from .key_filters import RepoKeyFilter
from .merge_util import (
    MergeContext,
    MergedIndex,
    RichConflict,
    rich_conflicts,
    ensure_conflicts_ready,
)
from .output_util import dump_json_output, resolve_output_path
from . import diff_util

L = logging.getLogger("kart.conflicts_writer")


class BaseConflictsWriter:
    """
    A base class for writing conflicts output.
    """

    def __init__(
        self,
        repo: pygit2.Repository,
        user_key_filters: tuple = (),
        output_path: str = "-",
        summarise: int = 0,
        flat: bool = False,
        *,
        json_style: str = "pretty",
        target_crs: CoordinateReferenceString = None,
        merged_index: MergedIndex = None,
        merge_context: MergeContext = None,
    ):
        self.repo = repo
        self.target_crs = target_crs
        self.flat = flat
        self.summarise = summarise
        self.merge_context = merge_context
        self.merged_index = merged_index
        self.repo_key_filter = RepoKeyFilter.build_from_user_patterns(user_key_filters)
        self.json_style = json_style
        self.output_path = self._check_output_path(
            repo, self._normalize_output_path(output_path)
        )
        self.json_style = json_style
        self.target_crs = target_crs
        target_rs = repo.structure("HEAD")
        self.all_ds_paths = diff_util.get_all_ds_paths(
            target_rs, target_rs, self.repo_key_filter
        )

        if merged_index is None:
            self.merged_index = MergedIndex.read_from_repo(repo)
        if merge_context is None:
            self.merge_context = MergeContext.read_from_repo(repo)

    @classmethod
    def _normalize_output_path(cls, output_path: str) -> Union[str, Path]:
        if not output_path or output_path == "-":
            return output_path
        if isinstance(output_path, str):
            output_path = Path(output_path)
        if isinstance(output_path, Path):
            output_path = output_path.expanduser()
        return output_path

    @classmethod
    def get_conflicts_writer_class(
        cls, output_format: str
    ) -> Union[Type[BaseConflictsWriter], None]:
        """Returns suitable subclass for desired output format"""
        output_format_to_writer = {
            "quiet": QuietConflictsWriter,
            "json": JsonConflictsWriter,
            "text": TextConflictsWriter,
            "geojson": GeojsonConflictsWriter,
        }

        cls.output_format = output_format
        if not output_format_to_writer.get(output_format):
            raise click.BadParameter(
                f"Unrecognized output format: {output_format}",
                param_hint="output_format",
            )

        return output_format_to_writer.get(output_format)

    def get_conflicts(self) -> List[RichConflict]:
        """Returns a list of rich conflicts"""
        conflicts = rich_conflicts(
            self.merged_index.unresolved_conflicts.values(),
            self.merge_context,
        )
        return conflicts

    @classmethod
    def _check_output_path(
        cls,
        repo: pygit2.Repository,
        output_path: Union[str, Path],
        output_format: str = None,
    ) -> Union[str, Path]:
        """Make sure the given output_path is valid for this implementation (ie, are directories supported)."""
        if output_format and isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                f"Directory is not valid for --output with -o {output_format}",
                param_hint="--output",
            )
        return output_path

    def exit_with_code(self):
        if self.merged_index.unresolved_conflicts:
            sys.exit(1)
        else:
            sys.exit(0)

    def list_conflicts(self) -> dict:
        """Lists all the conflicts in merged_index, categorised into nested dicts.

        Example:

        ::

            {
                "dataset_A": {
                    "feature":
                        "5": {"ancestor": "...", "ours": ..., "theirs": ...},
                        "11": {"ancestor": "...", "ours": ..., "theirs": ...},
                    },
                    "meta": {
                        "gpkg_spatial_ref_sys": {"ancestor": ..., "ours": ..., "theirs": ...}}
                    }
                },
                "dataset_B": {...}
            }
        """
        output_dict = {}
        conflict_output = _CONFLICT_PLACEHOLDER

        conflicts = self.get_conflicts()
        if not self.repo_key_filter.match_all:
            conflicts = (c for c in conflicts if c.matches_filter(self.repo_key_filter))

        if not self.summarise:
            conflicts = ensure_conflicts_ready(conflicts, self.merge_context.repo)

        for conflict in conflicts:
            if not self.summarise:
                conflict_output = conflict.output(
                    self.output_format,
                    include_label=self.flat,
                    target_crs=self.target_crs,
                )

            if self.flat:
                if isinstance(conflict_output, dict):
                    output_dict.update(conflict_output)
                else:
                    output_dict[conflict.label] = conflict_output
            else:
                set_value_at_dict_path(
                    output_dict, conflict.decoded_path, conflict_output
                )

        if self.summarise:
            output_dict = summarise_conflicts(output_dict, self.summarise)
        return output_dict


class QuietConflictsWriter(BaseConflictsWriter):
    def write_conflicts(self):
        pass


class JsonConflictsWriter(BaseConflictsWriter):
    """Writes JSON conficts.

    Of all the conflict-writers, JSON conflicts are the most descriptive - nothing is left out.
    The basic conflicts structure is as follows - for meta items:
        {"kart.conflicts/v1": {"dataset-path": { "meta": { "schema.json": { "ancestor/ours/theirs": [...]}}}}}
    And for features:
        {"kart.conflicts/v1": {"dataset-path": { "feature": { "id": { "ancestor/ours/theirs": [...]}}}}}
    """

    @classmethod
    def _check_output_path(
        cls,
        repo: pygit2.Repository,
        output_path: Union[str, Path],
        output_format: str = "json",
    ) -> Union[str, Path]:
        """Make sure the given output_path is valid for this implementation (ie, are directories supported)."""
        return super()._check_output_path(repo, output_path, output_format)

    def write_conflicts(self):
        output_obj = super().list_conflicts()
        dump_json_output(
            {"kart.conflicts/v1": output_obj},
            self.output_path,
            json_style=self.json_style,
        )


class TextConflictsWriter(BaseConflictsWriter):
    """Writes human-readable conflicts.

    Non-empty geometries are not specified in full - instead they look like this:
    POINT(...) or POLYGON(...) - so conflicts of this kind are lossy where geometry is involved, and shouldn't be parsed.
    Instead, use a JSON conflict if you need to parse it, as `kart create-patch` does.
    Any changes to schema.json will be highlighted in a human-readable way, other meta-items conflicts will simply show
    the changes on the ancestor, theirs and ours branch.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fp = resolve_output_path(self.output_path)
        self.pecho = {"file": self.fp, "color": self.fp.isatty()}

    @classmethod
    def _check_output_path(
        cls,
        repo: pygit2.Repository,
        output_path: Union[str, Path],
        output_format: str = "text",
    ) -> Union[str, Path]:
        """Make sure the given output_path is valid for this implementation (ie, are directories supported)."""
        return super()._check_output_path(repo, output_path, output_format)

    def write_conflicts(self):
        output_dict = super().list_conflicts()
        click.secho(conflicts_json_as_text(output_dict), **self.pecho)


class GeojsonConflictsWriter(BaseConflictsWriter):
    """Writes all feature conflicts as a single GeoJSON FeatureCollection of GeoJSON features.

    The id of each feature in the collection indicates which branch it belongs to.

    Example:

        dataset:feature:123:ancestor - the common ancestor version of feature 123
        dataset:feature:123:ours - our version of the conflicting change made to feature 123
        dataset:feature:123:theirs - their version of the conflicting change made to feature 123

    Note:

        Meta conflicts aren't output at all.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flat = True
        self.summarise = 0

    @classmethod
    def _check_output_path(cls, repo: pygit2.Repository, output_path: Path):
        """Make sure the given output_path is valid for this implementation (ie, are directories supported)."""
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

    def write_conflicts(self) -> None:
        if len(self.all_ds_paths) > 1 and not isinstance(self.output_path, Path):
            raise click.BadParameter(
                "Need to specify a directory via --output for GeoJSON with more than one dataset",
                param_hint="--output",
            )
        conflicts = self.get_conflicts()

        if not self.repo_key_filter.match_all:
            conflicts = (c for c in conflicts if c.matches_filter(self.repo_key_filter))

        conflicts = ensure_conflicts_ready(conflicts, self.merge_context.repo)

        geojson_conflicts = self.get_geojson_conflicts(conflicts)
        self.output_geojson_conflicts(geojson_conflicts)

    def get_geojson_conflicts(self, conflicts: List[RichConflict]) -> dict:
        """Returns geojson conflicts as a dict"""
        output_dict = {}
        for conflict in conflicts:
            conflict_output = conflict.output(
                "geojson", include_label=self.flat, target_crs=self.target_crs
            )

            if isinstance(conflict_output, dict):
                output_dict.update(conflict_output)
            else:
                output_dict[conflict.label] = conflict_output
        return output_dict

    def output_geojson_conflicts(self, json_obj: dict) -> None:
        """Writes the geojson conflicts to the specified output stream"""
        self._warn_about_any_meta_conflicts(json_obj)
        conflicts = self.separate_geojson_conflicts_by_ds(json_obj)

        for ds_path, features in conflicts.items():
            if self.output_path == "-":
                ds_output_path = "-"
            else:
                ds_output_filename = str(ds_path).replace("/", "__") + ".geojson"
                ds_output_path = self.output_path / ds_output_filename

            output_obj = {"type": "FeatureCollection", "features": features}
            dump_json_output(
                output_obj,
                ds_output_path,
                json_style=self.json_style,
            )

    def separate_geojson_conflicts_by_ds(self, json_obj: dict) -> dict:
        """Separates geojson conflicts by datasets"""
        conflicts = dict()
        for key, feature in json_obj.items():
            if "meta" == key.split(":")[1]:
                continue
            ds_path = key.split(":")[0]
            features = conflicts.get(ds_path, [])
            feature["id"] = key
            features.append(feature)
            conflicts[ds_path] = features
        return conflicts

    def _warn_about_any_meta_conflicts(self, json_obj: dict) -> None:
        ds_path = set()
        for key, feature in json_obj.items():
            if "meta" == key.split(":")[1]:
                ds_path.add(key.split(":")[0])

        for path in ds_path:
            click.echo(
                f"Warning: {path} meta changes aren't included in GeoJSON output.",
                err=True,
            )
