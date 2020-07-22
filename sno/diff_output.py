import contextlib
import io
import itertools
import json
import os
import re
import string
import sys
import tempfile
import webbrowser
from pathlib import Path

import click

from .geometry import gpkg_geom_to_ogr, gpkg_geom_to_hex_wkb
from .output_util import dump_json_output, resolve_output_path
from .utils import ungenerator


@contextlib.contextmanager
def diff_output_quiet(**kwargs):
    """
    Contextmanager.
    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    Writes nothing to the output. This is useful when you just want to find out
    whether anything has changed in the diff (you can use the exit code)
    and don't need output.
    """

    def _out(dataset, diff):
        pass

    yield _out


_NULL = object()


@contextlib.contextmanager
def diff_output_text(*, output_path, **kwargs):
    """
    Contextmanager.

    Yields a callable which can be called with dataset diffs.
    The callable takes two arguments:
        dataset: A sno.structure.DatasetStructure instance representing
                 either the old or new version of the dataset.
        diff:    The sno.diff.Diff instance to serialize

    On exit, writes a human-readable diff to the given output file.

    Certain shortcuts are taken to make the diff human readable,
    so it may not be suitable as a patch to apply.
    In particular, geometry WKT is abbreviated and null values are represented
    by a unicode "␀" character.
    """
    fp = resolve_output_path(output_path)
    pecho = {'file': fp, 'color': fp.isatty()}
    if isinstance(output_path, Path) and output_path.is_dir():
        raise click.BadParameter(
            "Directory is not valid for --output with --text", param_hint="--output"
        )

    def _out(dataset, diff):
        path = dataset.path

        prefix = f"{path}:meta:"
        for key, delta in sorted(diff.get('meta', {}).items()):
            if delta.old:
                click.secho(f"--- {prefix}{delta.old.key}", bold=True, **pecho)
            if delta.new:
                click.secho(f"+++ {prefix}{delta.new.key}", bold=True, **pecho)
            if delta.old:
                click.secho(prefix_json(delta.old.value, "- "), fg="red", **pecho)
            if delta.new:
                click.secho(prefix_json(delta.new.value, "+ "), fg="green", **pecho)

        pk_field = dataset.primary_key
        repr_excl = [pk_field]
        prefix = f"{path}:feature:"
        for key, delta in sorted(diff.get('feature', {}).items()):
            if delta.type == "insert":
                click.secho(f"+++ {prefix}{delta.new.key}", bold=True, **pecho)
                click.secho(
                    text_row(delta.new.value, prefix="+ ", exclude=repr_excl),
                    fg="green",
                    **pecho,
                )

            elif delta.type == "delete":
                click.secho(f"--- {prefix}{delta.old.key}", bold=True, **pecho)
                click.secho(
                    text_row(delta.old.value, prefix="- ", exclude=repr_excl),
                    fg="red",
                    **pecho,
                )

            elif delta.type == "update":
                click.secho(
                    f"--- {prefix}{delta.old.key}\n+++ {prefix}{delta.new.key}",
                    bold=True,
                    **pecho,
                )

                # This preserves row order:
                all_keys = itertools.chain(
                    delta.old.value.keys(),
                    (k for k in delta.new.value.keys() if k not in delta.old.value),
                )

                for k in all_keys:
                    if k.startswith("__") or k in repr_excl:
                        continue
                    if delta.old.value.get(k, _NULL) == delta.new.value.get(k, _NULL):
                        continue
                    if k in delta.old.value:
                        click.secho(
                            text_row_field(delta.old.value, k, prefix="- "),
                            fg="red",
                            **pecho,
                        )
                    if k in delta.new.value:
                        click.secho(
                            text_row_field(delta.new.value, k, prefix="+ "),
                            fg="green",
                            **pecho,
                        )

    yield _out


def prefix_json(jdict, prefix):
    json_str = json.dumps(jdict, indent=2)
    return re.sub("^", prefix, json_str, flags=re.MULTILINE)


def text_row(row, prefix="", exclude=None):
    result = []
    exclude = exclude or set()
    for key in row.keys():
        if key.startswith("__") or key in exclude:
            continue
        result.append(text_row_field(row, key, prefix))
    return "\n".join(result)


def text_row_field(row, key, prefix):
    val = row[key]

    if isinstance(val, bytes):
        g = gpkg_geom_to_ogr(val)
        geom_typ = g.GetGeometryName()
        if g.IsEmpty():
            val = f"{geom_typ} EMPTY"
        else:
            val = f"{geom_typ}(...)"
        del g

    val = "␀" if val is None else val
    return f"{prefix}{key:>40} = {val}"


@contextlib.contextmanager
def diff_output_geojson(*, output_path, dataset_count, json_style='pretty', **kwargs):
    """
    Contextmanager.

    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    For features already existed but have changed, two features are written to the output:
    one for the 'deleted' version of the feature, and one for the 'added' version.
    This is intended for visualising in a map diff.

    On exit, writes the diff as GeoJSON to the given output file.
    For repos with more than one dataset, the output path must be a directory.
    In that case:
        * any .geojson files already in that directory will be deleted
        * files will be written to `{layer_name}.geojson in the given directory

    If the output file is stdout and isn't piped anywhere,
    the json is prettified before writing.
    """
    if dataset_count > 1:
        # output_path needs to be a directory
        if not output_path:
            raise click.BadParameter(
                "Need to specify a directory via --output for --geojson with >1 dataset",
                param_hint="--output",
            )
        elif output_path == "-" or output_path.is_file():
            raise click.BadParameter(
                "A file is not valid for --output + --geojson with >1 dataset",
                param_hint="--output",
            )

        if not output_path.exists():
            output_path.mkdir()
        else:
            for p in output_path.glob("*.geojson"):
                p.unlink()

    def _out(dataset, diff):
        if not output_path or output_path == '-':
            fp = sys.stdout
        elif isinstance(output_path, io.StringIO):
            fp = output_path
        elif output_path.is_dir():
            fp = (output_path / f"{dataset.name}.geojson").open("w")
        else:
            fp = output_path.open("w")

        pk_field = dataset.primary_key

        for k in diff.get("meta", {}):
            click.secho(
                f"Warning: meta changes aren't included in GeoJSON output: {k}",
                fg="yellow",
                file=sys.stderr,
            )

        fc = {"type": "FeatureCollection", "features": []}

        for key, delta in sorted(diff.get("feature", {}).items()):
            if delta.type == "insert":
                fc["features"].append(geojson_row(delta.new.value, pk_field, "I"))
            elif delta.type == "update":
                fc["features"].append(geojson_row(delta.old.value, pk_field, "U-"))
                fc["features"].append(geojson_row(delta.new.value, pk_field, "U+"))
            elif delta.type == "delete":
                fc["features"].append(geojson_row(delta.old.value, pk_field, "D"))

        dump_json_output(fc, fp, json_style=json_style)

    yield _out


@contextlib.contextmanager
def diff_output_json(
    *,
    output_path,
    dataset_count,
    json_style="pretty",
    dump_function=dump_json_output,
    **kwargs,
):
    """
    Contextmanager.
    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    On exit, writes the diff as JSON to the given output file.
    If the output file is stdout and isn't piped anywhere,
    the json is prettified first.
    """
    if isinstance(output_path, Path):
        if output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --output-format=json",
                param_hint="--output",
            )

    accumulated = {}

    def _out(dataset, ds_diff):
        ds_result = {}
        meta_diff = ds_diff.get("meta")
        if meta_diff:
            ds_result["meta"] = {
                key: meta_delta_as_json(delta)
                for key, delta in sorted(meta_diff.items())
            }
        feature_diff = ds_diff.get("feature")
        if feature_diff:
            ds_result["feature"] = [
                feature_delta_as_json(delta)
                for key, delta in sorted(feature_diff.items())
            ]
        accumulated[dataset.path] = ds_result

    yield _out

    dump_function(
        {"sno.diff/v1+hexwkb": accumulated}, output_path, json_style=json_style
    )


def meta_delta_as_json(delta):
    if delta.type == "insert":
        return {"+": delta.new.value}
    elif delta.type == "delete":
        return {"-": delta.old.value}
    elif delta.type == "update":
        return {"-": delta.old.value, "+": delta.new.value}


def feature_delta_as_json(delta):
    if delta.type == "insert":
        return {"+": json_row(delta.new.value)}
    elif delta.type == "delete":
        return {"-": json_row(delta.old.value)}
    elif delta.type == "update":
        return {"-": json_row(delta.old.value), "+": json_row(delta.new.value)}


@ungenerator(dict)
def json_row(row):
    """
    Turns a row into a dict for serialization as JSON.
    The geometry is serialized as hexWKB.
    """
    for k, v in row.items():
        if isinstance(v, bytes):
            v = gpkg_geom_to_hex_wkb(v)
        yield k, v


def geojson_row(row, pk_field, change=None):
    """
    Turns a row into a dict representing a GeoJSON feature.
    """
    raw_id = row[pk_field]
    change_id = f"{change}::{raw_id}" if change else raw_id
    f = {
        "type": "Feature",
        "geometry": None,
        "properties": {},
        "id": change_id,
    }

    for k in row.keys():
        v = row[k]
        if isinstance(v, bytes):
            g = gpkg_geom_to_ogr(v)
            f['geometry'] = json.loads(g.ExportToJson())
        else:
            f["properties"][k] = v

    return f


@contextlib.contextmanager
def diff_output_html(*, output_path, repo, base, target, dataset_count, **kwargs):
    """
    Contextmanager.
    Yields a callable which can be called with dataset diffs
    (see `diff_output_text` docstring for more on that)

    On exit, writes an HTML diff to the given output file
    (defaults to 'DIFF.html' in the repo directory).

    If `-` is given as the output file, the HTML is written to stdout,
    and no web browser is opened.
    """
    if isinstance(output_path, Path):
        if output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --html", param_hint="--output"
            )
    with open(
        Path(__file__).resolve().with_name("diff-view.html"), "r", encoding="utf8"
    ) as ft:
        template = string.Template(ft.read())

    title = f"{Path(repo.path).name}: {base.short_id} .. {target.short_id if target else 'working-copy'}"

    with tempfile.TemporaryDirectory() as tempdir:
        tempdir = Path(tempdir)
        # Write a bunch of geojson files to a temporary directory
        with diff_output_geojson(
            output_path=tempdir, dataset_count=dataset_count, json_style="extracompact",
        ) as json_writer:
            yield json_writer

        if not output_path:
            output_path = Path(repo.path) / "DIFF.html"
        fo = resolve_output_path(output_path)

        # Read all the geojson back in, and stick them in a dict
        all_datasets_geojson = {}
        for filename in os.listdir(tempdir):
            with open(tempdir / filename) as json_file:
                all_datasets_geojson[os.path.splitext(filename)[0]] = json.load(
                    json_file
                )
        fo.write(
            template.substitute(
                {"title": title, "geojson_data": json.dumps(all_datasets_geojson)}
            )
        )
    if fo != sys.stdout:
        fo.close()
        webbrowser.open_new(f"file://{output_path.resolve()}")
