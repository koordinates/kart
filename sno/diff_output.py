import contextlib
import io
import json
import string
import sys
import webbrowser
from pathlib import Path

import click
from osgeo import ogr

from . import gpkg
from .output_util import dump_json_output, resolve_output_path


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
        pk_field = dataset.primary_key
        prefix = f"{path}:"
        repr_excl = [pk_field]

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(
                f"--- {prefix}meta/{k}\n+++ {prefix}meta/{k}", bold=True, **pecho
            )

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = set(diff_del.keys()) | set(diff_add.keys())

            for k in all_keys:
                if k in diff_del:
                    click.secho(
                        repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl),
                        fg="red",
                        **pecho,
                    )
                if k in diff_add:
                    click.secho(
                        repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl),
                        fg="green",
                        **pecho,
                    )

        prefix = f"{path}:{pk_field}="

        for k, v_old in diff["D"].items():
            click.secho(f"--- {prefix}{k}", bold=True, **pecho)
            click.secho(
                repr_row(v_old, prefix="- ", exclude=repr_excl), fg="red", **pecho
            )

        for o in diff["I"]:
            click.secho(f"+++ {prefix}{o[pk_field]}", bold=True, **pecho)
            click.secho(
                repr_row(o, prefix="+ ", exclude=repr_excl), fg="green", **pecho
            )

        for _, (v_old, v_new) in diff["U"].items():
            click.secho(
                f"--- {prefix}{v_old[pk_field]}\n+++ {prefix}{v_new[pk_field]}",
                bold=True,
                **pecho,
            )

            s_old = set(v_old.items())
            s_new = set(v_new.items())

            diff_add = dict(s_new - s_old)
            diff_del = dict(s_old - s_new)
            all_keys = sorted(set(diff_del.keys()) | set(diff_add.keys()))

            for k in all_keys:
                if k in diff_del:
                    rk = repr_row({k: diff_del[k]}, prefix="- ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="red", **pecho)
                if k in diff_add:
                    rk = repr_row({k: diff_add[k]}, prefix="+ ", exclude=repr_excl)
                    if rk:
                        click.secho(rk, fg="green", **pecho)

    yield _out


def repr_row(row, prefix="", exclude=None):
    m = []
    exclude = exclude or set()
    for k in sorted(row.keys()):
        if k.startswith("__") or k in exclude:
            continue

        v = row[k]

        if isinstance(v, bytes):
            g = gpkg.gpkg_geom_to_ogr(v)
            geom_typ = g.GetGeometryName()
            if g.IsEmpty():
                v = f"{geom_typ} EMPTY"
            else:
                v = f"{geom_typ}(...)"
            del g

        v = "␀" if v is None else v
        m.append("{prefix}{k:>40} = {v}".format(k=k, v=v, prefix=prefix))

    return "\n".join(m)


@contextlib.contextmanager
def diff_output_geojson(*, output_path, dataset_count, json_style, **kwargs):
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
        elif output_path.is_dir():
            fp = (output_path / f"{dataset.name}.geojson").open("w")
        else:
            fp = output_path.open("w")

        pk_field = dataset.primary_key

        fc = {"type": "FeatureCollection", "features": []}

        for k, (v_old, v_new) in diff["META"].items():
            click.secho(
                f"Warning: meta changes aren't included in GeoJSON output: {k}",
                fg="yellow",
                file=sys.stderr,
            )

        for k, v_old in diff["D"].items():
            fc["features"].append(
                _json_row(v_old, "D", pk_field, geom_encoder=_geom_geojson)
            )

        for o in diff["I"]:
            fc["features"].append(
                _json_row(o, "I", pk_field, geom_encoder=_geom_geojson)
            )

        for _, (v_old, v_new) in diff["U"].items():
            fc["features"].append(
                _json_row(v_old, "U-", pk_field, geom_encoder=_geom_geojson)
            )
            fc["features"].append(
                _json_row(v_new, "U+", pk_field, geom_encoder=_geom_geojson)
            )

        dump_json_output(fc, fp, json_style=json_style)

    yield _out


@contextlib.contextmanager
def diff_output_json(*, output_path, dataset_count, json_style="pretty", **kwargs):
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
                "Directory is not valid for --output with --json", param_hint="--output"
            )

    accumulated = {}

    def _out(dataset, diff):
        pk_field = dataset.primary_key

        d = {"metaChanges": {}, "featureChanges": []}
        for k, (v_old, v_new) in diff["META"].items():
            d["metaChanges"][k] = [v_old, v_new]

        for k, v_old in diff["D"].items():
            d["featureChanges"].append(
                {
                    '-': _json_row(
                        v_old, "D", pk_field, geom_encoder=gpkg.gpkg_geom_to_hex_wkb
                    )
                }
            )

        for o in diff["I"]:
            d["featureChanges"].append(
                {'+': _json_row(o, "I", pk_field, geom_encoder=gpkg.gpkg_geom_to_hex_wkb)}
            )

        for _, (v_old, v_new) in diff["U"].items():
            d["featureChanges"].append(
                {
                    '-': _json_row(
                        v_old, "U-", pk_field, geom_encoder=gpkg.gpkg_geom_to_hex_wkb
                    ),
                    '+': _json_row(
                        v_new, "U+", pk_field, geom_encoder=gpkg.gpkg_geom_to_hex_wkb
                    ),
                }
            )

        # sort for reproducibility
        d["featureChanges"].sort(
            key=lambda fc: (
                fc['-']["id"] if '-' in fc else "",
                fc['+']["id"] if '+' in fc else "",
            )
        )
        accumulated[dataset.path] = d

    yield _out

    dump_json_output(
        {"sno.diff/v1": accumulated}, output_path, json_style=json_style
    )


def _geom_geojson(v):
    g = gpkg.gpkg_geom_to_ogr(v)
    return json.loads(g.ExportToJson())


def _json_row(row, change, pk_field, *, geom_encoder):
    f = {
        "type": "Feature",
        "geometry": None,
        "properties": {},
        "id": f"{change}::{row[pk_field]}",
    }

    for k in row.keys():
        v = row[k]
        if isinstance(v, bytes):
            hex_wkb = geom_encoder(v)
            f["geometry"] = hex_wkb
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

    json_data = io.StringIO()
    with diff_output_json(
        output_path=json_data, dataset_count=dataset_count
    ) as json_writer:
        yield json_writer

    with open(
        Path(__file__).resolve().with_name("diff-view.html"), "r", encoding="utf8"
    ) as ft:
        template = string.Template(ft.read())

    title = f"{Path(repo.path).name}: {base.short_id} .. {target.short_id if target else 'working-copy'}"

    if not output_path:
        output_path = Path(repo.path) / "DIFF.html"
    fo = resolve_output_path(output_path)

    fo.write(
        template.substitute({"title": title, "geojson_data": json_data.getvalue()})
    )
    if fo != sys.stdout:
        fo.close()
        webbrowser.open_new(f"file://{output_path.resolve()}")
