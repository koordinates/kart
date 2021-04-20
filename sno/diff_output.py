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

from .exceptions import InvalidOperation
from .geometry import Geometry, gpkg_geom_to_ogr, ogr_to_hex_wkb
from .output_util import dump_json_output, resolve_output_path, format_wkt_for_output
from .schema import Schema
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
        dataset: A sno.base_dataset.BaseDataset instance representing
                 either the old or new version of the dataset.
        diff:    The sno.diff.Diff instance to serialize

    On exit, writes a human-readable diff to the given output file.

    Certain shortcuts are taken to make the diff human readable,
    so it may not be suitable as a patch to apply.
    In particular, geometry WKT is abbreviated and null values are represented
    by a unicode "␀" character.
    """
    fp = resolve_output_path(output_path)
    pecho = {"file": fp, "color": fp.isatty()}
    if isinstance(output_path, Path) and output_path.is_dir():
        raise click.BadParameter(
            "Directory is not valid for --output with --text", param_hint="--output"
        )

    def _out(dataset, diff):
        path = dataset.path

        prefix = f"{path}:meta:"
        for key, delta in sorted(diff.get("meta", {}).items()):
            if delta.old:
                click.secho(f"--- {prefix}{delta.old_key}", bold=True, **pecho)
            if delta.new:
                click.secho(f"+++ {prefix}{delta.new_key}", bold=True, **pecho)

            if key == "schema.json" and delta.old and delta.new:
                # Make a more readable schema diff.
                click.echo(
                    schema_diff_as_text(
                        Schema.from_column_dicts(delta.old_value),
                        Schema.from_column_dicts(delta.new_value),
                    ),
                    **pecho,
                )
                continue

            if delta.old:
                click.secho(
                    prefix_meta_item(delta.old_value, delta.old_key, "- "),
                    fg="red",
                    **pecho,
                )
            if delta.new:
                click.secho(
                    prefix_meta_item(delta.new_value, delta.new_key, "+ "),
                    fg="green",
                    **pecho,
                )

        pk_field = dataset.primary_key
        repr_excl = [pk_field]
        prefix = f"{path}:feature:"
        for key, delta in sorted(diff.get("feature", {}).items()):
            old_pk = delta.old_key
            new_pk = delta.new_key
            old_feature = delta.old_value
            new_feature = delta.new_value

            if delta.type == "insert":
                click.secho(f"+++ {prefix}{new_pk}", bold=True, **pecho)
                click.secho(
                    text_row(new_feature, prefix="+ ", exclude=repr_excl),
                    fg="green",
                    **pecho,
                )

            elif delta.type == "delete":
                click.secho(f"--- {prefix}{old_pk}", bold=True, **pecho)
                click.secho(
                    text_row(old_feature, prefix="- ", exclude=repr_excl),
                    fg="red",
                    **pecho,
                )

            elif delta.type == "update":
                click.secho(
                    f"--- {prefix}{old_pk}\n+++ {prefix}{new_pk}",
                    bold=True,
                    **pecho,
                )

                # This preserves row order:
                all_keys = itertools.chain(
                    old_feature.keys(),
                    (k for k in new_feature.keys() if k not in old_feature),
                )

                for k in all_keys:
                    if k.startswith("__") or k in repr_excl:
                        continue
                    if old_feature.get(k, _NULL) == new_feature.get(k, _NULL):
                        continue
                    if k in old_feature:
                        click.secho(
                            text_row_field(old_feature, k, prefix="- "),
                            fg="red",
                            **pecho,
                        )
                    if k in new_feature:
                        click.secho(
                            text_row_field(new_feature, k, prefix="+ "),
                            fg="green",
                            **pecho,
                        )

    yield _out


def schema_diff_as_text(old_schema, new_schema):
    # Start by pairing column schemas with matching ids from old schema and new schema
    column_schema_pairs = diff_schema(old_schema, new_schema)
    cols_output = []
    for old_column_schema, new_column_schema in column_schema_pairs:
        old_column_dict = old_column_schema.to_dict() if old_column_schema else None
        new_column_dict = new_column_schema.to_dict() if new_column_schema else None
        if new_column_dict is None:
            # Old column schema deleted
            cols_output.append(
                click.style(prefix_json(old_column_dict, "-   ") + ",", fg="red")
            )
            continue
        if old_column_dict is None:
            # New column schema inserted
            cols_output.append(
                click.style(prefix_json(new_column_dict, "+   ") + ",", fg="green")
            )
            continue
        if old_column_dict == new_column_dict:
            # Column schema unchanged
            cols_output.append(prefix_json(new_column_dict, "    ") + ",")
            continue

        # Column schema changed.
        cols_output.append(diff_properties(old_column_dict, new_column_dict))

    cols_output = "\n".join(cols_output)
    return f"  [\n{cols_output}\n  ]"


def diff_properties(old_column, new_column):
    # break column schema into properties and pair them
    output = []
    for old_property, new_property in pair_properties(old_column, new_column):
        if old_property == new_property:
            # Property unchanged
            key = json.dumps(new_property[0])
            value = json.dumps(new_property[1])
            output.append(f"      {key}: {value},")
            continue

        if old_property:
            # Property changed or deleted, print old value
            key = json.dumps(old_property[0])
            value = json.dumps(old_property[1])
            output.append(click.style(f"-     {key}: {value},", fg="red"))

        if new_property:
            # Property changed or inserted, print new value
            key = json.dumps(new_property[0])
            value = json.dumps(new_property[1])
            output.append(click.style(f"+     {key}: {value},", fg="green"))
    output = "\n".join(output)
    return f"    {{\n{output}\n    }},"


def pair_properties(old_column, new_column):
    # This preserves row order
    all_keys = itertools.chain(
        old_column.keys(),
        (k for k in new_column.keys() if k not in old_column.keys()),
    )

    for key in all_keys:
        old_prop = (key, old_column[key]) if key in old_column else None
        new_prop = (key, new_column[key]) if key in new_column else None
        yield old_prop, new_prop


def diff_schema(old_schema, new_schema):
    old_ids = [c.id for c in old_schema]
    new_ids = [c.id for c in new_schema]

    def transform(id_pair):
        old_id, new_id = id_pair
        return (
            old_schema[old_id] if old_id else None,
            new_schema[new_id] if new_id else None,
        )

    return [transform(id_pair) for id_pair in pair_items(old_ids, new_ids)]


def pair_items(old_list, new_list):
    old_index = 0
    new_index = 0
    deleted_set = set(old_list) - set(new_list)
    inserted_set = set(new_list) - set(old_list)
    while old_index < len(old_list) or new_index < len(new_list):
        old_item = old_list[old_index] if old_index < len(old_list) else None
        new_item = new_list[new_index] if new_index < len(new_list) else None
        if old_item and old_item in deleted_set:
            # Old item deleted, or already treated as moved (inserted at another position)
            yield (old_item, None)
            old_index += 1
            continue
        if new_item and new_item in inserted_set:
            # New item inserted, or already treated as moved (deleted from another position)
            yield (None, new_item)
            new_index += 1
            continue
        if old_item == new_item:
            # Items match
            yield (old_item, new_item)
            old_index += 1
            new_index += 1
            continue

        # Items don't match. Decide which item to treat as moved.

        # Get move length if new item treated as moved (inserted here, deleted from another position)
        insert_move_len = 1
        while old_list[old_index + insert_move_len] != new_item:
            insert_move_len += 1

        # Get move length if old item treated as moved (deleted here, inserted at another position)
        remove_move_len = 1
        while new_list[new_index + remove_move_len] != old_item:
            remove_move_len += 1

        # Prefer longer moves, because this should reduce total number of moves.
        if insert_move_len > remove_move_len:
            yield (None, new_item)
            # New item treated as moved (inserted here).
            # So matching item must be treated as deleted when its position is found in old_list
            deleted_set.add(new_item)
            new_index += 1
            continue
        else:
            yield (old_item, None)
            # Old item treated as moved (deleted from here).
            # So matching item must be treated as inserted when its position is found in new_list
            inserted_set.add(old_item)
            old_index += 1


def prefix_meta_item(meta_item, meta_item_name, prefix):
    if meta_item_name.endswith(".wkt"):
        return prefix_wkt(meta_item, prefix)
    elif meta_item_name.endswith(".json"):
        return prefix_json(meta_item, prefix)
    else:
        return re.sub("^", prefix, str(meta_item), flags=re.MULTILINE)


def prefix_wkt(wkt, prefix):
    wkt = format_wkt_for_output(wkt)
    return re.sub("^", prefix, wkt, flags=re.MULTILINE)


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
def diff_output_geojson(
    *,
    output_path,
    dataset_count,
    json_style="pretty",
    dataset_geometry_transforms=None,
    **kwargs,
):
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
        if not output_path or output_path == "-":
            fp = sys.stdout
        elif isinstance(output_path, io.StringIO):
            fp = output_path
        elif output_path.is_dir():
            fp = (output_path / f"{dataset.table_name}.geojson").open("w")
        else:
            fp = output_path.open("w")

        for k in diff.get("meta", {}):
            click.secho(
                f"Warning: meta changes aren't included in GeoJSON output: {k}",
                fg="yellow",
                file=sys.stderr,
            )

        features = []
        geometry_transform = dataset_geometry_transforms.get(dataset.path)
        for key, delta in sorted(diff.get("feature", {}).items()):
            if delta.old:
                change_type = "U-" if delta.new else "D"
                features.append(
                    LazyGeojsonFeatureOutput(
                        change_type, delta.old, geometry_transform=geometry_transform
                    )
                )
            if delta.new:
                change_type = "U+" if delta.old else "I"
                features.append(
                    LazyGeojsonFeatureOutput(
                        change_type, delta.new, geometry_transform=geometry_transform
                    )
                )

        fc = {"type": "FeatureCollection", "features": features}
        dump_json_output(fc, fp, json_style=json_style)

    yield _out


class LazyGeojsonFeatureOutput:
    """Wrapper of KeyValue that lazily serialises it as GEOJSON when sent to json.dumps"""

    __slots__ = ("change_type", "key_value", "geometry_transform")

    def __init__(self, change_type, key_value, geometry_transform=None):
        self.change_type = change_type
        self.key_value = key_value
        self.geometry_transform = geometry_transform

    def __json__(self):
        return geojson_row(
            self.key_value.get_lazy_value(),
            self.key_value.key,
            self.change_type,
            geometry_transform=self.geometry_transform,
        )


def geojson_row(row, pk_value, change=None, geometry_transform=None):
    """
    Turns a row into a dict representing a GeoJSON feature.
    """
    change_id = f"{change}::{pk_value}" if change else str(pk_value)
    f = {
        "type": "Feature",
        "geometry": None,
        "properties": {},
        "id": change_id,
    }

    for k in row.keys():
        v = row[k]
        if isinstance(v, Geometry):
            g = v.to_ogr()
            if geometry_transform is not None:
                # reproject
                try:
                    g.Transform(geometry_transform)
                except RuntimeError as e:
                    raise InvalidOperation(
                        f"Can't reproject geometry at '{change_id}' into target CRS"
                    ) from e
            f["geometry"] = json.loads(g.ExportToJson())
        else:
            f["properties"][k] = v

    return f


@contextlib.contextmanager
def diff_output_json(
    *,
    output_path,
    dataset_count,
    json_style="pretty",
    dump_function=dump_json_output,
    dataset_geometry_transforms=None,
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

    def prepare_meta_delta(delta):
        # No lazy streaming of meta deltas.
        result = {}
        if delta.old:
            result["-"] = delta.old.get_lazy_value()
        if delta.new:
            result["+"] = delta.new.get_lazy_value()
        return result

    def prepare_feature_delta(delta, geometry_transform=None):
        result = {}
        if delta.old:
            result["-"] = LazyJsonFeatureOutput(
                delta.old, geometry_transform=geometry_transform
            )
        if delta.new:
            result["+"] = LazyJsonFeatureOutput(
                delta.new, geometry_transform=geometry_transform
            )
        return result

    repo_result = {}

    def _out(dataset, ds_diff):
        ds_result = {}
        if "meta" in ds_diff:
            ds_result["meta"] = {
                key: prepare_meta_delta(delta)
                for key, delta in sorted(ds_diff["meta"].items())
            }
        if "feature" in ds_diff:
            geometry_transform = dataset_geometry_transforms.get(dataset.path)
            ds_result["feature"] = [
                prepare_feature_delta(delta, geometry_transform=geometry_transform)
                for key, delta in sorted(ds_diff["feature"].items())
            ]
        repo_result[dataset.path] = ds_result

    yield _out

    dump_function(
        {"kart.diff/v1+hexwkb": repo_result}, output_path, json_style=json_style
    )


class LazyJsonFeatureOutput:
    """Wrapper of KeyValue that lazily serialises it as JSON when sent to json.dumps"""

    __slots__ = ("key_value", "geometry_transform")

    def __init__(self, key_value, geometry_transform=None):
        self.key_value = key_value
        self.geometry_transform = geometry_transform

    def __json__(self):
        return json_row(
            self.key_value.get_lazy_value(),
            self.key_value.key,
            geometry_transform=self.geometry_transform,
        )


@ungenerator(dict)
def json_row(row, pk_value, geometry_transform=None):
    """
    Turns a row into a dict for serialization as JSON.
    The geometry is serialized as hexWKB.
    """
    for k, v in row.items():
        if isinstance(v, Geometry):
            if geometry_transform is None:
                v = v.to_hex_wkb()
            else:
                # reproject
                ogr_geom = v.to_ogr()
                try:
                    ogr_geom.Transform(geometry_transform)
                except RuntimeError as e:
                    raise InvalidOperation(
                        f"Can't reproject geometry with ID '{pk_value}' into target CRS"
                    ) from e
                v = ogr_to_hex_wkb(ogr_geom)
        yield k, v


@contextlib.contextmanager
def diff_output_html(
    *,
    output_path,
    repo,
    base,
    target,
    dataset_count,
    dataset_geometry_transforms=None,
    **kwargs,
):
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
            output_path=tempdir,
            dataset_count=dataset_count,
            json_style="extracompact",
            dataset_geometry_transforms=dataset_geometry_transforms,
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
