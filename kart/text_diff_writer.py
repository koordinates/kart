import itertools
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click

from kart.base_diff_writer import BaseDiffWriter
from kart.diff_format import DiffFormat
from kart.diff_structs import BINARY_FILE
from kart.list_of_conflicts import ListOfConflicts
from kart.output_util import format_wkt_for_output, resolve_output_path
from kart.tabular.feature_output import feature_as_text, feature_field_as_text
from kart.schema import Schema

_NULL = object()


class TextDiffWriter(BaseDiffWriter):
    """
    Writes human-readable diffs. Non-empty geometries are not specified in full - instead they look like this:
    POINT(...) or POLYGON(...) - so diffs of this kind are lossy where geometry is involved, and shouldn't be parsed.
    Instead, use a JSON diff if you need to parse it, as `kart create-patch` does.
    When self.commit is set, info about the commit will be output before the diff.
    Any changes to schema.json will be highlighted in a human-readable way, other meta-items diffs will simply show
    the complete old value and the complete new value.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fp = resolve_output_path(self.output_path)
        self.pecho = {"file": self.fp, "color": self.fp.isatty()}

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --text", param_hint="--output"
            )
        return output_path

    def write_header(self):
        if not self.commit:
            return
        author = self.commit.author
        author_time_utc = datetime.fromtimestamp(author.time, timezone.utc)
        author_timezone = timezone(timedelta(minutes=author.offset))
        author_time_in_author_timezone = author_time_utc.astimezone(author_timezone)

        click.secho(f"commit {self.commit.hex}", fg="yellow", **self.pecho)
        click.secho(f"Author: {author.name} <{author.email}>", **self.pecho)
        click.secho(
            f'Date:   {author_time_in_author_timezone.strftime("%c %z")}',
            **self.pecho,
        )
        click.secho(**self.pecho)
        for line in self.commit.message.splitlines():
            click.secho(f"    {line}", **self.pecho)
        click.secho(**self.pecho)

    def write_ds_diff(self, ds_path, ds_diff, diff_format=DiffFormat.FULL):
        if "meta" in ds_diff:
            for key, delta in ds_diff["meta"].sorted_items():
                self.write_meta_delta(ds_path, key, delta)
        if diff_format != DiffFormat.NO_DATA_CHANGES:
            item_type = self._get_old_or_new_dataset(ds_path).ITEM_TYPE
            if item_type:
                for key, delta in self.filtered_dataset_deltas(ds_path, ds_diff):
                    self.write_dict_delta_only_show_diffs(
                        ds_path, item_type, key, delta
                    )

    def write_full_delta(self, ds_path, item_type, key, delta):
        """Writes the old and new halves of a delta in full - ie, not just those parts that have changed."""

        if delta.old:
            click.secho(
                f"--- {ds_path}:{item_type}:{delta.old_key}", bold=True, **self.pecho
            )
        if delta.new:
            click.secho(
                f"+++ {ds_path}:{item_type}:{delta.new_key}", bold=True, **self.pecho
            )

        if delta.old:
            output = self._prefix_item(delta.old_value, delta.old_key, "- ")
            click.secho(output, fg="red", **self.pecho)
        if delta.new:
            output = self._prefix_item(delta.new_value, delta.new_key, "+ ")
            click.secho(output, fg="green", **self.pecho)

    def write_meta_delta(self, ds_path, key, delta):
        if (
            key == "schema.json"
            and delta.old
            and delta.new
            and not isinstance(delta.new_value, ListOfConflicts)
        ):
            # Make a more readable schema diff.
            click.secho(f"--- {ds_path}:meta:schema.json", bold=True, **self.pecho)
            click.secho(f"+++ {ds_path}:meta:schema.json", bold=True, **self.pecho)
            output = self._schema_diff_as_text(
                Schema(delta.old_value),
                Schema(delta.new_value),
            )
            click.echo(output, **self.pecho)
        else:
            self.write_full_delta(ds_path, "meta", key, delta)

    @classmethod
    def _prefix_item(cls, item, item_name, prefix):
        output = cls._format_item(item, item_name)
        return re.sub("^", prefix, output, flags=re.MULTILINE)

    @classmethod
    def _format_item(cls, item, item_name):
        if isinstance(item, ListOfConflicts):
            conflict_output = "\n======== \n".join(
                cls._format_item(i, item_name) for i in item
            )
            return f"<<<<<<< \n{conflict_output}\n>>>>>>> "
        elif item_name.endswith(".wkt"):
            return format_wkt_for_output(item)
        elif isinstance(item, (dict, list)) or item_name.endswith(".json"):
            return json.dumps(item, indent=2)
        else:
            return str(item)

    @classmethod
    def _prefix_json(cls, jdict, prefix):
        json_str = json.dumps(jdict, indent=2)
        return re.sub("^", prefix, json_str, flags=re.MULTILINE)

    def write_dict_delta_only_show_diffs(self, ds_path, item_type, key, delta):
        old_key = delta.old_key
        new_key = delta.new_key
        old_value = delta.old_value
        new_value = delta.new_value

        if delta.type == "insert":
            click.secho(f"+++ {ds_path}:{item_type}:{new_key}", bold=True, **self.pecho)
            output = feature_as_text(new_value, prefix="+ ")

            click.secho(output, fg="green", **self.pecho)
            return

        if delta.type == "delete":
            click.secho(f"--- {ds_path}:{item_type}:{old_key}", bold=True, **self.pecho)
            output = feature_as_text(old_value, prefix="- ")
            click.secho(output, fg="red", **self.pecho)
            return

        # More work to do when delta.type == "update"
        click.secho(
            f"--- {ds_path}:{item_type}:{old_key}\n+++ {ds_path}:{item_type}:{new_key}",
            bold=True,
            **self.pecho,
        )

        for k in self._all_dict_keys(old_value, new_value):
            if k.startswith("__"):
                continue
            if old_value.get(k, _NULL) == new_value.get(k, _NULL):
                continue
            if k in old_value:
                output = feature_field_as_text(old_value, k, prefix="- ")
                click.secho(output, fg="red", **self.pecho)
            if k in new_value:
                output = feature_field_as_text(new_value, k, prefix="+ ")
                click.secho(output, fg="green", **self.pecho)

    # The rest of the class is all just so we can get nice schema diffs. Still, that's important.
    @classmethod
    def _schema_diff_as_text(cls, old_schema, new_schema):
        # Start by pairing column schemas with matching ids from old schema and new schema
        column_schema_pairs = cls._diff_schema(old_schema, new_schema)
        cols_output = []
        for old_column_schema, new_column_schema in column_schema_pairs:
            old_column_dict = old_column_schema.to_dict() if old_column_schema else None
            new_column_dict = new_column_schema.to_dict() if new_column_schema else None
            if new_column_dict is None:
                # Old column schema deleted
                cols_output.append(
                    click.style(
                        cls._prefix_json(old_column_dict, "-   ") + ",", fg="red"
                    )
                )
                continue
            if old_column_dict is None:
                # New column schema inserted
                cols_output.append(
                    click.style(
                        cls._prefix_json(new_column_dict, "+   ") + ",", fg="green"
                    )
                )
                continue
            if old_column_dict == new_column_dict:
                # Column schema unchanged
                cols_output.append(cls._prefix_json(new_column_dict, "    ") + ",")
                continue

            # Column schema changed.
            cols_output.append(cls._diff_properties(old_column_dict, new_column_dict))

        cols_output = "\n".join(cols_output)
        return f"  [\n{cols_output}\n  ]"

    @classmethod
    def _diff_properties(cls, old_column, new_column):
        # break column schema into properties and pair them
        output = []
        for old_property, new_property in cls._pair_properties(old_column, new_column):
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

    @classmethod
    def _pair_properties(cls, old_column, new_column):
        # This preserves row order
        all_keys = itertools.chain(
            old_column.keys(),
            (k for k in new_column.keys() if k not in old_column.keys()),
        )

        for key in all_keys:
            old_prop = (key, old_column[key]) if key in old_column else None
            new_prop = (key, new_column[key]) if key in new_column else None
            yield old_prop, new_prop

    @classmethod
    def _diff_schema(cls, old_schema, new_schema):
        old_ids = [c.best_identifier for c in old_schema]
        new_ids = [c.best_identifier for c in new_schema]

        def transform(id_pair):
            old_id, new_id = id_pair
            return (
                old_schema[old_id] if old_id else None,
                new_schema[new_id] if new_id else None,
            )

        return [transform(id_pair) for id_pair in cls._pair_items(old_ids, new_ids)]

    @classmethod
    def _pair_items(cls, old_list, new_list):
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

    def write_file_diff(self, file_diff):
        for key, delta in file_diff.sorted_items():
            self.write_file_delta(key, delta)
        return bool(file_diff)

    def write_file_delta(self, key, delta):
        if delta.old:
            click.secho(f"--- {key}", bold=True, **self.pecho)
        if delta.new:
            click.secho(f"+++ {key}", bold=True, **self.pecho)

        if self.do_full_file_diffs:
            delta = self._full_file_delta(delta, skip_binary_files=True)
            if not (delta.flags & BINARY_FILE):
                if delta.old:
                    output = self._prefix_item(delta.old_value, delta.old_key, "- ")
                    click.secho(output, fg="red", **self.pecho)
                if delta.new:
                    output = self._prefix_item(delta.new_value, delta.new_key, "+ ")
                    click.secho(output, fg="green", **self.pecho)
                return

        file_type = "binary file" if delta.flags & BINARY_FILE else "file"
        if delta.old:
            click.secho(f"- ({file_type} {delta.old_value})", fg="red", **self.pecho)
        if delta.new:
            click.secho(f"+ ({file_type} {delta.new_value})", fg="green", **self.pecho)
