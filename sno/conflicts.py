import logging
import sys

import click

from .crs_util import CoordinateReferenceString
from .exceptions import SUCCESS, SUCCESS_WITH_FLAG
from .filter_util import build_feature_filter, UNFILTERED
from .merge_util import MergeIndex, MergeContext, rich_conflicts
from .output_util import dump_json_output
from .repo import SnoRepoState


L = logging.getLogger("sno.conflicts")


# Stand in for a conflict if the conflict is going to be summarised anyway -
# this helps code re-use between summary and full-diff output modes.
_CONFLICT_PLACEHOLDER = object()


def list_conflicts(
    merge_index,
    merge_context,
    output_format="text",
    conflict_filter=UNFILTERED,
    summarise=0,
    flat=False,
    target_crs=None,
):
    """
    Lists all the conflicts in merge_index, categorised into nested dicts.
    Example:
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

    merge_index - MergeIndex object containing the conflicts found.
    merge_context - MergeContext object containing RepoStructures.
    output_format - one of 'text', 'json', 'geojson'
    summarise - 1 means summarise (names only), 2 means *really* summarise (counts only).
    categorise - if True, adds another layer between feature and ID which is the type of conflict, eg "edit/edit"
    flat - if True, put all features at the top level, with the entire path as the key eg:
        {"dataset_A:feature:5:ancestor": ..., "dataset_A:feature:5:ours": ...}
    """
    output_dict = {}
    conflict_output = _CONFLICT_PLACEHOLDER
    conflict_filter = conflict_filter or UNFILTERED

    if output_format == "geojson":
        flat = True  # geojson must be flat or it is not valid geojson
        summarise = 0

    conflicts = rich_conflicts(merge_index.unresolved_conflicts.values(), merge_context)
    if conflict_filter != UNFILTERED:
        conflicts = (c for c in conflicts if c.matches_filter(conflict_filter))

    for conflict in conflicts:
        if not summarise:
            conflict_output = conflict.output(
                output_format, include_label=flat, target_crs=target_crs
            )

        if flat:
            if isinstance(conflict_output, dict):
                output_dict.update(conflict_output)
            else:
                output_dict[conflict.label] = conflict_output
        else:
            set_value_at_dict_path(output_dict, conflict.decoded_path, conflict_output)

    if summarise:
        output_dict = summarise_conflicts(output_dict, summarise)

    if output_format == "text":
        return conflicts_json_as_text(output_dict)
    elif output_format == "geojson":
        return conflicts_json_as_geojson(output_dict)
    else:
        return output_dict


def set_value_at_dict_path(root_dict, path, value):
    """
    Ensures the given path exists as a nested dict structure in root dict,
    and then places the given value there. For example:
    >>> d = {"x": 1}
    >>> add_value_at_dict_path(d, ("a", "b", "c"), 100)
    >>> d
    {"a": {"b": {"c": 100}}, "x": 1}
    """
    cur_dict = root_dict
    for c in path[:-1]:
        cur_dict.setdefault(c, {})
        cur_dict = cur_dict[c]

    leaf = path[-1]
    cur_dict[leaf] = value


def summarise_conflicts(cur_dict, summarise):
    """
    Recursively traverses the tree of categorised conflicts,
    looking for a dict where the values are placeholders.
    For example:
    {
        K1: _CONFLICT_PLACEHOLDER,
        K2: _CONFLICT_PLACEHOLDER,
    }
    When found, it will be replaced with one of the following,
    depending on the summarise-level specified:
    summarise=1: [K1, K2]
    summarise=2: 2 (the size of the dict)
    """
    first_value = next(iter(cur_dict.values())) if cur_dict else None
    if first_value == _CONFLICT_PLACEHOLDER:
        if summarise == 1:
            return sorted(cur_dict.keys(), key=_path_sort_key)
        elif summarise >= 2:
            return len(cur_dict)

    for k, v in cur_dict.items():
        cur_dict[k] = summarise_conflicts(v, summarise)
    return cur_dict


def _path_sort_key(path):
    """Sort conflicts in a sensible way."""
    if isinstance(path, str) and ":" in path:
        return tuple(_path_part_sort_key(p) for p in path.split(":"))
    else:
        return _path_part_sort_key(path)


def _path_part_sort_key(path_part):
    # Treat stringified numbers as numbers
    if isinstance(path_part, str) and path_part.isdigit():
        path_part = int(path_part)

    # Put meta before features:
    if path_part == "meta":
        return "A", path_part
    elif path_part == "feature":
        return "B", path_part

    # Put complicated conflicts last:
    if isinstance(path_part, str) and "," in path_part:
        return "Z", path_part

    if isinstance(path_part, int):
        return "N", "", path_part
    else:
        return "N", path_part


def conflicts_json_as_text(json_obj):
    """
    Converts the JSON output of list_conflicts to a string.
    The conflicts themselves should already be in the appropriate format -
    this function deals with the hierarchy that contains them.
    """

    def style_key_text(key_text, level):
        indent = "    " * level
        style = {}
        if key_text.endswith(":ancestor:"):
            style["fg"] = "red"
        elif key_text.endswith(":ours:"):
            style["fg"] = "green"
        elif key_text.endswith(":theirs:"):
            style["fg"] = "cyan"
        return click.style(indent + key_text, **style)

    def value_to_text(value, path, level):
        if isinstance(value, str):
            return f"{value}\n"
        elif isinstance(value, int):
            return f"{value} conflicts\n"
        elif isinstance(value, dict):
            separator = "\n" if level == 0 else ""
            return separator.join(
                item_to_text(k, v, path, level) for k, v in sorted(value.items())
            )
        elif isinstance(value, list):
            indent = "    " * level
            return "".join(f"{indent}{path}{item}\n" for item in value)

    def item_to_text(key, value, path, level):
        key_text = f"{path}{key}:"

        styled_key_text = style_key_text(key_text, level)
        value_text = value_to_text(value, key_text, level + 1)

        if isinstance(value, int):
            return f"{styled_key_text} {value_text}"
        else:
            return f"{styled_key_text}\n{value_text}"

    return value_to_text(json_obj, "", 0)


def conflicts_json_as_geojson(json_obj):
    """Converts the JSON output of list_conflicts to geojson."""
    features = []
    for key, feature in json_obj.items():
        feature["id"] = key
        features.append(feature)
    return {"type": "FeatureCollection", "features": features}


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json", "geojson", "quiet"]),
    default="text",
    help="Output format. 'quiet' disables all output and implies --exit-code.",
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with 1 if there are conflicts and 0 means no conflicts.",
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with `-o json` and `-o geojson`",
)
@click.option(
    "--exit-code",
    is_flag=True,
    help="Make the program exit with 1 if there are conflicts and 0 means no conflicts.",
)
@click.option(
    "-s",
    "--summarise",
    "--summarize",
    count=True,
    help="Summarise the conflicts rather than output each one in full. Use -ss for short summary.",
)
@click.option(
    "--flat",
    is_flag=True,
    hidden=True,
    help="Output all conflicts in a flat list, instead of in a hierarchy.",
)
@click.option(
    "--crs",
    type=CoordinateReferenceString(encoding="utf-8"),
    help="Reproject geometries into the given coordinate reference system. Accepts: 'EPSG:<code>'; proj text; OGC WKT; OGC URN; PROJJSON.)",
)
@click.argument("filters", nargs=-1)
def conflicts(
    ctx,
    output_format,
    exit_code,
    json_style,
    summarise,
    flat,
    crs,
    filters,
):
    """
    Lists merge conflicts that need to be resolved before the ongoing merge can be completed.

    To list only particular conflicts, supply one or more FILTERS of the form [DATASET[:PRIMARY_KEY]]
    """

    repo = ctx.obj.get_repo(allowed_states=SnoRepoState.MERGING)
    merge_index = MergeIndex.read_from_repo(repo)

    if output_format == "quiet":
        ctx.exit(SUCCESS_WITH_FLAG if merge_index.conflicts else SUCCESS)

    merge_context = MergeContext.read_from_repo(repo)
    conflict_filter = build_feature_filter(filters)
    result = list_conflicts(
        merge_index,
        merge_context,
        output_format,
        conflict_filter,
        summarise,
        flat,
        crs,
    )

    if output_format == "text":
        click.echo(result)
    elif output_format == "json":
        dump_json_output({"kart.conflicts/v1": result}, sys.stdout, json_style)
    elif output_format == "geojson":
        dump_json_output(result, sys.stdout, json_style)

    if exit_code:
        ctx.exit(SUCCESS_WITH_FLAG if merge_context.conflicts else SUCCESS)
