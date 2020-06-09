import logging
import sys

import click

from .exceptions import SUCCESS, SUCCESS_WITH_FLAG
from .merge_util import MergeIndex, MergeContext, rich_conflicts
from .output_util import dump_json_output
from .repo_files import RepoState


L = logging.getLogger("sno.conflicts")


# Stand in for a conflict if the conflict is going to be summarised anyway -
# this helps code re-use between summary and full-diff output modes.
_CONFLICT_PLACEHOLDER = object()


def list_conflicts(
    merge_index, merge_context, output_format="text", summarise=0, flat=False,
):
    """
        Lists all the conflicts in merge_index, categorised into nested dicts.
        Example:
        {
            "table_A": {
                "featureConflicts":
                    "edit/edit": {
                        "table_A:fid=5": {"ancestor": "...", "ours": ..., "theirs": ...},
                        "table_A:fid=11": {"ancestor": "...", "ours": ..., "theirs": ...},
                    },
                    "add/add": {...}
                },
                "metaConflicts": {
                    "edit/edit": {
                        "table_A:meta:gpkg_spatial_ref_sys": {"ancestor": ..., "ours": ..., "theirs": ...}
                    }
                }
            },
            "table_B": {...}
        }
        Depending on the output_format, the conflicts themselves could be summarised as counts
        or as lists of names, eg ["table_1:fid=5", "table_1:fid=11"]

        merge_index - MergeIndex object containing the conflicts found.
        merge_context - MergeContext object containing RepositoryStructures.
        output_format - one of 'text', 'json', 'geojson'
        summarise - 1 means summarise (names only), 2 means *really* summarise (counts only).
        flat - if True, don't categorise conflicts. Put them all at the top level.
    """
    conflicts = {}
    conflict_output = _CONFLICT_PLACEHOLDER

    for conflict in rich_conflicts(
        merge_index.unresolved_conflicts.values(), merge_context
    ):
        if not summarise:
            conflict_output = conflict.output(output_format)

        if flat:
            conflicts[conflict.label] = conflict_output
        else:
            add_conflict_at_label(
                conflicts, conflict.categorised_label, conflict_output
            )

    if summarise:
        conflicts = summarise_conflicts(conflicts, summarise)

    if output_format == "text":
        return conflicts_json_as_text(conflicts)
    else:
        return conflicts


def add_conflict_at_label(root_dict, categorised_label, conflict):
    """
    Ensures the given category of conflicts exists, and then adds
    the given conflict dict to it.
    """
    cur_dict = root_dict
    for c in categorised_label[:-1]:
        cur_dict.setdefault(c, {})
        cur_dict = cur_dict[c]

    leaf = categorised_label[-1]
    cur_dict[leaf] = conflict


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
            return sorted(cur_dict.keys(), key=_label_sort_key)
        elif summarise == 2:
            return len(cur_dict)

    for k, v in cur_dict.items():
        cur_dict[k] = summarise_conflicts(v, summarise)
    return cur_dict


def _label_sort_key(label):
    """Sort labels of conflicts in a sensible way."""
    if label.startswith(("ancestor=", "ours=", "theirs=")):
        # Put the complicated conflicts last.
        return "Z multiple-path", label

    parts = label.split('=', 1)
    if len(parts) == 2:
        prefix, pk = parts
        pk = int(pk) if pk.isdigit() else pk
        return "B feature", prefix, pk
    else:
        return "A meta", label


_JSON_KEYS_TO_TEXT_HEADERS = {
    "featureConflicts": "Feature conflicts",
    "metaConflicts": "META conflicts",
}


def conflicts_json_as_text(json_obj):
    """
    Converts the JSON output of list_conflicts to a string.
    The conflicts themselves should already be in the appropriate format -
    this function deals with the hierarchy that contains them.
    """

    def key_to_text(key, level):
        indent = "  " * level
        heading = _JSON_KEYS_TO_TEXT_HEADERS.get(key, key)
        styled_heading = f"{indent}{heading}:"
        if level == 0:
            styled_heading = click.style(styled_heading, bold=True)
        return styled_heading

    def value_to_text(value, level):
        if isinstance(value, (str, int)):
            return f"{value}\n"
        elif isinstance(value, dict):
            separator = "\n" if level == 0 else ""
            return separator.join(
                item_to_text(k, v, level) for k, v in sorted(value.items())
            )
        elif isinstance(value, list):
            indent = "  " * level
            return "".join(f"{indent}{item}\n" for item in value)

    def item_to_text(key, value, level):
        key_text = key_to_text(key, level)
        if isinstance(value, int):
            return f"{key_text} {value}\n"
        else:
            return f"{key_text}\n{value_to_text(value, level + 1)}"

    return value_to_text(json_obj, 0)


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
    '-s',
    '--summarise',
    '--summarize',
    count=True,
    help="Summarise the conflicts rather than output each one in full. Use -ss for short summary.",
)
@click.option(
    '--flat',
    is_flag=True,
    help="Output all conflicts in a flat list, instead of in a hierarchy.",
)
def conflicts(ctx, output_format, exit_code, json_style, summarise, flat):
    """ Lists merge conflicts that need to be resolved before the ongoing merge can be completed. """

    repo = ctx.obj.get_repo(allowed_states=[RepoState.MERGING])
    merge_index = MergeIndex.read_from_repo(repo)

    if output_format == "quiet":
        ctx.exit(SUCCESS_WITH_FLAG if merge_index.conflicts else SUCCESS)

    merge_context = MergeContext.read_from_repo(repo)
    result = list_conflicts(merge_index, merge_context, output_format, summarise, flat)

    if output_format == "text":
        click.echo(result)
    else:
        dump_json_output({"sno.conflicts/v1": result}, sys.stdout, json_style)

    if exit_code:
        ctx.exit(SUCCESS_WITH_FLAG if merge_context.conflicts else SUCCESS)
