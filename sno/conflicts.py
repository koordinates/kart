from collections import namedtuple
import json
import logging
import re
import sys

import click
import pygit2

from .cli_util import MutexOption
from .diff_output import text_row, json_row, geojson_row
from .exceptions import InvalidOperation, SUCCESS, SUCCESS_WITH_FLAG
from .output_util import dump_json_output
from .repo_files import (
    MERGE_HEAD,
    MERGE_INDEX,
    MERGE_LABELS,
    is_ongoing_merge,
    read_repo_file,
    repo_file_path,
)
from .structs import AncestorOursTheirs
from .structure import RepositoryStructure


L = logging.getLogger("sno.conflicts")


class ConflictIndex:
    """
    Like a pygit2.Index, but every conflict has a short key independent of its path,
    and the entire index including conflicts can be serialised to a tree.
    Conflicts are easier to modify than in a pygit2.Index (where they are backed by C iterators).
    When serialised to a tree, conflicts will be added in a special .conflicts/ directory.
    """

    # We could use pygit2.IndexEntry everywhere but it has unhelpful __eq__ and __repr__ behaviour.
    # So we have this equivalent struct.
    Entry = namedtuple("Entry", ("path", "id", "mode"))

    def __init__(self, index, serialised=False):
        self.entries = {}
        self.conflicts = {}

        if serialised and index.conflicts:
            raise RuntimeError(
                "pygit2.Index.conflicts should be empty if index has been serialised"
            )

        for entry in index:
            if entry.path.startswith(".conflicts/"):
                if not serialised:
                    raise RuntimeError(
                        ".conflicts/ directory shouldn't exist if index has not been serialised"
                    )
                key, conflict_part = self._deserialise_conflict_part(entry)
                self.conflicts.setdefault(key, AncestorOursTheirs.EMPTY)
                self.add_conflict(key, self.conflicts.get(key) | conflict_part)
            else:
                self.add(entry)

        if index.conflicts:
            for key, conflict3 in enumerate(index.conflicts):
                self.add_conflict(str(key), conflict3)

    def __eq__(self, other):
        if not isinstance(other, ConflictIndex):
            return False
        return self.entries == other.entries and self.conflicts == other.conflicts

    def __repr__(self):
        contents = json.dumps(
            {"entries": self.entries, "conflicts": self.conflicts},
            default=lambda o: str(o),
            indent=2,
        )
        return f'<ConflictIndex {contents}>'

    def add(self, index_entry):
        index_entry = self._ensure_entry(index_entry)
        self.entries[index_entry.path] = index_entry

    def remove(self, path):
        del self.entries[path]

    def __iter__(self):
        return iter(self.entries.values())

    def __getitem__(self, path):
        return self.entries[path]

    def __setitem__(self, path, index_entry):
        assert path == index_entry.path
        self.entries[path] = index_entry

    def add_conflict(self, key, conflict):
        if not isinstance(key, str):
            raise TypeError("conflict key must be str", type(key))
        self.conflicts[key] = self._ensure_conflict(conflict)

    def remove_conflict(self, key):
        del self.conflicts[key]

    def _serialise_conflict(self, key, conflict):
        result = []
        for version, entry in zip(AncestorOursTheirs.NAMES, conflict):
            if not entry:
                continue
            result_path = f".conflicts/{key}/{version}/{entry.path}"
            result.append(self.Entry(result_path, entry.id, entry.mode))
        return result

    _PATTERN = re.compile(
        r"^.conflicts/(?P<key>.+?)/(?P<version>ancestor|ours|theirs)/(?P<path>.+)$"
    )

    def _deserialise_conflict_part(self, index_entry):
        match = self._PATTERN.match(index_entry.path)
        if not match:
            raise RuntimeError(f"Couldn't deserialise conflict: {index_entry.path}")

        key = match.group("key")
        version = match.group("version")
        result_path = match.group("path")
        result_entry = self.Entry(result_path, index_entry.id, index_entry.mode)
        result = AncestorOursTheirs.partial(**{version: result_entry})
        return key, result

    def conflicts_as_entries(self):
        for key, conflict3 in self.conflicts.items():
            for index_entry in self._serialise_conflict(key, conflict3):
                yield index_entry

    @classmethod
    def read(cls, path):
        index = pygit2.Index(str(path))
        return ConflictIndex(index, serialised=True)

    def write(self, path):
        index = pygit2.Index(str(path))
        index.clear()

        for e in self.entries.values():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        for e in self.conflicts_as_entries():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        return index.write()

    def _ensure_entry(self, entry):
        if entry is None or isinstance(entry, self.Entry):
            return entry
        elif isinstance(entry, pygit2.IndexEntry):
            return self.Entry(entry.path, entry.id, entry.mode)
        else:
            raise TypeError(
                "Expected entry to be type Entry or IndexEntry", type(entry)
            )

    def _ensure_conflict(self, conflict):
        if isinstance(conflict, AncestorOursTheirs):
            return conflict
        elif isinstance(conflict, tuple):
            return AncestorOursTheirs(
                self._ensure_entry(conflict[0]),
                self._ensure_entry(conflict[1]),
                self._ensure_entry(conflict[2]),
            )
        else:
            raise TypeError(
                "Expected conflict to be type AncestorOursTheirs or tuple",
                type(conflict),
            )


# Stand in for a conflict if the conflict is going to be summarised anyway -
# this helps code re-use between summary and full-diff output modes.
_CONFLICT_PLACEHOLDER = object()


def list_conflicts(
    conflict_index, repo_structure3, output_format="text", summarise=0, flat=False,
):
    """
        Lists all the conflicts in conflict_index, categorised into nested dicts.
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

        conflict_index - the ConflictIndex containing the conflicts found.
        repo_structure3 - AncestorOursTheirs tuple containing RepositoryStructures.
        output_format - one of 'text', 'json', 'geojson'
        flat - if True, don't categorise conflicts. Put them all at the top level.
    """
    conflicts = {}
    conflict_output = _CONFLICT_PLACEHOLDER

    for key, conflict3 in conflict_index.conflicts.items():
        decoded_path3 = decode_conflict_paths(conflict3, repo_structure3)
        if not summarise:
            conflict_output = conflict_repr(
                decoded_path3, repo_structure3, output_format
            )

        if flat:
            label = get_conflict_label(decoded_path3)
            conflicts[label] = conflict_output
        else:
            label = get_categorised_conflict_label(decoded_path3)
            add_conflict_at_label(conflicts, label, conflict_output)

    if summarise:
        conflicts = summarise_conflicts(conflicts, summarise)

    if output_format == "text":
        return conflicts_json_as_text(conflicts)
    else:
        return conflicts


def decode_conflict_paths(conflict3, repo_structure3):
    """
    Given 3 versions of an IndexEntry, and 3 versions of a repository_structure,
    return 3 versions of a decoded path - see RepositoryStructure.decode_path.
    """
    return AncestorOursTheirs(
        *(
            rs.decode_path(c.path) if c else None
            for c, rs, in zip(conflict3, repo_structure3)
        )
    )


def get_categorised_conflict_label(decoded_path3):
    """
    Given 3 versions of the decoded path, tries to categorise the conflict,
    so that similar conflicts can be grouped together.
    For example, a returned categorised label might be:
    ["table_A", "featureConflicts", "edit/edit", "table_A:fid=3"]
    Meaning conflicting edits were made to a feature fid=3 in table_A.
    """
    label = get_conflict_label(decoded_path3)

    dpath3 = decoded_path3
    actual_dpaths = [p for p in dpath3 if p]
    actual_tables = [p[0] for p in actual_dpaths]
    all_same_table = len(set(actual_tables)) == 1

    if not all_same_table:
        return ["<uncategorised>", label]
    table = actual_tables[0]

    actual_tableparts = [p[1] for p in actual_dpaths]
    all_same_tablepart = len(set(actual_tableparts)) == 1
    if all_same_tablepart:
        tablepart = actual_tableparts[0] + "Conflicts"
    else:
        # Meta/feature conflict. Shouldn't really happen.
        return [table, "<uncategorised>", label]

    # <uncategorised> type currently includes everything involving renames.
    conflict_type = "<uncategorised>"
    all_same_path = len(set(actual_dpaths)) == 1
    if all_same_path:
        if not dpath3.ancestor:
            if dpath3.ours and dpath3.theirs:
                conflict_type = "add/add"
        else:
            if dpath3.ours and dpath3.theirs:
                conflict_type = "edit/edit"
            elif dpath3.ours or dpath3.theirs:
                conflict_type = "edit/delete"

    return [table, tablepart, conflict_type, label]


def get_conflict_label(decoded_path3):
    """
    Given 3 versions of the decoded path, returns a unique name for a conflict.
    In simply cases, this will be something like: "table_A:fid=5"
    But if renames have occurred, it could have multiple names, eg:
    "ancestor=table_A:fid=5 ours=table_A:fid=6 theirs=table_A:fid=12"
    """
    dpath3 = decoded_path3
    actual_dpaths = [p for p in dpath3 if p]
    all_same_path = len(set(actual_dpaths)) == 1

    if all_same_path:
        return decoded_path_to_label(actual_dpaths[0])

    label3 = AncestorOursTheirs(
        *(
            f"{v}={decoded_path_to_label(p)}" if p else None
            for v, p, in zip(AncestorOursTheirs.NAMES, decoded_path3)
        )
    )
    return " ".join([l for l in label3 if l])


def decoded_path_to_label(decoded_path):
    """
    Converts a decoded path to a unique name, eg:
    ("table_A", "feature", "fid", 5) -> "table_A:fid=5"
    """
    if decoded_path is None:
        return None
    if decoded_path[1] == "feature":
        table, tablepart, pk_field, pk = decoded_path
        return f"{table}:{pk_field}={pk}"
    else:
        return ":".join(decoded_path)


def conflict_repr(decoded_path3, repo_structure3, output_format):
    """
    Returns a dict containing up to 3 versions of the conflict
    (at keys "ancestor", "theirs" and "ours") using conflict_version_repr.
    """
    return AncestorOursTheirs(
        *(
            conflict_version_repr(dp, rs, output_format) if dp else None
            for dp, rs, in zip(decoded_path3, repo_structure3)
        )
    ).as_dict()


def conflict_version_repr(decoded_path, repo_structure, output_format):
    """
    Returns the feature / metadata in repo_structure at decoded_path,
    according to output_format.
    If the decoded_path points to a feature, it will be output as follows:
    - FULL_TEXT_DIFF - as a string using diff_output.text_row.
    - FULL_JSON_DIFF - as JSON using diff_output.json_row
    - FULL_GEOJSON_DIFF - as GEOJSON using diff_output.geojson_row
    If the decoded path points to metadata, it will be output as follows:
    - FULL_JSON_DIFF / FULL_GEOJSON_DIFF - as JSON object
    - FULL_TEXT_DIFF - as stringified JSON object
    """
    if decoded_path[1] == "feature":
        table, tablepart, pk_field, pk = decoded_path
        _, feature = repo_structure[table].get_feature(pk, ogr_geoms=False)
        if output_format == "text":
            return text_row(feature)
        elif output_format == "json":
            return json_row(feature, pk_field)
        elif output_format == "geojson":
            return geojson_row(feature, pk_field)
    else:
        table, tablepart, meta_path = decoded_path
        jdict = repo_structure[table].get_meta_item(meta_path)
        if output_format == "text":
            return json.dumps(jdict)
        else:
            return jdict


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
    first_value = next(iter(cur_dict.values()))
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
    "--text",
    "output_format",
    flag_value="text",
    default=True,
    help="Get the diff in text format",
    cls=MutexOption,
    exclusive_with=["json", "geojson", "quiet"],
)
@click.option(
    "--json",
    "output_format",
    flag_value="json",
    help="Get the diff in JSON format",
    hidden=True,
    cls=MutexOption,
    exclusive_with=["text", "geojson", "quiet"],
)
@click.option(
    "--geojson",
    "output_format",
    flag_value="geojson",
    help="Get the diff in GeoJSON format",
    cls=MutexOption,
    exclusive_with=["text", "json", "quiet"],
)
@click.option(
    "--quiet",
    "output_format",
    flag_value="quiet",
    help="Disable all output of the program. Implies --exit-code.",
    cls=MutexOption,
    exclusive_with=["json", "text", "geojson", "html"],
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
    help="How to format the output. Only used with --json or --geojson",
    cls=MutexOption,
    exclusive_with=["text", "quiet"],
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

    repo = ctx.obj.repo
    if not is_ongoing_merge(repo):
        raise InvalidOperation("Cannot list conflicts - there is no ongoing merge")

    cindex = ConflictIndex.read(repo_file_path(repo, MERGE_INDEX))

    if output_format == "quiet":
        ctx.exit(SUCCESS_WITH_FLAG if cindex.conflicts else SUCCESS)

    ours = RepositoryStructure.lookup(repo, "HEAD")
    theirs = RepositoryStructure.lookup(repo, read_repo_file(repo, MERGE_HEAD).strip())
    ancestor_id = repo.merge_base(theirs.id, ours.id)
    ancestor = RepositoryStructure.lookup(repo, ancestor_id)

    repo_structure3 = AncestorOursTheirs(ancestor, ours, theirs)
    result = list_conflicts(cindex, repo_structure3, output_format, summarise, flat)

    if output_format == "text":
        click.echo(result)
    else:
        dump_json_output({"sno.conflicts/v1": result}, sys.stdout, json_style)

    if exit_code:
        ctx.exit(SUCCESS_WITH_FLAG if cindex.conflicts else SUCCESS)
