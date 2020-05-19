from collections import namedtuple
import json
import logging
import re

import click
import pygit2

from .diff_output import text_row, json_row, geojson_row
from .exceptions import InvalidOperation
from .repo_files import (
    ORIG_HEAD,
    MERGE_HEAD,
    MERGE_MSG,
    MERGE_INDEX,
    MERGE_LABELS,
    repo_file_path,
    write_repo_file,
    remove_repo_file,
    is_ongoing_merge,
    repo_file_exists,
)
from .structs import AncestorOursTheirs


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


class ConflictOutputFormat:
    """Different ways of showing all the conflicts resulting from a merge."""

    # Only SHORT_SUMMARY is used so far, by `sno merge`.

    # Summaries:
    SHORT_SUMMARY = 0  # Show counts of types of conflicts.
    SUMMARY = 1  # List all the features that conflicted.

    # Full diffs: Show all versions of all the features that conflicted, in...
    FULL_TEXT_DIFF = 2  # ... text format.
    FULL_JSON_DIFF = 3  # ... JSON format.
    FULL_GEOJSON_DIFF = 4  # ... GEOJSON format.

    _SUMMARY_FORMATS = (SHORT_SUMMARY, SUMMARY)


# Stand in for a conflict if the conflict is going to be summarised anyway -
# this helps code re-use between summary and full-diff output modes.
_CONFLICT_PLACEHOLDER = object()


def list_conflicts(
    conflict_index,
    repo_structure3,
    output_format=ConflictOutputFormat.SHORT_SUMMARY,
    flat=False,
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
        output_format - one of the constants in ConflictOutputFormat.
        flat - if True, don't categorise conflicts. Put them all at the top level.
    """
    conflicts = {}

    for key, conflict3 in conflict_index.conflicts.items():
        decoded_path3 = decode_conflict_paths(conflict3, repo_structure3)
        conflict_output = conflict_repr(decoded_path3, repo_structure3, output_format)

        if flat:
            label = get_conflict_label(decoded_path3)
            conflicts[label] = conflict_output
        else:
            label = get_categorised_conflict_label(decoded_path3)
            add_conflict_at_label(conflicts, label, conflict_output)

    if output_format in ConflictOutputFormat._SUMMARY_FORMATS:
        conflicts = summarise_conflicts(conflicts, output_format)

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
    If the output format is a summary format, just returns a placeholder.
    """

    # For a summary, we don't actually need to fill in the conflict -
    # this is removed later anyway in summarise_conflicts.
    if output_format in ConflictOutputFormat._SUMMARY_FORMATS:
        return _CONFLICT_PLACEHOLDER

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
    assert output_format not in ConflictOutputFormat._SUMMARY_FORMATS
    if decoded_path[1] == "feature":
        table, tablepart, pk_field, pk = decoded_path
        _, feature = repo_structure[table].get_feature(pk, ogr_geoms=False)
        if output_format == ConflictOutputFormat.FULL_TEXT_DIFF:
            return text_row(feature)
        elif output_format == ConflictOutputFormat.FULL_JSON_DIFF:
            return json_row(feature, pk_field)
        elif output_format == ConflictOutputFormat.FULL_GEOJSON_DIFF:
            return geojson_row(feature, pk_field)
    else:
        table, tablepart, meta_path = decoded_path
        jdict = repo_structure[table].get_meta_item(meta_path)
        if output_format == ConflictOutputFormat.FULL_TEXT_DIFF:
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


def summarise_conflicts(cur_dict, output_format):
    """
    Recursively traverses the tree of categorised conflicts,
    looking for a dict where the values are placeholders.
    For example:
    {
        K1: _CONFLICT_PLACEHOLDER,
        K2: _CONFLICT_PLACEHOLDER,
    }
    When found, it will be replaced with one of the following:
    1) SHORT_SUMMARY: 2 (the size of the dict)
    2) SUMMARY: [K1, K2]
    """
    first_value = next(iter(cur_dict.values()))
    if first_value == _CONFLICT_PLACEHOLDER:
        if output_format == ConflictOutputFormat.SHORT_SUMMARY:
            return len(cur_dict)
        else:
            return sorted(cur_dict.keys(), key=_label_sort_key)

    for k, v in cur_dict.items():
        cur_dict[k] = summarise_conflicts(v, output_format)
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


def move_repo_to_merging_state(
    repo, conflict_index, merge_message, *, ancestor, ours, theirs
):
    """
    Move the sno repository into a "merging" state in which conflicts
    can be resolved one by one.
    repo - the pygit2.Repository.
    conflict_index - the ConflictIndex containing the conflicts found.
    merge_message - the commit message for when the merge is completed.
    ancestor, ours, theirs - CommitWithReference objects.
    """
    if is_ongoing_merge(repo):
        raise InvalidOperation("A merge is already ongoing")

    # These are git standard files
    write_repo_file(repo, ORIG_HEAD, ours.id.hex)
    write_repo_file(repo, MERGE_HEAD, theirs.id.hex)
    write_repo_file(repo, MERGE_MSG, merge_message)

    # These are specific to sno repositories
    conflict_index.write(repo_file_path(repo, MERGE_INDEX))
    write_repo_file(
        repo, MERGE_LABELS, "\n".join([str(ancestor), str(ours), str(theirs)])
    )


def abort_merging_state(repo):
    """
    Put things back how they were before the merge began.
    Tries to be robust against failure, in case the user has messed up the repo's state.
    """
    is_ongoing_merge = repo_file_exists(repo, MERGE_HEAD)
    # If we are in a merge, we now need to delete all the MERGE_* files.
    # If we are not in a merge, we should clean them up anyway.
    remove_repo_file(repo, MERGE_HEAD)
    remove_repo_file(repo, MERGE_MSG)
    remove_repo_file(repo, MERGE_INDEX)
    remove_repo_file(repo, MERGE_LABELS)

    if not is_ongoing_merge:
        raise InvalidOperation("Repository is not in `merging` state.")

    # TODO - maybe restore HEAD to ORIG_HEAD.
    # Not sure if it matters - we don't modify HEAD when we move into merging state.


_JSON_KEYS_TO_TEXT_HEADERS = {
    "featureConflicts": "Feature conflicts",
    "metaConflicts": "META conflicts",
}


def output_conflicts_as_text(jdict, level=0):
    """Writes the JSON output of list_conflicts to stdout as text, using click.echo."""
    top_level = level == 0
    indent = "  " * level

    for k, v in sorted(jdict.items()):
        heading = _JSON_KEYS_TO_TEXT_HEADERS.get(k, k)
        if isinstance(v, dict):
            click.secho(f"{indent}{heading}:", bold=top_level)
            output_conflicts_as_text(v, level + 1)
            if top_level:
                click.echo()
        elif isinstance(v, list):
            click.secho(f"{indent}{heading}:", bold=top_level)
            for item in v:
                click.echo(f"{indent}  {item}")
            if top_level:
                click.echo()
        elif isinstance(v, str):
            click.echo(f"{indent}{heading}:")
            click.echo(v)
        else:
            click.echo(f"{indent}{heading}: {v}")
