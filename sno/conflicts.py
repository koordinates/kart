from collections import namedtuple
import json
import logging
import re

import click
import pygit2

from .exceptions import NotYetImplemented
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
        index = pygit2.Index(path)
        return ConflictIndex(index, serialised=True)

    def write(self, path):
        index = pygit2.Index(path)
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


def summarise_conflicts_json(repo, conflict_index):
    # Shortcut used often below
    def aot(generator_or_tuple):
        return AncestorOursTheirs(*generator_or_tuple)

    conflicts = {}

    for key, conflict3 in conflict_index.conflicts.items():
        paths3 = aot(c.path if c else None for c in conflict3)
        if not any(paths3):
            # Shouldn't happen
            raise RuntimeError("Conflict has no paths")

        # Paths look like this: # path/to/table/.sno-table/feature_path for features
        # Or path/to/table/.sno-table/meta/... for metadata
        # (This will need updating if newer dataset versions don't follow this pattern.)
        tables3 = aot(p.split("/.sno-table/", 1)[0] if p else None for p in paths3)
        actual_tables = [t for t in tables3 if t]
        all_same_table = all(a == actual_tables[0] for a in actual_tables)
        if not all_same_table:
            # This is a really bad conflict - it seems to involve multiple tables.
            # Perhaps features were moved from one table to another, or perhaps
            # a table was renamed.
            conflicts.setdefault("<other>", 0)
            conflicts["<other>"] += 1
            continue

        table = actual_tables[0]
        conflicts.setdefault(table, {})
        conflicts_table = conflicts[table]

        meta_change = any("/.sno-table/meta/" in (p or "") for p in paths3)
        if meta_change:
            conflicts_table.setdefault("metaConflicts", 0)
            conflicts_table["metaConflicts"] += 1
            continue

        conflicts_table.setdefault("featureConflicts", {})
        feature_conflicts = conflicts_table["featureConflicts"]

        all_same_path = all((p == paths3[0] for p in paths3))
        if all_same_path:
            feature_conflicts.setdefault("edit/edit", 0)
            feature_conflicts["edit/edit"] += 1
            continue

        feature_conflicts.setdefault("other", 0)
        feature_conflicts["other"] += 1

    return conflicts


def move_repo_to_merging_state(repo, conflict_index, ancestor, ours, theirs):
    raise NotYetImplemented(
        "Sorry, putting the repository into a merging state is not yet supported"
    )


def output_json_conflicts_as_text(jdict):
    for table, table_conflicts in sorted(jdict.items()):
        if table == "<other>":
            continue
        click.secho(f"{table}:", bold=True)
        meta_conflicts = table_conflicts.get("metaConflicts", 0)
        if meta_conflicts:
            click.echo(f"  META conflicts: {meta_conflicts}")
        feature_conflicts = table_conflicts.get("featureConflicts", {})
        if feature_conflicts:
            click.echo("  Feature conflicts:")
            for k, v in sorted(feature_conflicts.items()):
                click.echo(f"    {k}: {v}")
        click.echo()

    non_table_conflicts = jdict.get("<other>", 0)
    if non_table_conflicts:
        click.secho(f"Other conflicts: {non_table_conflicts}", bold=True)
