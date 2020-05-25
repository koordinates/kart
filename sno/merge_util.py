from collections import namedtuple
import json
import re

import pygit2

from .diff_output import text_row, json_row, geojson_row
from .exceptions import InvalidOperation
from .repo_files import (
    ORIG_HEAD,
    MERGE_HEAD,
    MERGE_INDEX,
    MERGE_LABELS,
    is_ongoing_merge,
    read_repo_file,
    write_repo_file,
    repo_file_path,
)
from .structure import RepositoryStructure


# Utility classes relevant to merges - used by merge command, conflicts command, resolve command.


# pygit2 always has this order - we use it too for consistency,
# and so we can meaningfully zip() our tuples with theirs
_ANCESTOR_OURS_THEIRS_ORDER = ("ancestor", "ours", "theirs")


class AncestorOursTheirs(namedtuple("AncestorOursTheirs", _ANCESTOR_OURS_THEIRS_ORDER)):
    """
    When merging two commits, we can end up with three versions of lots of things -
    mostly pygit2 IndexEntrys, but could also be paths, repositories, structures, datasets.
    The 3 versions are the common ancestor, and 2 versions to be merged, "ours" and "theirs".
    Like pygit2, we keep the 3 versions always in the same order - ancestor, ours, theirs.
    """

    NAMES = _ANCESTOR_OURS_THEIRS_ORDER

    @staticmethod
    def partial(*, ancestor=None, ours=None, theirs=None):
        """Supply some or all keyword arguments: ancestor, ours, theirs"""
        return AncestorOursTheirs(ancestor, ours, theirs)

    def __or__(self, other):
        # We don't allow any field to be set twice
        assert not self.ancestor or not other.ancestor
        assert not self.ours or not other.ours
        assert not self.theirs or not other.theirs
        result = AncestorOursTheirs(
            ancestor=self.ancestor or other.ancestor,
            ours=self.ours or other.ours,
            theirs=self.theirs or other.theirs,
        )
        return result

    def map(self, fn, skip_nones=True):
        actual_fn = fn
        if skip_nones:
            actual_fn = lambda x: fn(x) if x else None
        return AncestorOursTheirs(*map(actual_fn, self))

    def as_dict(self):
        return dict(zip(self.NAMES, self))


AncestorOursTheirs.EMPTY = AncestorOursTheirs(None, None, None)


class MergeIndex:
    """
    Like a pygit2.Index, but every conflict has a short key independent of its path,
    and the entire index including conflicts can be serialised to a tree.
    Conflicts are easier to modify than in a pygit2.Index (where they are backed by C iterators).
    When serialised to a tree, conflicts will be added in a special .conflicts/ directory.
    """

    # We could use pygit2.IndexEntry everywhere but it has unhelpful __eq__ and __repr__ behaviour.
    # So we have this equivalent struct.
    Entry = namedtuple("Entry", ("path", "id", "mode"))

    # Note that MergeIndex only contains Entries, which are simple structs -
    # not RichConflicts, which refer to the entire RepositoryStructure to give extra functionality.

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
        if not isinstance(other, MergeIndex):
            return False
        return self.entries == other.entries and self.conflicts == other.conflicts

    def __repr__(self):
        contents = json.dumps(
            {"entries": self.entries, "conflicts": self.conflicts},
            default=lambda o: str(o),
            indent=2,
        )
        return f'<MergeIndex {contents}>'

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
        return MergeIndex(index, serialised=True)

    @classmethod
    def read_from_repo(cls, repo):
        return cls.read(repo_file_path(repo, MERGE_INDEX))

    def write(self, path):
        index = pygit2.Index(str(path))
        index.clear()

        for e in self.entries.values():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        for e in self.conflicts_as_entries():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        index.write()

    def write_to_repo(self, repo):
        self.write(repo_file_path(repo, MERGE_INDEX))

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


class VersionContext:
    """
    The necessary context for categorising or outputting a single version of a conflict.
    Holds the appropriate version of the repository structure,
    the name of that version - one of "ancestor", "ours" or "theirs",
    and a label for that version (the branch name or commit SHA).
    """

    def __init__(self, repo_structure, version_name, version_label):
        self.repo_structure = repo_structure
        self.version_name = version_name
        self.version_label = version_label


class MergeContext:
    """The necessary context for categorising or outputting each conflict in a merge."""

    def __init__(self, versions):
        """An AncestorOursTheirs of VersionContext objects."""
        self.versions = versions

    @classmethod
    def _zip_together(cls, repo_structures3, labels3):
        names3 = AncestorOursTheirs.NAMES
        versions = AncestorOursTheirs(
            *(
                VersionContext(rs, n, l)
                for rs, n, l in zip(repo_structures3, names3, labels3)
            )
        )
        return MergeContext(versions)

    @classmethod
    def from_commit_with_refs(cls, commit_with_refs3, repo):
        repo_structures3 = commit_with_refs3.map(
            lambda c: RepositoryStructure(repo, commit=c.commit)
        )
        labels3 = commit_with_refs3.map(lambda c: str(c))
        return cls._zip_together(repo_structures3, labels3)

    @classmethod
    def read_from_repo(cls, repo):
        if not is_ongoing_merge(repo):
            raise InvalidOperation("Repository is not in 'merging' state")
        ours = RepositoryStructure.lookup(repo, "HEAD")
        theirs = RepositoryStructure.lookup(
            repo, read_repo_file(repo, MERGE_HEAD).strip()
        )
        # We find the ancestor be recalculating it fresh each time. TODO: is that good?
        ancestor_id = repo.merge_base(theirs.id, ours.id)
        ancestor = RepositoryStructure.lookup(repo, ancestor_id)
        repo_structures3 = AncestorOursTheirs(ancestor, ours, theirs)
        labels3 = AncestorOursTheirs(
            *read_repo_file(repo, MERGE_LABELS).strip().split("\n")
        )
        return cls._zip_together(repo_structures3, labels3)

    def write_to_repo(self, repo):
        commits3 = self.versions.map(lambda v: v.repo_structure.head_commit)
        labels3 = self.versions.map(lambda v: v.version_label)
        # We don't write the ancestor, but recalculate it fresh each time.
        write_repo_file(repo, ORIG_HEAD, commits3.ours.id.hex)
        write_repo_file(repo, MERGE_HEAD, commits3.theirs.id.hex)
        write_repo_file(repo, MERGE_LABELS, "".join(f"{l}\n" for l in labels3))


class RichConflictVersion:
    """
    An IndexEntry but with the relevant context attached - mainly the
    repository structure - so that it can be labelled or output.

    Parameters:
        entry - a pygit2.IndexEntry
        context - a VersionContext
    """

    def __init__(self, entry, context):
        self.entry = entry
        self.context = context

    @property
    def path(self):
        return self.entry.path

    @property
    def id(self):
        return self.entry.id

    @property
    def mode(self):
        return self.entry.mode

    @property
    def version_name(self):
        return self.context.version_name

    @property
    def version_label(self):
        return self.context.version_label

    @property
    def repo_structure(self):
        return self.context.repo_structure

    @property
    def decoded_path(self):
        if not hasattr(self, "_decoded_path"):
            self._decoded_path = self.repo_structure.decode_path(self.path)
        return self._decoded_path

    @property
    def table(self):
        return self.decoded_path[0]

    @property
    def dataset(self):
        return self.repo_structure[self.table]

    @property
    def table_part(self):
        return self.decoded_path[1]

    @property
    def is_meta(self):
        return self.table_part == "meta"

    @property
    def pk_field(self):
        assert self.table_part == "feature"
        return self.decoded_path[2]

    @property
    def pk(self):
        assert self.table_part == "feature"
        return self.decoded_path[3]

    @property
    def meta_path(self):
        assert self.table_part == "meta"
        return self.decoded_path[3]

    @property
    def feature(self):
        assert self.table_part == "feature"
        _, feature = self.dataset.get_feature(self.pk, ogr_geoms=False)
        return feature

    @property
    def meta_item(self):
        assert self.table_part == "meta"
        return self.dataset.get_meta_item(self.meta_path)

    @property
    def path_label(self):
        if self.is_meta:
            return f"{self.table}:meta:{self.meta_path}"
        else:
            return f"{self.table}:{self.pk_field}={self.pk}"

    def output(self, output_format):
        """
        Output this version of this feature or meta_item in the
        given output_format - one of "text", "json" or "geojson".
        """
        if self.is_meta:
            result = self.meta_item
            if output_format == "text":
                result = json.dumps(result)
            return result

        if output_format == "text":
            return text_row(self.feature)
        elif output_format == "json":
            return json_row(self.feature, self.pk_field)
        elif output_format == "geojson":
            return geojson_row(self.feature, self.pk_field)


class RichConflict:
    """
    Up to three RichConflictVersions that form a conflict.
    All necessary context of the conflict is collected in this class
    so that the conflict can be categorised, labelled, or output.
    """

    def __init__(self, entry3, merge_context):
        self.versions = AncestorOursTheirs(
            *(
                RichConflictVersion(e, ctx) if e else None
                for e, ctx in zip(entry3, merge_context.versions)
            )
        )

    @property
    def true_versions(self):
        """The versions that are truthy (not None)"""
        return (v for v in self.versions if v)

    @property
    def any_true_version(self):
        """Returns one of the truthy versions, for the case where it doesn't matter which we use."""
        return next(self.true_versions)

    @property
    def has_multiple_paths(self):
        """True if the conflict involves renames and has more than one path."""
        if not hasattr(self, "_has_multiple_paths"):
            paths = set(v.path for v in self.true_versions)
            self._has_multiple_paths = len(paths) > 1
        return self._has_multiple_paths

    @property
    def label(self):
        """
        A unique label for this conflict - eg: "tableA:fid=5" if
        this conflict only involves the feature in tableA in row 5.
        """
        if not hasattr(self, "_label"):
            if self.has_multiple_paths:
                self._label = " ".join(
                    f"{v.version_name}={v.path_label}" for v in self.versions if v
                )
            else:
                self._label = self.any_true_version.path_label
        return self._label

    @property
    def categorised_label(self):
        """
        A unique label for this conflict, but as part of a hierarchy so it
        can be grouped with similar conflicts. Eg:
        ["tableA", "featureConflicts", "edit/edit" "tableA:fid=5"]
        """
        if (
            self.has_multiple_paths
            and len(set(v.table for v in self.true_versions)) > 1
        ):
            return ["<uncategorised>", self.label]
        table = self.any_true_version.table

        if (
            self.has_multiple_paths
            and len(set(v.table_part for v in self.true_versions)) > 1
        ):
            # Meta/feature conflict. Shouldn't really happen.
            return [table, "<uncategorised>", self.label]
        table_part = self.any_true_version.table_part + "Conflicts"

        # <uncategorised> type currently includes everything involving renames.
        conflict_type = "<uncategorised>"
        if not self._has_multiple_paths:
            if not self.versions.ancestor:
                if self.versions.ours and self.versions.theirs:
                    conflict_type = "add/add"
            else:
                if self.versions.ours and self.versions.theirs:
                    conflict_type = "edit/edit"
                elif self.versions.ours or self.versions.theirs:
                    conflict_type = "edit/delete"

        return [table, table_part, conflict_type, self.label]

    def output(self, output_format):
        """Output this conflict in the given output_format - text, json or geojson."""
        return {v.version_name: v.output(output_format) for v in self.true_versions}


def rich_conflicts(raw_conflicts, merge_context):
    """Convert a list of AncestorOursTheirs tuples of Entrys to a list of RichConflicts."""
    return (RichConflict(c, merge_context) for c in raw_conflicts)
