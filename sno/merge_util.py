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
from .utils import ungenerator


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


_MERGED_OURS_THEIRS_ORDER = ("merged", "ours", "theirs")


class MergedOursTheirs(namedtuple("MergedOursTheirs", _MERGED_OURS_THEIRS_ORDER)):
    """
    When resolving a conflict, there can also be multiple versions. Possible versions are
    the "merged" version - the version that contains both "our" changes and "their" changes -
    but also a resolved "ours" and "theirs" version, in the case that both versions should
    be preserved in some form to complete the merge.
    Having all 3 versions is generally not required, but is allowed by this class.
    """

    NAMES = _MERGED_OURS_THEIRS_ORDER

    @staticmethod
    def partial(*, merged=None, ours=None, theirs=None):
        """Supply some or all keyword arguments: merged, ours, theirs"""
        return MergedOursTheirs(merged, ours, theirs)

    def __or__(self, other):
        # We don't allow any field to be set twice
        assert not self.merged or not other.merged
        assert not self.ours or not other.ours
        assert not self.theirs or not other.theirs
        result = MergedOursTheirs(
            merged=self.merged or other.merged,
            ours=self.ours or other.ours,
            theirs=self.theirs or other.theirs,
        )
        return result

    def map(self, fn, skip_nones=True):
        actual_fn = fn
        if skip_nones:
            actual_fn = lambda x: fn(x) if x else None
        return MergedOursTheirs(*map(actual_fn, self))

    def as_dict(self):
        return dict(zip(self.NAMES, self))


MergedOursTheirs.EMPTY = MergedOursTheirs(None, None, None)


class MergeIndex:
    """
    Like a pygit2.Index, but every conflict has a short key independent of its path,
    and the entire index including conflicts can be serialised to an index file.
    Resolutions to conflicts can also be stored, independently of entries of conflicts.
    Conflicts are easier to modify than in a pygit2.Index (where they are backed by C iterators).
    When serialised to an index file, conflicts will be added in a special .conflicts/ directory,
    and resolutions will be added in a special .resolves/ directory (resolutions are called
    "resolves" here for brevity and with consistency with the verb, ie "sno resolve").
    """

    # We could use pygit2.IndexEntry everywhere but it has unhelpful __eq__ and __repr__ behaviour.
    # So we have this equivalent struct.
    # TODO - fix pygit2.IndexEntry.
    Entry = namedtuple("Entry", ("path", "id", "mode"))

    # Note that MergeIndex only contains Entries, which are simple structs -
    # not RichConflicts, which refer to the entire RepositoryStructure to give extra functionality.

    def __init__(self, entries, conflicts, resolves):
        self.entries = entries
        self.conflicts = conflicts
        self.resolves = resolves

    @classmethod
    def from_pygit2_index(cls, index):
        """
        Converts a pygit2.Index to a MergeIndex, preserving both entries and conflicts.
        Conflicts are assigned arbitrary unique keys based on the iteration order.
        """
        entries = {e.path: cls._ensure_entry(e) for e in index}
        conflicts = {
            str(k): cls._ensure_conflict(c) for k, c in enumerate(index.conflicts)
        }
        resolves = {}
        return MergeIndex(entries, conflicts, resolves)

    def __eq__(self, other):
        if not isinstance(other, MergeIndex):
            return False
        return (
            self.entries == other.entries
            and self.conflicts == other.conflicts
            and self.resolves == other.resolves
        )

    def __repr__(self):
        contents = json.dumps(
            {
                "entries": self.entries,
                "conflicts": self.conflicts,
                "resolves": self.resolves,
            },
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

    @classmethod
    def _serialise_conflict(cls, key, conflict):
        for version, entry in zip(AncestorOursTheirs.NAMES, conflict):
            if not entry:
                continue
            result_path = f".conflicts/{key}/{version}/{entry.path}"
            yield cls.Entry(result_path, entry.id, entry.mode)

    def _serialise_conflicts(self):
        for key, conflict3 in self.conflicts.items():
            yield from self._serialise_conflict(key, conflict3)

    _CONFLICT_PATTERN = re.compile(
        r"^.conflicts/(?P<key>.+?)/(?P<version>ancestor|ours|theirs)/(?P<path>.+)$"
    )

    @classmethod
    def _deserialise_conflict_part(cls, index_entry):
        match = cls._CONFLICT_PATTERN.match(index_entry.path)
        if not match:
            raise RuntimeError(f"Couldn't deserialise conflict: {index_entry.path}")

        key = match.group("key")
        version = match.group("version")
        result_path = match.group("path")
        result_entry = cls.Entry(result_path, index_entry.id, index_entry.mode)
        result = AncestorOursTheirs.partial(**{version: result_entry})
        return key, result

    @ungenerator(set)
    def _conflicts_paths(self):
        """All the paths in all the entries in all the conflicts, as a set."""
        for conflict in self.conflicts.values():
            for entry in conflict:
                if entry:
                    yield entry.path

    def add_resolve(self, key, resolve):
        if not isinstance(key, str):
            raise TypeError("resolve key must be str", type(key))
        self.resolves[key] = self._ensure_resolve(resolve)

    def remove_resolve(self, key):
        del self.resolves[key]

    _EMPTY_OID = pygit2.Oid(hex="0" * 40)
    _EMPTY_MODE = pygit2.GIT_FILEMODE_BLOB

    @classmethod
    def _serialise_resolve(cls, key, resolve):
        if resolve == MergedOursTheirs.EMPTY:
            yield cls.Entry(f".resolves/{key}/deleted", cls._EMPTY_OID, cls._EMPTY_MODE)
            return

        for version, entry in zip(MergedOursTheirs.NAMES, resolve):
            if not entry:
                continue
            result_path = f".resolves/{key}/{version}/{entry.path}"
            yield cls.Entry(result_path, entry.id, entry.mode)

    def _serialise_resolves(self):
        for key, resolve3 in self.resolves.items():
            yield from self._serialise_resolve(key, resolve3)

    _RESOLVE_PATTERN = re.compile(
        r"^.resolves/(?P<key>.+?)/(?P<version>merged|ours|theirs)/(?P<path>.+)$"
    )

    _RESOLVE_DELETE_PATTERN = re.compile(r"^.resolves/(?P<key>.+?)/delete")

    @classmethod
    def _deserialise_resolve_part(cls, index_entry):
        match = cls._RESOLVE_DELETE_PATTERN.match(index_entry.path)
        if match:
            return match.group("key"), None

        match = cls._RESOLVE_PATTERN.match(index_entry.path)
        if not match:
            raise RuntimeError(f"Couldn't deserialise resolve: {index_entry.path}")

        key = match.group("key")
        version = match.group("version")
        result_path = match.group("path")
        result_entry = cls.Entry(result_path, index_entry.id, index_entry.mode)
        result = MergedOursTheirs.partial(**{version: result_entry})
        return key, result

    def _resolves_entries(self):
        """All the entries in all the resolves."""
        for resolve in self.resolves.values():
            for entry in resolve:
                if entry:
                    yield entry

    @property
    def unresolved_conflicts(self):
        return {k: v for k, v in self.conflicts.items() if k not in self.resolves}

    @classmethod
    def read(cls, path):
        """Deserialise a MergeIndex from the given path."""
        index = pygit2.Index(str(path))
        if index.conflicts:
            raise RuntimeError("pygit2.Index conflicts should be empty")
        entries = {}
        conflicts = {}
        resolves = {}
        for e in index:
            if e.path.startswith(".conflicts/"):
                key, conflict_part = cls._deserialise_conflict_part(e)
                conflicts.setdefault(key, AncestorOursTheirs.EMPTY)
                conflicts[key] |= conflict_part
            elif e.path.startswith(".resolves/"):
                key, resolve_part = cls._deserialise_resolve_part(e)
                resolves.setdefault(key, MergedOursTheirs.EMPTY)
                if resolve_part:
                    resolves[key] |= resolve_part
            else:
                entries[e.path] = cls._ensure_entry(e)

        return MergeIndex(entries, conflicts, resolves)

    @classmethod
    def read_from_repo(cls, repo):
        """Deserialise a MergeIndex from the MERGE_INDEX file in the given repo."""
        return cls.read(repo_file_path(repo, MERGE_INDEX))

    def write(self, path):
        """
        Serialise this MergeIndex to the given path.
        Regular entries, conflicts, and resolves are each serialised separately,
        so that they can be roundtripped accurately.
        """
        index = pygit2.Index(str(path))
        index.clear()

        for e in self.entries.values():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        for e in self._serialise_conflicts():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        for e in self._serialise_resolves():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))
        index.write()

    def write_to_repo(self, repo):
        """Serialise this MergeIndex to the MERGE_INDEX file in the given repo."""
        self.write(repo_file_path(repo, MERGE_INDEX))

    def write_resolved_tree(self, repo):
        """
        Write all the merged entries and the resolved conflicts to a tree in the given repo.
        Resolved conflicts will be written the same as merged entries in the resulting tree.
        Only works when all conflicts are resolved.
        """
        assert not self.unresolved_conflicts
        index = pygit2.Index()

        # Entries that were merged automatically by libgit2, often trivially:
        for e in self.entries.values():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))

        # libgit2 leaves entries in the main part of the index, even if they are conflicts.
        # We make sure this index only contains merged entries and resolved conflicts.
        index.remove_all(list(self._conflicts_paths()))

        # Entries that have been explicitly selected to resolve conflicts:
        for e in self._resolves_entries():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))

        return index.write_tree(repo)

    @classmethod
    def _ensure_entry(cls, entry):
        if entry is None or isinstance(entry, cls.Entry):
            return entry
        elif isinstance(entry, pygit2.IndexEntry):
            return cls.Entry(entry.path, entry.id, entry.mode)
        else:
            raise TypeError(
                "Expected entry to be type Entry or IndexEntry", type(entry)
            )

    @classmethod
    def _ensure_conflict(cls, conflict):
        if isinstance(conflict, AncestorOursTheirs):
            return conflict
        elif isinstance(conflict, tuple):
            return AncestorOursTheirs(
                cls._ensure_entry(conflict[0]),
                cls._ensure_entry(conflict[1]),
                cls._ensure_entry(conflict[2]),
            )
        else:
            raise TypeError(
                "Expected conflict to be type AncestorOursTheirs or tuple",
                type(conflict),
            )

    @classmethod
    def _ensure_resolve(cls, resolve):
        if isinstance(resolve, MergedOursTheirs):
            return resolve
        elif isinstance(resolve, tuple):
            return AncestorOursTheirs(
                cls._ensure_entry(resolve[0]),
                cls._ensure_entry(resolve[1]),
                cls._ensure_entry(resolve[2]),
            )
        else:
            raise TypeError(
                "Expected resolve to be type MergedOursTheirs or tuple", type(resolve),
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
        # We don't write commits3.ancestor - we just recalculate it each time.
        # And we don't write commits3.ours - thats already stored in HEAD.
        # So we just write commits3.theirs:
        write_repo_file(repo, MERGE_HEAD, commits3.theirs.id.hex)
        # We also don't write an ORIG_HEAD, since we don't change HEAD during a merge.

        # We write labels for what we are merging to MERGE_LABELS - these include
        # the names of the branches sno merge was given to merge, although these
        # are merely informational since those branch heads could move ahead
        # to new commits before this merge is completed.
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
