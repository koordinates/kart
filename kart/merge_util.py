import functools
import json
import re
from collections import namedtuple

import click
import pygit2

from .lfs_util import pointer_file_bytes_to_dict, get_local_path_from_lfs_hash
from .key_filters import RepoKeyFilter
from .repo import KartRepoFiles
from .structs import CommitWithReference
from .tabular.feature_output import feature_as_geojson, feature_as_json, feature_as_text
from .utils import ungenerator
from kart.lfs_commands import fetch_lfs_blobs_for_commits
from kart.point_cloud.tilename_util import set_tile_extension
from kart.reflink_util import try_reflink


MERGE_HEAD = KartRepoFiles.MERGE_HEAD
MERGE_BRANCH = KartRepoFiles.MERGE_BRANCH
MERGE_MSG = KartRepoFiles.MERGE_MSG

MERGED_INDEX = KartRepoFiles.MERGED_INDEX
MERGED_TREE = KartRepoFiles.MERGED_TREE

ALL_MERGE_FILES = (MERGE_HEAD, MERGE_BRANCH, MERGE_MSG, MERGED_INDEX, MERGED_TREE)


def write_merged_index_flags(repo):
    flags = pygit2.GIT_INDEX_WRITE_TREE_DEFAULT
    if repo.is_partial_clone:
        # Don't validate the individual blob OIDS for a partial clone since - they might be "promised",
        # which looks the same as missing once they're in the index that the merge results in.
        flags &= ~pygit2.GIT_INDEX_WRITE_TREE_VALIDATE_OIDS

    return flags


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


class MergedIndex:
    """
    Like a pygit2.Index, but every conflict has a short key independent of its path,
    and the entire index including conflicts can be serialised to an index file.
    Resolutions to conflicts can also be stored, independently of entries of conflicts.
    Conflicts are easier to modify than in a pygit2.Index (where they are backed by C iterators).
    When serialised to an index file, conflicts will be added in a special .conflicts/ directory,
    and resolutions will be added in a special .resolves/ directory (resolutions are called
    "resolves" here for brevity and with consistency with the verb, ie "kart resolve").
    """

    # We could use pygit2.IndexEntry everywhere but it has unhelpful __eq__ and __repr__ behaviour.
    # So we have this equivalent struct.
    # TODO - fix pygit2.IndexEntry.
    Entry = namedtuple("Entry", ("path", "id", "mode"))

    # Note that MergedIndex only contains Entries, which are simple structs -
    # not RichConflicts, which refer to the entire RepoStructure to give extra functionality.

    def __init__(self, entries, conflicts, resolves):
        self.entries = entries
        self.conflicts = conflicts
        self.resolves = resolves

    @classmethod
    def from_pygit2_index(cls, index):
        """
        Converts a pygit2.Index to a MergedIndex, preserving both entries and conflicts.
        Conflicts are assigned arbitrary unique keys based on the iteration order.
        """
        entries = {e.path: cls._ensure_entry(e) for e in index}
        conflicts = {
            str(k): cls._ensure_conflict(c) for k, c in enumerate(index.conflicts)
        }
        resolves = {}
        return MergedIndex(entries, conflicts, resolves)

    def __eq__(self, other):
        if not isinstance(other, MergedIndex):
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
        return f"<MergedIndex {contents}>"

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
        r"^.conflicts/(?P<key>[^/]+)/(?P<version>ancestor|ours|theirs)/(?P<path>.+)$"
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
        # We always yield at least one entry per resolve, even when the resolve
        # has no features - otherwise it would appear to be unresolved.
        yield cls.Entry(f".resolves/{key}/resolved", cls._EMPTY_OID, cls._EMPTY_MODE)

        for i, entry in enumerate(resolve):
            result_path = f".resolves/{key}/{i}/{entry.path}"
            yield cls.Entry(result_path, entry.id, entry.mode)

    def _serialise_resolves(self):
        for key, resolve3 in self.resolves.items():
            yield from self._serialise_resolve(key, resolve3)

    _RESOLVED_PATTERN = re.compile(r"^.resolves/(?P<key>.+?)/resolved$")
    _RESOLVE_PART_PATTERN = re.compile(
        r"^.resolves/(?P<key>[^/]+)/(?P<i>[^/]+)/(?P<path>.+)$"
    )

    @classmethod
    def _deserialise_resolve_part(cls, index_entry):
        match = cls._RESOLVED_PATTERN.match(index_entry.path)
        if match:
            return match.group("key"), None

        match = cls._RESOLVE_PART_PATTERN.match(index_entry.path)
        if not match:
            raise RuntimeError(f"Couldn't deserialise resolve: {index_entry.path}")

        key = match.group("key")
        result_path = match.group("path")
        result_entry = cls.Entry(result_path, index_entry.id, index_entry.mode)
        return key, result_entry

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
        """Deserialise a MergedIndex from the given path."""
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
                resolves.setdefault(key, [])
                if resolve_part:
                    resolves[key] += [resolve_part]
            else:
                entries[e.path] = cls._ensure_entry(e)

        return MergedIndex(entries, conflicts, resolves)

    @classmethod
    def read_from_repo(cls, repo):
        """Deserialise a MergedIndex from the MERGED_INDEX file in the given repo."""
        return cls.read(repo.gitdir_file(MERGED_INDEX))

    def write(self, path):
        """
        Serialise this MergedIndex to the given path.
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
        """Serialise this MergedIndex to the MERGED_INDEX file in the given repo."""
        self.write(repo.gitdir_file(MERGED_INDEX))

    def write_resolved_tree(self, repo, resolve_conflict_fn=None):
        """
        Write all the merged entries and the resolved conflicts to a tree in the given repo.
        Resolved conflicts will be written the same as merged entries in the resulting tree.
        If a resolve_conflict_fn is supplied, then not all conflicts need to be resolved -
        those that are unresolved will be resolved automatically using the supplied function.
        """
        unresolved_conflicts = self.unresolved_conflicts
        if resolve_conflict_fn is None:
            assert not unresolved_conflicts

        index = pygit2.Index()

        # Entries that were merged automatically by libgit2, often trivially:
        for e in self.entries.values():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))

        # libgit2 leaves entries in the main part of the index, even if they are conflicts.
        # We make sure this index only contains merged entries and resolved conflicts.
        index.remove_all(list(self._conflicts_paths()))

        # Force-resolve any unresolved conflicts using the conflict-resolver:
        if resolve_conflict_fn and unresolved_conflicts:
            for c in unresolved_conflicts.values():
                for e in resolve_conflict_fn(c):
                    index.add(pygit2.IndexEntry(e.path, e.id, e.mode))

        # Entries that have been explicitly selected to resolve conflicts:
        for e in self._resolves_entries():
            index.add(pygit2.IndexEntry(e.path, e.id, e.mode))

        return index.write_tree(repo, write_merged_index_flags(repo))

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
        return [cls._ensure_entry(e) for e in resolve]


class VersionContext:
    """
    The necessary context for categorising or outputting a single version of a conflict.
    Holds the name of that version - one of "ancestor", "ours" or "theirs", the commit ID
    of that version, and optionally the name of the branch that was dereferenced to select
    that commit ID (note that  the branch may or may not still point to that commit ID).
    """

    def __init__(self, repo, version_name, commit_id, short_id, branch=None):
        # The Kart repository
        self.repo = repo
        # The name of the version - one of "ancestor", "ours" or "theirs".
        self.version_name = version_name
        # The commit ID - a pygit2.Oid object.
        self.commit_id = commit_id
        # A shorter but still unique prefix of the commit ID - a string.
        self.short_id = short_id
        # The name of the branch used to find the commit, or None if none was used.
        self.branch = branch

    @property
    @functools.lru_cache(maxsize=1)
    def repo_structure(self):
        return self.repo.structure(self.commit_id)

    @property
    def shorthand(self):
        return self.branch if self.branch else self.commit_id.hex

    def as_json(self):
        result = {"commit": self.commit_id.hex, "abbrevCommit": self.short_id}
        if self.branch:
            result["branch"] = self.branch
        return result


class MergeContext:
    """The necessary context for categorising or outputting each conflict in a merge."""

    def __init__(self, repo, versions):
        """An AncestorOursTheirs of VersionContext objects."""
        self.repo = repo
        self.versions = versions

    @classmethod
    def _zip_together(cls, repo, commit_ids3, short_ids3, branches3):
        names3 = AncestorOursTheirs.NAMES
        versions = AncestorOursTheirs(
            *(
                VersionContext(repo, n, c, s, b)
                for n, c, s, b in zip(names3, commit_ids3, short_ids3, branches3)
            )
        )
        return MergeContext(repo, versions)

    @classmethod
    def from_commit_with_refs(cls, commit_with_refs3, repo):
        commit_ids3 = commit_with_refs3.map(lambda c: c.id)
        short_ids3 = commit_with_refs3.map(lambda c: c.short_id)
        branches3 = commit_with_refs3.map(lambda c: c.branch_shorthand)
        return cls._zip_together(repo, commit_ids3, short_ids3, branches3)

    @classmethod
    def read_from_repo(cls, repo):
        # HEAD is assumed to be our side of the merge. MERGE_HEAD (and MERGED_INDEX)
        # are not version controlled, but are simply files in the repo. For these
        # reasons, the user should not be able to change branch mid merge.

        head = CommitWithReference.resolve(repo, "HEAD")
        ours_commit_id = head.id
        theirs_commit_id = pygit2.Oid(hex=repo.read_gitdir_file(MERGE_HEAD).strip())

        commit_ids3 = AncestorOursTheirs(
            # We find the ancestor by recalculating it fresh each time.
            repo.merge_base(ours_commit_id, theirs_commit_id),
            ours_commit_id,
            theirs_commit_id,
        )
        short_ids3 = commit_ids3.map(lambda c: repo[c].short_id)
        branches3 = AncestorOursTheirs(
            None,
            head.branch_shorthand,
            repo.read_gitdir_file(MERGE_BRANCH, missing_ok=True, strip=True),
        )

        return cls._zip_together(repo, commit_ids3, short_ids3, branches3)

    def write_to_repo(self, repo):
        # We don't write ancestor.commit_id - we just recalculate it when needed.
        # We don't write ours.commit_id - we can learn that from HEAD.
        # So we just write theirs.commit_id in MERGE_HEAD.
        repo.write_gitdir_file(MERGE_HEAD, self.versions.theirs.commit_id.hex)

        # We don't write ancestor.branch, since it's always None anyway.
        # We don't write ours.branch. we can learn that from HEAD.
        # So we just write theirs.branch in MERGE_BRANCH, unless its None.
        if self.versions.theirs.branch:
            repo.write_gitdir_file(MERGE_BRANCH, self.versions.theirs.branch)
        else:
            repo.remove_gitdir_file(MERGE_BRANCH)

    def get_message(self):
        theirs = self.versions.theirs
        theirs_desc = f'branch "{theirs.branch}"' if theirs.branch else theirs.shorthand
        return f"Merge {theirs_desc} into {self.versions.ours.shorthand}"

    def as_json(self):
        json3 = self.versions.map(lambda v: v.as_json())
        return json3.as_dict()


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
    @functools.lru_cache(maxsize=1)
    def decoded_path(self):
        return self.repo_structure.decode_path(self.path)

    @property
    def dataset_path(self):
        return self.decoded_path[0]

    @property
    def dataset(self):
        return self.repo_structure.datasets()[self.dataset_path]

    @property
    def dataset_part(self):
        return self.decoded_path[1]

    @property
    def is_meta(self):
        return self.dataset_part == "meta"

    @property
    def is_feature(self):
        return self.dataset_part == "feature"

    @property
    def is_tile(self):
        return self.dataset_part == "tile"

    @property
    def pk(self):
        assert self.is_feature
        return self.decoded_path[2]

    @property
    def pk_field(self):
        assert self.is_feature
        return self.dataset.primary_key

    @property
    def meta_path(self):
        assert self.is_meta
        return self.decoded_path[2]

    @property
    def tile_name(self):
        assert self.is_tile
        return self.decoded_path[2]

    @property
    @functools.lru_cache(maxsize=1)
    def feature(self):
        assert self.is_feature
        feature = self.dataset.get_feature(self.pk)
        return feature

    @property
    def meta_item(self):
        assert self.is_meta
        return self.dataset.get_meta_item(self.meta_path)

    @property
    @functools.lru_cache(maxsize=1)
    def tile_summary(self):
        assert self.is_tile
        return self.dataset.get_tile_summary(self.tile_name)

    def output(self, output_format, target_crs=None):
        """
        Output this version of this feature or meta_item in the
        given output_format - one of "text", "json" or "geojson".
        """
        if self.is_meta:
            result = self.meta_item
            if output_format == "text":
                result = json.dumps(result)
            return result

        if self.is_tile:
            result = self.tile_summary
            if output_format == "text":
                # TODO - transform extents to the target_crs.
                result = feature_as_text(result)
            return result

        if self.is_feature:
            if output_format == "text":
                return feature_as_text(self.feature)

            transform_func = (
                feature_as_json if output_format == "json" else feature_as_geojson
            )
            geometry_transform = None
            if target_crs is not None:
                geometry_transform = self.dataset.get_geometry_transform(target_crs)

            return transform_func(
                self.feature, self.pk, geometry_transform=geometry_transform
            )

    def matches_filter(self, repo_filter):
        if repo_filter.match_all:
            return True
        ds_filter = repo_filter.get(self.dataset_path, None)
        if not ds_filter:
            return False

        if self.is_meta:
            # Metadata always matches, since metadata filtering is not yet implemented.
            return True

        pk_filter = ds_filter.get("feature", None)
        return pk_filter and self.pk in pk_filter


class RichConflict:
    """
    Up to three RichConflictVersions that form a conflict.
    All necessary context of the conflict is collected in this class
    so that the conflict can be categorised, labelled, or output.
    """

    def __init__(self, entry3, merge_context):
        # Instead of (ancestor_entry, ours_entry, theirs_entry)
        # the caller may also supply (key, (ancestor_entry, ours_entry, theirs_entry))
        # to associate a particular key with this conflict:
        if isinstance(entry3, tuple) and isinstance(entry3[0], str):
            self.key, self.entry3 = entry3
        else:
            self.key = None
            self.entry3 = entry3
        self.versions = AncestorOursTheirs(
            *(
                RichConflictVersion(e, ctx) if e else None
                for e, ctx in zip(self.entry3, merge_context.versions)
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
    @functools.lru_cache(maxsize=1)
    def has_multiple_paths(self):
        """True if the conflict involves renames and has more than one path."""
        # Note: this never returns True in practise since we don't do rename detection during merges.
        paths = set(v.path for v in self.true_versions)
        return len(paths) > 1

    def _multiversion_decoded_path(self):
        def path_part(i):
            parts = set(v.decoded_path[i] for v in self.true_versions)
            if len(parts) == 1:
                return next(iter(parts))
            return ",".join(
                f"{v.version_name}={v.decoded_path[i]}" for v in self.true_versions
            )

        return tuple(path_part(i) for i in range(2))

    @property
    @functools.lru_cache(maxsize=1)
    def decoded_path(self):
        """
        Generally returns the same as calling decoded_path on any of the versions,
        ie a tuple that describes which feature or metadata this conflict involves:
        ("datasetA", "feature", "5")
        However any part of the tuple can be more complicated if the conflict spans
        multiple paths (because it involves renames), eg:
        ("datasetA", "feature", "ancestor=5,ours=6,theirs=7")
        """
        if self.has_multiple_paths:
            # Note: this never happens in practise since we don't do rename detection during merges.
            return self._multiversion_decoded_path()
        else:
            return self.any_true_version.decoded_path

    def as_key_filter(self):
        result = RepoKeyFilter()
        for version in self.true_versions:
            result.recursive_set(version.decoded_path, True)
        return result

    @property
    @functools.lru_cache(maxsize=1)
    def label(self):
        """
        A unique label for this conflict - eg: "datsetA:feature:5".
        See decoded_path.
        """
        return ":".join(str(p) for p in self.decoded_path)

    def output(self, output_format, include_label=False, target_crs=None):
        """Output this conflict in the given output_format - text, json or geojson."""
        l = f"{self.label}:" if include_label else ""
        return {
            (l + v.version_name): v.output(output_format, target_crs=target_crs)
            for v in self.true_versions
        }

    def matches_filter(self, conflict_filter):
        """Returns True if this conflict matches (or part of this conflict matches) the given filter."""
        return any(v.matches_filter(conflict_filter) for v in self.true_versions)


def rich_conflicts(raw_conflicts, merge_context):
    """Convert a list of AncestorOursTheirs tuples of Entrys to a list of RichConflicts."""
    return (RichConflict(c, merge_context) for c in raw_conflicts)


def ensure_conflicts_ready(rich_conflicts, repo):
    from .promisor_utils import fetch_promised_blobs

    rich_conflicts = list(rich_conflicts)
    if not rich_conflicts:
        return rich_conflicts

    missing_ids = set()
    for c in rich_conflicts:
        for v in c.true_versions:
            try:
                repo[v.id].size
            except KeyError:
                # We can't test if something is promised by the time its a conflict, unfortunately.
                missing_ids.add(v.id)

    if missing_ids:
        click.echo(
            f"Fetching {len(missing_ids)} missing but required conflicting versions of features...",
            err=True,
        )
        fetch_promised_blobs(repo, missing_ids)
    return rich_conflicts


def merge_context_to_text(jdict):
    theirs = jdict["theirs"]
    ours = jdict["ours"]
    theirs_branch = theirs.get("branch", None)
    theirs_desc = (
        f'branch "{theirs_branch}"' if theirs_branch else theirs["abbrevCommit"]
    )
    ours_desc = ours.get("branch", None) or ours["abbrevCommit"]
    return f"Merging {theirs_desc} into {ours_desc}"


def merge_status_to_text(jdict, fresh):
    """
    Converts the json output of kart merge (or of kart status, which uses
    the same format during a merge) to text output.

    jdict - the dictionary of json output.
    fresh - True if we just arrived in this state due to a merge command,
            False if the user is just checking the current state.
    """
    # this is here to avoid an import loop
    from .conflicts_util import conflicts_json_as_text

    merging_text = merge_context_to_text(jdict["merging"])

    if jdict.get("noOp", False):
        return merging_text + "\nAlready up to date"

    dry_run = jdict.get("dryRun", False)
    commit = jdict.get("commit", None)

    if jdict.get("fastForward", False):
        if dry_run:
            ff_text = (
                f"Can fast-forward to {commit}\n"
                "(Not actually fast-forwarding due to --dry-run)"
            )
        else:
            ff_text = f"Fast-forwarded to {commit}"
        return "\n".join([merging_text, ff_text])

    conflicts = jdict.get("conflicts", None)
    if not conflicts:
        if dry_run:
            no_conflicts_text = (
                "No conflicts: merge will succeed!\n"
                "(Not actually merging due to --dry-run)"
            )
        else:
            if fresh:
                no_conflicts_text = f"No conflicts!\nMerge committed as {commit}"
            else:
                no_conflicts_text = (
                    f"No conflicts!\nUse `kart merge --continue` to complete the merge"
                )
        return "\n".join([merging_text, no_conflicts_text])

    conflicts_header = "Conflicts found:" if fresh else "Conflicts:"
    conflicts_text = "\n\n".join([conflicts_header, conflicts_json_as_text(conflicts)])

    if dry_run:
        dry_run_text = "(Not actually merging due to --dry-run)"
        return "\n".join([merging_text, conflicts_text, dry_run_text])

    conflicts_help_text = (
        "View conflicts with `kart conflicts` and resolve them with `kart resolve`.\n"
        "Once no conflicts remain, complete this merge with `kart merge --continue`.\n"
        "Or use `kart merge --abort` to return to the previous state."
    )
    is_in = "is now in" if fresh else "is in"
    repo_state_text = f'Repository {is_in} "merging" state.'

    if fresh:
        # When the user performs a merge, we format the output as follows:
        # 1. Merging X and Y. 2. Conflicts found: XYZ. 3. Repo is now in merging state.
        return "\n".join(
            [merging_text, conflicts_text, repo_state_text, conflicts_help_text]
        )
    else:
        # When the user requests the current status, we format the output as follows:
        # 1. Repo is in merging state. 2. Merging X and Y. 3. Conflicts: XYZ.
        return "\n".join(
            [repo_state_text, merging_text, conflicts_text, conflicts_help_text]
        )


class WorkingCopyMerger:
    """
    This class helps us to write a merge index working copy so that it stays - where possible - up to date
    with the current state of the merge.
    This lets the user see the final state of those objects that were merged cleanly, and makes it
    simpler for them to put resolutions into the working copy for objects that were not merged cleanly
    (see `kart resolve --with=workingcopy`).
    """

    def __init__(self, repo, merge_context):
        self.repo = repo
        self.merge_context = merge_context

    def write_merged_tree(self, merged_index):
        """
        Given a MergedIndex that represents the merged-state *so far* - unresolved conflicts may still exist - we
        do our best to write it as a tree. This is mostly used for updating the working-copy below, but it is also
        used for serialising feature-resolves.
        """
        tree_id = merged_index.write_resolved_tree(self.repo, self.resolve_conflict)
        self.repo.write_gitdir_file(KartRepoFiles.MERGED_TREE, str(tree_id))
        return self.repo[tree_id]

    def update_working_copy(self, merged_index, merged_tree):
        """
        Given a MergedIndex that represents the merged-state *so far* - unresolved conflicts may still exist - we
        do our best to write it to the working copy anyway, so that the user can see those parts that merged cleanly,
        and those conflicts that we can we write to the WC, and so that they can use the WC as a starting point for
        specifying resolves.
        """
        # Fetch all LFS tiles from all sides of the conflict.
        self.ensure_lfs_tiles_fetched()
        # First pass - write the merged-tree to the WC. Conflicts have been forcibly resolved to make it fit into a tree,
        # so some information will be missing.
        self.repo.working_copy.reset(merged_tree, quiet=True)
        # Second pass - where possible, handle conflicts that can be written in a more complicated way without resolving them:
        self.write_conflicts_to_working_copy(merged_index)

    def ensure_lfs_tiles_fetched(self):
        workdir = self.repo.working_copy.workdir
        if not workdir:
            return

        commit_ids = set(v.commit_id for v in self.merge_context.versions if v)
        fetch_lfs_blobs_for_commits(self.repo, commit_ids)

    def resolve_conflict(self, conflict):
        """
        Generator: yields the resolution(s), if any, to a conflict, as pygit2.IndexEntrys.
        First pass - we forcibly but somewhat arbitrarily resolve any outstanding conflicts in the merge-index,
        write the result to tree, and use that tree to update the working copy.
        """
        rich_conflict = RichConflict(conflict, self.merge_context)
        # For feature and meta conflicts we can really only fit one possibility in the working copy.
        # We go with the "ours" versions, until the user specifies something else.
        dataset_part = rich_conflict.decoded_path[1]
        if not rich_conflict.has_multiple_paths and dataset_part in ("meta", "feature"):
            ours = rich_conflict.versions.ours
            if ours:
                yield ours.entry
        # Tile conflicts are effectively resolved as "deleted" here since we don't yield any resolve for them.
        # They are handled below in "write_conflicts_to_working_copy"

    def write_conflicts_to_working_copy(self, merged_index):
        """
        Second pass - those conflicts that can be written to the working copy in a more complicated way (ie
        without resolving them one way or another) are handled here.
        """
        # So far we only write conflicts to the workdir - writing other types conflicts to the working copy
        # (to the extent possible) is handled in the first pass by the conflict resolver.
        if not self.repo.working_copy.workdir:
            return

        for conflict in merged_index.unresolved_conflicts.values():
            rich_conflict = RichConflict(conflict, self.merge_context)
            dataset_path, dataset_part, item_path = rich_conflict.decoded_path

            if not rich_conflict.has_multiple_paths and dataset_part == "tile":
                # Tile conflict found - write all versions we can find to the workdir, with different names.
                tilename = item_path

                for version in rich_conflict.true_versions:
                    version_name = version.version_name
                    pointer_blob = self.repo[version.id]
                    pointer_dict = pointer_file_bytes_to_dict(pointer_blob)
                    lfs_path = get_local_path_from_lfs_hash(
                        self.repo, pointer_dict["oid"]
                    )
                    if not lfs_path.is_file():
                        click.echo(
                            f"Couldn't find tile {version_name} {tilename} locally - skipping...",
                            err=True,
                        )
                        continue
                    filename = set_tile_extension(
                        f"{tilename}.{version_name}", tile_format=pointer_dict
                    )
                    try_reflink(
                        lfs_path,
                        self.repo.working_copy.workdir.path / dataset_path / filename,
                    )
