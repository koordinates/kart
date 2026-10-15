import logging
import re
from pathlib import Path

from kart.diff_format import DiffFormat
from kart.diff_structs import FILES_KEY, Delta, DeltaDiff, DatasetDiff, RepoDiff
from kart.exceptions import SubprocessError
from kart.key_filters import DatasetKeyFilter, RepoKeyFilter
from kart.structure import RepoStructure
from kart import subprocess_util as subprocess

L = logging.getLogger("kart.diff_util")

# Pathspecs identifying attachment files - everything that is not a Kart-internal blob
# and not part of any dataset's contents. Used as exclude patterns with `git`.
ATTACHMENT_PATHSPECS = (
    ":^.kart.*",  # Top-level hidden kart blobs
    ":^**/.*dataset*/**",  # Data inside datasets
)


def get_all_ds_paths(
    base_rs: RepoStructure,
    target_rs: RepoStructure,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
):
    """Returns a list of all dataset paths in either RepoStructure (that match repo_key_filter).

    Args:
        base_rs (kart.structure.RepoStructure)
        target_rs (kart.structure.RepoStructure)
        repo_key_filter (kart.key_filters.RepoKeyFilter): Controls which datasets match and are included in the result.

    Returns:
        Sorted list of all dataset paths in either RepoStructure (that match repo_key_filter).
    """
    base_ds_paths = {ds.path for ds in base_rs.datasets()}
    target_ds_paths = {ds.path for ds in target_rs.datasets()}
    all_ds_paths = base_ds_paths | target_ds_paths

    if not repo_key_filter.match_all:
        all_ds_paths = repo_key_filter.filter_keys(all_ds_paths)

    return sorted(list(all_ds_paths))


def get_repo_diff(
    base_rs,
    target_rs,
    *,
    include_wc_diff=False,
    workdir_diff_cache=None,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
    convert_to_dataset_format=None,
    include_files=False,
    diff_format=DiffFormat.FULL,
):
    """
    Generates a RepoDiff containing an entry for every dataset in the repo
    (so long as it matches repo_key_filter and has any changes).

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    include_wc_diff - if True the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    workdir_diff_cache - not required, but can be provided if a WorkdirDiffCache is already in use
        to save repeated work.
    repo_key_filter - controls which datasets (and PK values) match and are included in the diff.
    convert_to_dataset_format - whether to show the diff of what would be committed if files were
       converted to dataset format at commit-time (ie, for point-cloud and raster tiles)
    include_files - whether to include a DatasetDiff in the result for changes to files that
       are simply standalone files, rather than part of a dataset's contents.
    """

    all_ds_paths = get_all_ds_paths(base_rs, target_rs, repo_key_filter)

    if include_wc_diff and workdir_diff_cache is None:
        workdir_diff_cache = target_rs.repo.working_copy.workdir_diff_cache()
    repo_diff = RepoDiff()
    for ds_path in all_ds_paths:
        repo_diff[ds_path] = get_dataset_diff(
            ds_path,
            base_rs.datasets(),
            target_rs.datasets(),
            diff_format=diff_format,
            include_wc_diff=include_wc_diff,
            workdir_diff_cache=workdir_diff_cache,
            ds_filter=repo_key_filter[ds_path],
            convert_to_dataset_format=convert_to_dataset_format,
        )
    if include_files:
        file_diff = get_file_diff(
            base_rs,
            target_rs,
            include_wc_diff=include_wc_diff,
            workdir_diff_cache=workdir_diff_cache,
            repo_key_filter=repo_key_filter,
        )
        if file_diff:
            repo_diff.recursive_set([FILES_KEY, FILES_KEY], file_diff)

    # No need to prune recursively since self.get_dataset_diff already prunes the dataset diffs.
    repo_diff.prune(recurse=False)
    return repo_diff


def get_dataset_diff(
    ds_path,
    base_datasets,
    target_datasets,
    *,
    include_wc_diff=False,
    workdir_diff_cache=None,
    ds_filter=DatasetKeyFilter.MATCH_ALL,
    convert_to_dataset_format=None,
    diff_format=DiffFormat.FULL,
):
    """
    Generates the DatasetDiff for the dataset at path dataset_path.

    base_rs, target_rs - kart.structure.RepoStructure objects to diff between.
    include_wc_diff - if True the diff generated will be from base_rs<>working_copy
        (in which case, target_rs must be the HEAD commit which the working copy is tracking).
    workdir_diff_cache - reusing the same WorkdirDiffCache for every dataset that is being diffed at one time
        is more efficient as it can save FileSystemWorkingCopy.raw_diff_from_index being called multiple times
    ds_filter - controls which PK values match and are included in the diff.
    """
    base_target_diff = None
    target_wc_diff = None

    if base_datasets == target_datasets:
        base_ds = target_ds = base_datasets.get(ds_path)

    else:
        # diff += base_ds<>target_ds
        base_ds = base_datasets.get(ds_path)
        target_ds = target_datasets.get(ds_path)

        if base_ds is not None:
            from_ds, to_ds = base_ds, target_ds
            reverse = False
        else:
            from_ds, to_ds = target_ds, base_ds
            reverse = True

        # If the diff_format is none, then we don't need to do any work to generate the diff. Else:
        if diff_format != DiffFormat.NONE:
            base_target_diff = from_ds.diff(
                to_ds, ds_filter=ds_filter, reverse=reverse, diff_format=diff_format
            )
            L.debug("base<>target diff (%s): %s", ds_path, repr(base_target_diff))

    if include_wc_diff:
        # diff += target_ds<>working_copy
        # note: target_ds may be None if the dataset as deleted between the base & target commits
        if target_ds is not None:
            if workdir_diff_cache is None:
                workdir_diff_cache = target_ds.repo.working_copy.workdir_diff_cache()
            target_wc_diff = target_ds.diff_to_working_copy(
                workdir_diff_cache,
                ds_filter=ds_filter,
                convert_to_dataset_format=convert_to_dataset_format,
            )
            L.debug(
                "target<>working_copy diff (%s): %s",
                ds_path,
                repr(target_wc_diff),
            )
    ds_diff = DatasetDiff.concatenated(base_target_diff, target_wc_diff)
    if include_wc_diff:
        # Get rid of parts of the diff-structure that are "empty":
        ds_diff.prune()
    return ds_diff


ZEROES = re.compile(r"0+")


def get_file_diff(
    base_rs,
    target_rs,
    *,
    include_wc_diff=False,
    workdir_diff_cache=None,
    repo_key_filter=RepoKeyFilter.MATCH_ALL,
):
    """
    Returns a delta-diff for changed files aka attachments.
    Each delta just contains the old and new file OIDs - any more than this may be unhelpful since it takes
    CPU time to produce but isn't necessarily easier to consume than OIDs, which are straight-forward to
    turn into raw files once you know how. (Various diff-writers can transform these OIDs into inline diffs if you
    set the --diff-files flag).

    If include_wc_diff is True the diff is generated between base_rs.tree and the working
    directory (target_rs is then assumed to be the HEAD-equivalent tracked by the working
    directory). Otherwise it is generated between base_rs.tree and target_rs.tree.
    """
    repo = target_rs.repo
    old_tree = base_rs.tree

    if include_wc_diff:
        return _get_workdir_file_diff(repo, old_tree, repo_key_filter, workdir_diff_cache)

    new_tree = target_rs.tree

    # TODO - make sure this is skipping over datasets efficiently.
    # TODO - we could turn on rename detection.
    cmd = [
        "git",
        "-C",
        repo.path,
        "diff",
        old_tree.hex,
        new_tree.hex,
        "--raw",
        "--no-renames",
        "--",
        *ATTACHMENT_PATHSPECS,
    ]
    try:
        lines = subprocess.check_output(cmd, encoding="utf8").strip().splitlines()
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git diff: {e}", called_process_error=e
        )

    attachment_deltas = DeltaDiff()

    for line in lines:
        parts = line.split()
        old_sha, new_sha, path = parts[2], parts[3], parts[5]
        if not path_matches_repo_key_filter(path, repo_key_filter):
            continue
        old_half_delta = (path, old_sha) if not ZEROES.fullmatch(old_sha) else None
        new_half_delta = (path, new_sha) if not ZEROES.fullmatch(new_sha) else None
        attachment_deltas.add_delta(Delta(old_half_delta, new_half_delta))

    return attachment_deltas


def _kart_managed_workdir_files(repo):
    """
    Returns a set of workdir-relative file paths that Kart manages internally and that should
    not appear as attachments (e.g. the SQLite/GeoPackage working copy file).
    """
    wc_location = repo.workingcopy_location
    if wc_location and "://" not in wc_location:
        return {wc_location}
    return set()


def _make_attachment_filter(workdir_diff_cache):
    """
    Returns a callable ``is_attachment_non_tile(path)`` that returns True only for paths that
    are attachment files and do NOT live inside a tile-dataset working directory.

    Tile files such as ``elevation/EK.tif`` are tracked in the workdir-index by
    FileSystemWorkingCopy and therefore appear in ``_git_diff_paths()`` when modified.
    ``is_attachment_path()`` alone cannot exclude them because it only knows about the internal
    dataset structure (``.raster-dataset.v1/`` etc.) and not the top-level dataset directory
    names.  Only *tile* datasets store tiles under their directory; files under a *tabular*
    dataset directory (e.g. ``nz_pa_points_topo_150k/metadata.xml``) are user-facing
    attachments, so we exclude only the tile-dataset directory prefixes.
    """
    tile_ds_prefixes = tuple(
        ds + "/" for ds in workdir_diff_cache.tile_dataset_paths()
    )

    def _is_attachment_non_tile(p):
        if not is_attachment_path(p):
            return False
        if tile_ds_prefixes and p.startswith(tile_ds_prefixes):
            return False
        return True

    return _is_attachment_non_tile


def _classify_workdir_attachment_paths(
    repo, repo_key_filter, workdir_diff_cache, tree_files, managed
):
    """
    Returns (present_changed, deleted, untracked) - the attachment paths in the working directory
    that may differ from the tree:

    - present_changed: tracked files present on disk whose content *may* differ (the caller
      confirms by hashing - the fast path narrows this to git-flagged changes, the slow path
      returns every present tracked file).
    - deleted: tracked files that were extracted to disk but are now missing.
    - untracked: files on disk that are not in the tree at all.

    When workdir_diff_cache is provided this uses the workdir-index (git's mtime optimisation)
    so unchanged large files are never re-hashed. Without a cache it falls back to enumerating
    every tree attachment and stat-ing it - correct, but O(n) in the number of attachments.
    Both branches feed the same delta/status builders in the callers, so the only thing that
    differs between fast and slow is how these three lists are computed.
    """
    workdir = str(repo.workdir_path)
    workdir_path = Path(workdir)

    if workdir_diff_cache is not None:
        # Fast path: the workdir-index already knows which files changed.
        # _git_diff_paths()  = tracked files that changed (modified or deleted on disk).
        # _git_ls_others_paths() = files on disk not in the index (new/untracked).
        is_attachment_non_tile = _make_attachment_filter(workdir_diff_cache)
        diff_paths = [
            p
            for p in workdir_diff_cache._git_diff_paths()
            if is_attachment_non_tile(p)
            and path_matches_repo_key_filter(p, repo_key_filter)
        ]
        untracked = [
            p
            for p in workdir_diff_cache._git_ls_others_paths()
            if is_attachment_non_tile(p)
            and p not in tree_files
            and p not in managed
            and path_matches_repo_key_filter(p, repo_key_filter)
        ]
        present_changed = [p for p in diff_paths if (workdir_path / p).is_file()]
        deleted = [p for p in diff_paths if not (workdir_path / p).is_file()]
        return present_changed, deleted, untracked

    # Slow path (no workdir-index): enumerate all tree attachments and stat them.
    tracked = [
        p for p in tree_files if path_matches_repo_key_filter(p, repo_key_filter)
    ]
    present_changed = [p for p in tracked if (workdir_path / p).is_file()]
    # Only report as deleted if the file was previously extracted (present in the git index).
    index_files = set(_ls_index_attachments(workdir))
    deleted = [
        p for p in tracked if not (workdir_path / p).is_file() and p in index_files
    ]
    untracked = [
        p
        for p in ls_workdir_untracked_attachments(workdir)
        if p not in tree_files
        and p not in managed
        and path_matches_repo_key_filter(p, repo_key_filter)
    ]
    return present_changed, deleted, untracked


def _get_workdir_file_diff(repo, base_tree, repo_key_filter, workdir_diff_cache=None):
    """
    Returns a delta-diff for attachment files between base_tree and the working directory.

    Path classification (which files are modified / deleted / untracked) is delegated to
    _classify_workdir_attachment_paths, which uses the workdir-index fast path when a cache is
    available and falls back to a full scan otherwise. This function then hashes the candidate
    files and assembles the deltas - identically regardless of which path produced the lists.

    New blobs are written to the object database via `git hash-object -w` so the resulting
    deltas reference real OIDs, allowing diff-writers to fetch their content for `--diff-files`
    output. Any blobs not subsequently referenced by a commit will be cleaned up by `git gc`.
    """
    workdir = str(repo.workdir_path)
    managed = _kart_managed_workdir_files(repo)
    tree_files = ls_tree_attachments(workdir, base_tree.hex)

    present_changed, deleted, untracked = _classify_workdir_attachment_paths(
        repo, repo_key_filter, workdir_diff_cache, tree_files, managed
    )

    attachment_deltas = DeltaDiff()
    new_oids = _hash_workdir_files(workdir, present_changed + untracked)

    for path in present_changed:
        old_sha = tree_files.get(path)
        new_sha = new_oids.get(path)
        if not new_sha or new_sha == old_sha:
            continue
        attachment_deltas.add_delta(
            Delta((path, old_sha) if old_sha else None, (path, new_sha))
        )

    for path in deleted:
        old_sha = tree_files.get(path)
        if old_sha:
            attachment_deltas.add_delta(Delta((path, old_sha), None))

    for path in untracked:
        new_sha = new_oids.get(path)
        if new_sha:
            attachment_deltas.add_delta(Delta(None, (path, new_sha)))

    return attachment_deltas


_ATTACHMENT_DATASET_DIR_RE = re.compile(r"\.[^/]*dataset[^/]*$")
# Top-level path prefixes (matching `:^.kart.*` and `:^.git*` semantics) that are reserved for
# kart/git internals - both the directories and any sibling top-level files starting with these
# prefixes (e.g. `.kart.repostructure.version`).
_ATTACHMENT_TOP_LEVEL_PREFIXES = (".kart", ".git")
# Per-branding README files (e.g. KART_README.txt, SNO_README.txt) that kart writes to the
# workdir as a courtesy and excludes from version control via .kart/info/exclude.
_ATTACHMENT_README_RE = re.compile(r"^[A-Z]+_README\.[^/]*$")


def is_attachment_path(path):
    """Returns True if path is an attachment file (anything that is not a Kart-internal blob,
    a managed Kart courtesy file, or part of a dataset's contents)."""
    parts = path.split("/")
    top = parts[0]
    if any(
        top == prefix or top.startswith(prefix + ".")
        for prefix in _ATTACHMENT_TOP_LEVEL_PREFIXES
    ):
        return False
    if len(parts) == 1 and _ATTACHMENT_README_RE.match(top):
        return False
    # Exclude any path with a `.<...>dataset...` directory component, matching the
    # `:^**/.*dataset*/**` pathspec used by `git diff`.
    for part in parts[:-1]:
        if _ATTACHMENT_DATASET_DIR_RE.match(part):
            return False
    return True


def ls_tree_attachments(workdir, tree_ref):
    """
    Returns {rel_path: blob_oid} for every attachment file in the given tree.

    `git ls-tree` does not accept exclude pathspecs (the `:^` magic is only honoured by `git diff`
    and `git ls-files`), so the equivalent filter is applied in Python via is_attachment_path().
    """
    cmd = ["git", "-C", workdir, "ls-tree", "-r", "-z", tree_ref]
    try:
        out = subprocess.check_output(cmd, encoding="utf8")
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git ls-tree: {e}", called_process_error=e
        )

    result = {}
    for entry in out.split("\0"):
        if not entry:
            continue
        # Format: "<mode> SP <type> SP <oid> TAB <path>"
        meta, path = entry.split("\t", 1)
        if not is_attachment_path(path):
            continue
        _mode, _type, oid = meta.split(" ", 2)
        result[path] = oid
    return result


def _ls_index_attachments(workdir):
    """
    Returns the set of attachment file paths currently staged in the git index.

    Only files that have been explicitly extracted to the working directory (via `git checkout`)
    appear in the index.  This lets callers distinguish between "file was checked out and then
    deleted by the user" (in index, missing from workdir) and "file was never extracted" (absent
    from both index and workdir).
    """
    cmd = ["git", "-C", workdir, "ls-files", "--stage", "-z"]
    try:
        out = subprocess.check_output(cmd, encoding="utf8")
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git ls-files: {e}", called_process_error=e
        )
    result = set()
    for entry in out.split("\0"):
        if not entry:
            continue
        # Format: "<mode> SP <oid> SP <stage>\t<path>"
        _meta, path = entry.split("\t", 1)
        if is_attachment_path(path):
            result.add(path)
    return result


def ls_workdir_untracked_attachments(workdir):
    """
    Returns the list of attachment files in the working directory that aren't tracked. Filtering
    is done in Python because `:^` exclude pathspecs do not reliably traverse subdirectories
    (e.g. `:^.kart.*` does not match `.kart/HEAD`); see is_attachment_path().
    """
    cmd = ["git", "-C", workdir, "ls-files", "--others", "--exclude-standard", "-z"]
    try:
        out = subprocess.check_output(cmd, encoding="utf8")
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git ls-files: {e}", called_process_error=e
        )
    return [p for p in out.split("\0") if p and is_attachment_path(p)]


def get_workdir_file_status(
    repo, base_tree=None, repo_key_filter=RepoKeyFilter.MATCH_ALL, workdir_diff_cache=None
):
    """
    Returns a classification of attachment files in the working directory relative to base_tree
    (defaulting to HEAD), as three sorted lists:

        {"modified": [...], "untracked": [...], "deleted": [...]}

    "modified" - tracked file present in the workdir but with different content from base_tree.
    "untracked" - file present in the workdir but absent from base_tree.
    "deleted" - tracked file in base_tree but absent from the workdir.

    When workdir_diff_cache is provided the function reuses its cached git-index results to avoid
    redundant git invocations when called alongside get_repo_diff().

    Cheaper than calling get_file_diff() with include_wc_diff=True when only the file lists are
    needed, because untouched modifications can be detected via blob OID comparison without
    writing new blobs to the object database (write_to_odb=False below).
    """
    if base_tree is None:
        base_tree = repo.head_tree
    workdir = str(repo.workdir_path)
    managed = _kart_managed_workdir_files(repo)
    tree_files = ls_tree_attachments(workdir, base_tree.hex)

    present_changed, deleted, untracked = _classify_workdir_attachment_paths(
        repo, repo_key_filter, workdir_diff_cache, tree_files, managed
    )

    # A "present_changed" path is only actually modified if its content differs from the tree.
    # Hash without -w since we only need the OIDs for comparison, not to persist blobs.
    new_oids = _hash_workdir_files(workdir, present_changed, write_to_odb=False)
    modified = sorted(p for p in present_changed if new_oids.get(p) != tree_files.get(p))

    return {
        "modified": modified,
        "untracked": sorted(untracked),
        "deleted": sorted(deleted),
    }


def _hash_workdir_files(workdir, rel_paths, write_to_odb=True):
    """
    Hashes each rel_path under workdir, returning {rel_path: blob_oid}.

    If write_to_odb is True the blobs are also written to the object database (so that diff-writers
    can later fetch their content); these dangling blobs will be cleaned up by `git gc` if
    `kart commit-files` is not subsequently used to record them. Pass write_to_odb=False when only
    the OIDs are needed (e.g. for status comparisons).
    """
    if not rel_paths:
        return {}
    cmd = ["git", "-C", workdir, "hash-object", "--no-filters", "--stdin-paths"]
    if write_to_odb:
        cmd.insert(4, "-w")
    try:
        proc = subprocess.run(
            cmd,
            input="\n".join(rel_paths) + "\n",
            capture_output=True,
            encoding="utf8",
            check=True,
        )
    except subprocess.CalledProcessError as e:
        raise SubprocessError(
            f"There was a problem with git hash-object: {e.stderr or e}",
            called_process_error=e,
        )
    oids = proc.stdout.strip().splitlines()
    return dict(zip(rel_paths, oids))


def path_matches_repo_key_filter(path, repo_key_filter):
    if repo_key_filter.match_all:
        return True
    # Return attachments that have a name that we are matching all of.
    if path in repo_key_filter and repo_key_filter[path].match_all:
        return True
    # Return attachments that are inside a folder that we are matching all of.
    for p, dataset_filter in repo_key_filter.items():
        if not dataset_filter.match_all:
            continue
        if p == path:
            return True
        if path.startswith(p) and (p.endswith("/") or path[len(p)] == "/"):
            return True
    # Don't return attachments inside a dataset / folder that we are only matching some of
    # ie, only matching certain features or meta items.
    return False
