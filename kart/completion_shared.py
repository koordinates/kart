import itertools

from kart.merge_util import MergeContext, MergedIndex, RichConflict
from kart.repo import KartRepoState
from kart.context import Context


def discover_repository(allowed_states=None):
    ctx = Context()
    try:
        return ctx.get_repo(allowed_states=allowed_states)
    except Exception:
        pass


def ref_completer(ctx=None, param=None, incomplete=""):
    repo = discover_repository()
    if not repo:
        return []
    all_refs = repo.listall_branches()
    if not incomplete:
        for ref in repo.references:
            new_ref = ref.split("/")[-1]
            if new_ref not in all_refs:
                all_refs.append(ref)
        return all_refs

    all_refs = itertools.chain(all_refs, repo.references)
    return [b for b in all_refs if b.startswith(incomplete)]


def conflict_completer(ctx=None, param=None, incomplete=""):
    repo = discover_repository(allowed_states=KartRepoState.MERGING)
    if not repo:
        return []

    merged_index = MergedIndex.read_from_repo(repo)
    merge_context = MergeContext.read_from_repo(repo)
    rich_conflicts = []
    ds_conflicts = set()

    for _, conflict in merged_index.conflicts.items():
        rich_conflict = RichConflict(conflict, merge_context)
        if rich_conflict.label.startswith(incomplete):
            rich_conflicts.append(rich_conflict.label)
            ds_conflicts.add(rich_conflict.label.split(":")[0])

    if not incomplete:
        return list(ds_conflicts)

    return rich_conflicts


def path_completer(ctx=None, param=None, incomplete=""):
    repo = discover_repository(allowed_states=KartRepoState.ALL_STATES)
    if not repo:
        return []

    all_ds_paths = list(repo.datasets("HEAD").paths())
    return all_ds_paths
