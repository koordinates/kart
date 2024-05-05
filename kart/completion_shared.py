from kart.merge_util import MergeContext, MergedIndex, RichConflict
from kart.repo import KartRepoState
from kart.context import Context


class CompletionSet(set):
    # This is a workaround for an inconvenience in Click's completion API:
    # Click expects that shell_complete functions return lists - Click checks the first value using result[0].
    # However, sets make more sense and are easier to test etc - don't have to worry about duplicates or ordering.
    # There are other workarounds that involve returning lists, but this works well for now.

    def __getitem__(self, index):
        if not self:
            raise IndexError("list index out of range")
        assert index == 0
        return next(iter(self))


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

    return CompletionSet(_do_complete_refs(repo, incomplete))


def _do_complete_refs(repo, incomplete=""):
    refs = set()
    for ref in repo.references:
        if incomplete and ref.startswith(incomplete):
            refs.add(ref)
        partial_ref = ref.split("/")[-1]
        if partial_ref.startswith(incomplete):
            refs.add(partial_ref)

    return refs


def conflict_completer(ctx=None, param=None, incomplete=""):
    repo = discover_repository(allowed_states=KartRepoState.MERGING)
    if not repo:
        return []

    merged_index = MergedIndex.read_from_repo(repo)
    merge_context = MergeContext.read_from_repo(repo)
    labels = set()

    for _, conflict in merged_index.conflicts.items():
        rich_conflict = RichConflict(conflict, merge_context)
        label = rich_conflict.label
        if label.startswith(incomplete):
            labels.add(label)

    # TODO - this does not work very well, because:
    # Conflict labels have ':' in them.
    # POSIX shells don't treat ':' as special - but click shell completion for some reason does.
    # It treats each ':' as starting a new argument, so, won't help with completing from that point on.
    # The user can fix this by quoting the argument, but we can't make them quote the argument - at this point, we can't
    # even detect if what they typed is already quoted or not, so we can't add any extra quotes.

    return CompletionSet(labels)


def repo_path_completer(ctx=None, param=None, incomplete=""):
    repo = discover_repository(allowed_states=KartRepoState.ALL_STATES)
    if not repo:
        return []

    return _do_complete_paths(repo, incomplete)


def _do_complete_paths(repo, incomplete=""):
    all_ds_paths = repo.datasets("HEAD").paths()
    return CompletionSet(p for p in all_ds_paths if p.startswith(incomplete))


def ref_or_repo_path_completer(ctx=None, param=None, incomplete=""):
    repo = discover_repository()
    if not repo:
        return []

    return CompletionSet(
        _do_complete_refs(repo, incomplete) | _do_complete_paths(repo, incomplete)
    )


def file_path_completer(ctx=None, param=None, incomplete=""):
    from click.shell_completion import CompletionItem

    # Return a special completion marker that tells the completion
    # system to use the shell to provide file path completions.
    return [CompletionItem(incomplete, type="file")]
