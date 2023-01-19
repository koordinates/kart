import re
import warnings

import click
import pygit2

from kart.cli_util import KartCommand, RemovalInKart012Warning
from kart.exceptions import NotFound
from kart.import_sources import from_spec


class PreserveDoubleDash(KartCommand):
    """
    Preserves the double-dash ("--") arg from user input.

    Click normally swallows this arg, but using this command class preserves it.
    """

    def parse_args(self, ctx, args):
        args = list(args)
        for i in range(len(args)):
            arg = args[i]
            if arg == "--":
                # Insert a second `--` arg.
                # One of the `--` gets consumed by Click during super() below.
                # Then the second one gets left alone and we can pass it to git.
                args.insert(i + 1, "--")
                break

        return super(PreserveDoubleDash, self).parse_args(ctx, args)


def _separate_options(args, allow_options):
    options = []
    others = []
    for arg in args:
        if not arg.startswith("-"):
            others.append(arg)
        elif not allow_options:
            raise click.UsageError(f"No such option: {arg}")
        else:
            # It's not explicitly stated by https://git-scm.com/docs/git-check-ref-format
            # but this isn't a valid commit-ish.
            #    $ git branch -c -- -x
            #    fatal: '-x' is not a valid branch name.
            # So we can assume it's a CLI flag, presumably for git rather than kart.
            # It *could* be a path, but in that case the user should add a `--` before this option
            # to disambiguate, and they haven't done so here.
            issue_link = "https://github.com/koordinates/kart/issues/508"
            warnings.warn(
                f"{arg!r} is unknown to Kart and will be passed directly to git. "
                f"This will be removed in Kart 0.12! Please comment on {issue_link} if you need to use this option.",
                RemovalInKart012Warning,
            )
            options.append(arg)
    return options, others


def _append_kwargs_to_options(options, kwargs, allow_options):
    if not kwargs:
        return
    assert allow_options

    for option_name, option_val in kwargs.items():
        option_name = option_name.replace("_", "-", 1)
        if isinstance(option_val, bool):
            if option_val:
                options.append(f"--{option_name}")
        elif isinstance(option_val, (int, str)):
            options.append(f"--{option_name}={option_val}")
        elif isinstance(option_val, tuple):
            options.extend([f"--{option_name}={o}" for o in option_val])


HEAD_PATTERN = re.compile(r"^HEAD\b")
RANGE_PATTERN = re.compile(r"[^/]\.{2,}[^/]")

HINT = "Use '--' to separate paths from revisions, like this:\n'kart <command> [<revision>...] -- [<filter>...]'"
SILENCING_HINT = "To silence this warning, use '--' to separate paths from revisions, like this:\n'kart <command> [<revision>...] -- [<filter>...]'\n"


def _is_revision(repo, arg, dedupe_warnings):
    # These things *could* be a path, but in that case the user should add a `--` before this arg to
    # disambiguate, and they haven't done that here.
    if (
        arg == "[EMPTY]"
        or arg.endswith("^?")
        or RANGE_PATTERN.search(arg)
        or HEAD_PATTERN.search(arg)
    ):
        return True

    if "*" in arg:
        return False

    filter_path = arg.split(":", maxsplit=1)[0]
    head_tree = repo.head_tree
    is_useful_filter_at_head = head_tree and filter_path in head_tree

    if ":" in arg:
        if not is_useful_filter_at_head:
            click.echo(
                f"Assuming '{arg}' is a filter argument (that doesn't match anything at HEAD)",
                err=True,
            )
            dedupe_warnings.add(SILENCING_HINT)
        return False

    try:
        repo.resolve_refish(arg)
        is_revision = True
    except (KeyError, ValueError, pygit2.InvalidSpecError):
        is_revision = False

    if is_revision and not is_useful_filter_at_head:
        return True
    elif is_useful_filter_at_head and not is_revision:
        return False
    elif is_revision and is_useful_filter_at_head:
        raise click.UsageError(
            f"Ambiguous argument '{arg}' - could be either a revision or a filter\n{HINT}"
        )
    else:
        raise NotFound(
            f"Ambiguous argument '{arg}' - doesn't appear to be either a revision or a filter\n{HINT}"
        )


def _disambiguate_revisions_and_filters(repo, args):
    revisions = []
    filters = []
    dedupe_warnings = set()
    for i, arg in enumerate(args):
        if _is_revision(repo, arg, dedupe_warnings):
            if filters:
                raise click.UsageError(
                    f"Filter argument '{filters[0]}' should go after revision argument '{arg}'\n{HINT}"
                )
            revisions.append(arg)
        else:
            filters.append(arg)
    for warning in dedupe_warnings:
        click.echo(warning, err=True)
    return revisions, filters


def parse_revisions_and_filters(
    repo,
    args,
    kwargs=None,
    allow_options=False,
):
    """
    Interprets positional args for kart diff, show, and log, including "--", commits/refs/ranges, and filters.
    Returns a three-tuple: (options, commits/refs/ranges, filters)
    """

    # If kwargs are passed, allow_options must be set.
    assert allow_options or not kwargs

    # As soon as we encounter a filter, we assume all remaining args are also filters.
    # i.e. the filters must be given *last*.
    # If it's ambiguous whether something is a filter or not, we assume it's a commit-ish.
    # If you want to be unambiguous, provide the `--` arg to separate the list of commit-ish-es and filters.
    # This behaviour should be consistent with git's behaviour.

    if "--" in args:
        dash_index = args.index("--")
        filters = list(args[dash_index + 1 :])
        args = args[:dash_index]
        options, revisions = _separate_options(args, allow_options)
        _append_kwargs_to_options(options, kwargs, allow_options)
        return options, revisions, filters
    else:
        options, args = _separate_options(args, allow_options)
        _append_kwargs_to_options(options, kwargs, allow_options)
        revisions, filters = _disambiguate_revisions_and_filters(repo, args)
        return options, revisions, filters


def parse_import_sources_and_datasets(args):
    """
    Interprets positional args for kart import, and its specific sub-variants: kart table-import, kart point-cloud-import.
    These commands support two different formats:
    - kart import SOURCE [SOURCE] [SOURCE]
    - kart import SOURCE [DATASET] [DATASET]
    (Although specific sub-variants may have no or limited support for either format).
    Returns a two-tuple: (sources, datasets).
    If len(sources) > 1, then datasets will be empty, and if datasets is not empty, len(sources) will be 1.
    Raises a UsageError if the user-input doesn't conform to this idea.
    """

    import_source_types = set()

    def is_import_source(arg, allow_unrecognised=True):
        import_source_type = from_spec(arg, allow_unrecognised=allow_unrecognised)
        if import_source_type is not None:
            import_source_types.add(import_source_type)
            return True
        return False

    if not args:
        return [], []

    first_arg = args[0]
    assert is_import_source(first_arg, allow_unrecognised=False)

    other_args = args[1:]
    other_sources = []
    datasets = []
    for i, arg in enumerate(other_args):
        if is_import_source(arg):
            other_sources.append(arg)
        else:
            datasets.append(arg)

        if other_sources and datasets:
            raise click.UsageError(
                "Specifying multiple sources as well as datasets is not allowed.\n"
                "When importing, you may either supply multiple import sources:\n"
                "    kart import SOURCE [SOURCE] [SOURCE]\n"
                "OR you may supply datasets to import from a single source:"
                "    kart import SOURCE [DATASET] [DATASET]\n"
                "but this appears to be a mix of both:\n"
                f"    {other_args[i - 1]}\n"
                f"    {other_args[i]}\n"
            )

        if len(import_source_types) > 1:
            raise click.UsageError(
                "Cannot import more than one type of data in a single operation, as happened here:\n"
                f"    {first_arg}\n"
                f"    {arg}\n\n"
                "Perform these imports as two separate operations."
            )

    return [first_arg, *other_sources], datasets
