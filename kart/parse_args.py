from enum import Enum, auto
import warnings

from .cli_util import KartCommand, RemovalInKart012Warning


import click
import pygit2


class PreserveDoubleDash(KartCommand):
    """
    Preserves the double-dash ("--") arg from user input.

    Click normally swallows this arg, but using this command class preserves it.
    """

    def parse_args(self, ctx, args):
        from kart.cli import get_version_tuple

        args = list(args)
        for i in range(len(args)):
            arg = args[i]
            if arg == "--":
                if "--" in args[i + 1 :] and get_version_tuple() <= ("0", "12"):
                    # Before we added this shim, we had users using a workaround (adding the `--` twice themselves),
                    # which ideally we'd like them to stop doing.
                    warnings.warn(
                        "Using '--' twice is no longer needed, and will behave differently or fail in Kart 0.12",
                        RemovalInKart012Warning,
                    )
                else:
                    # Insert a second `--` arg.
                    # One of the `--` gets consumed by Click during super() below.
                    # Then the second one gets left alone and we can pass it to git.
                    args.insert(i + 1, "--")
                break

        return super(PreserveDoubleDash, self).parse_args(ctx, args)


class ArgType(Enum):
    # Which revision(s) to display - a commit, ref, range, etc:
    COMMIT = auto()
    # How to log it.
    OPTION = auto()
    # Which item(s) the user is interested in. These must come last.
    # In Git you filter by path so these are called paths - but we don't expose the internal path
    # of most Kart items, so we we just call these filters.
    FILTER = auto()


def get_arg_type(repo, arg, allow_options=True, allow_commits=True, allow_filters=True):
    """Decides if some user-supplied argument is a commit-ish or a filter (or even an option)."""

    # We prefer to parse args as commits if at all plausible - if the user meant it to be a filter,
    # they always have the possibility to force it to be a filter using "--".
    # So we parse "foo...bar" as a commit range without checking if foo and bar exist - it's more likely that the user
    # meant that, than that they want to filter to the path "foo...bar" and if it doesn't work, we'll error accordingly.

    assert allow_commits or allow_filters

    if arg.startswith("-"):
        if allow_options:
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
            return ArgType.OPTION
        else:
            raise click.UsageError(f"No such option: {arg}")

    if allow_commits:
        if arg == "[EMPTY]" or ".." in arg:
            return ArgType.COMMIT

        try:
            repo.resolve_refish(arg)
            return ArgType.COMMIT
        except (KeyError, ValueError, pygit2.InvalidSpecError):
            pass

    if allow_filters:
        return ArgType.FILTER

    raise click.UsageError(
        f"Argument not recognised as a valid commit, ref, or range: {arg}"
    )


def parse_commits_and_filters(
    repo,
    args,
    kwargs=None,
    allow_options=False,
):
    """
    Interprets positional args for kart diff, show, and log, including "--", commits/refs/ranges, and filters.
    Returns a three-tuple: (options, commits/refs/ranges, filters)
    """

    # As soon as we encounter a filter, we assume all remaining args are also filters.
    # i.e. the filters must be given *last*.
    # If it's ambiguous whether something is a filter or not, we assume it's a commit-ish.
    # If you want to be unambiguous, provide the `--` arg to separate the list of commit-ish-es and filters.
    # This behaviour should be consistent with git's behaviour.

    if "--" in args:
        dash_index = args.index("--")
        filters = list(args[dash_index + 1 :])
        args = args[:dash_index]
    else:
        dash_index = None
        filters = []

    options = []
    commits = []

    lists_by_type = {
        ArgType.OPTION: options,
        ArgType.COMMIT: commits,
        ArgType.FILTER: filters,
    }

    allow_commits = True
    allow_filters = dash_index is None
    for arg in args:
        arg_type = get_arg_type(
            repo,
            arg,
            allow_options=allow_options,
            allow_commits=allow_commits,
            allow_filters=allow_filters,
        )
        lists_by_type[arg_type].append(arg)
        if arg_type == ArgType.FILTER:
            allow_commits = False

    if kwargs is not None and allow_options:
        for option_name, option_val in kwargs.items():
            option_name = option_name.replace("_", "-", 1)
            mapping = {
                "int": lambda: options.append(f"--{option_name}={option_val}"),
                "str": lambda: options.append(f"--{option_name}={option_val}"),
                "tuple": lambda: options.extend(
                    [f"--{option_name}={o}" for o in option_val]
                ),
                "bool": lambda: options.append(f"--{option_name}")
                if option_val
                else None,
            }
            mapping.get(type(option_val).__name__, lambda: "None")()

    return options, commits, filters
