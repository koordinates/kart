import click
import pygit2

from . import merge


@click.command()
@click.option(
    "--ff/--no-ff",
    default=True,
    help=(
        "When the merge resolves as a fast-forward, only update the branch pointer, without creating a merge commit. "
        "With --no-ff create a merge commit even when the merge resolves as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date or the merge can be resolved as a fast-forward."
    ),
)
@click.option(
    "--ff-only",
    default=False,
    is_flag=True,
    help=(
        "Refuse to merge and exit with a non-zero status unless the current HEAD is already up to date or the merge can be resolved as a fast-forward."
    ),
)
@click.argument("repository", required=False, metavar="REMOTE")
@click.argument("refspecs", nargs=-1, required=False, metavar="REFISH")
@click.pass_context
def pull(ctx, ff, ff_only, repository, refspecs):
    """ Fetch from and integrate with another repository or a local branch """
    repo = ctx.obj.repo

    if repository is None:
        # matches git-pull behaviour
        if repo.head_is_detached:
            raise click.UsageError((
                "You are not currently on a branch. "
                "Please specify which branch you want to merge with."
            ))

        # git-fetch:
        # When no remote is specified, by default the origin remote will be used,
        # unless there's an upstream branch configured for the current branch.

        current_branch = repo.branches[repo.head.shorthand]
        if current_branch.upstream:
            repository = current_branch.upstream.remote_name
        else:
            try:
                repository = repo.remotes['origin'].name
            except KeyError:
                # git-pull seems to just exit 0 here...?
                raise click.BadParameter("Please specify the remote you want to fetch from", param_hint="repository")

    remote = repo.remotes[repository]

    # do the fetch
    print("Running fetch:", repository, refspecs)
    remote.fetch((refspecs or None))
    # subprocess.check_call(["git", "-C", ctx.obj.repo_path, 'fetch', repository] + list(refspecs))

    # now merge with FETCH_HEAD
    print("Running merge:", {'ff': ff, 'ff_only': ff_only, 'commit': "FETCH_HEAD"})
    ctx.invoke(merge.merge, ff=ff, ff_only=ff_only, commit="FETCH_HEAD")
