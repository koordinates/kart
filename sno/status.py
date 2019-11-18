import click
import pygit2

from .structure import RepositoryStructure


@click.command()
@click.pass_context
def status(ctx):
    """ Show the working copy status """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    rs = RepositoryStructure(repo)

    commit = repo.head.peel(pygit2.Commit)

    if repo.head_is_detached:
        click.echo(f"{click.style('HEAD detached at', fg='red')} {commit.short_id}")
    else:
        click.echo(get_branch_status_message(repo))

    # working copy state
    working_copy = rs.working_copy
    if not working_copy:
        click.echo(
            '\nNo working copy.\n  (use "sno checkout" to create a working copy)'
        )
        return

    wc_changes = {}
    for dataset in rs:
        status = working_copy.status(dataset)
        if any(status.values()):
            wc_changes[dataset.path] = status

    if not wc_changes:
        click.echo("\nNothing to commit, working copy clean")
    else:
        click.echo(
            (
                "\nChanges in working copy:\n"
                '  (use "sno commit" to commit)\n'
                '  (use "sno reset" to discard changes)\n'
            )
        )
        click.echo(get_diff_status_message(wc_changes))


def get_branch_status_message(repo):
    commit = repo.head.peel(pygit2.Commit)

    branch = repo.branches[repo.head.shorthand]
    message = [f"On branch {branch.shorthand}"]

    if branch.upstream:
        upstream_head = branch.upstream.peel(pygit2.Commit)
        n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)
        if n_ahead == n_behind == 0:
            message += [
                f"\nYour branch is up to date with '{branch.upstream.shorthand}'."
            ]
        elif n_ahead > 0 and n_behind > 0:
            message += [
                f"Your branch and '{branch.upstream.shorthand}' have diverged,"
                f"and have {n_ahead} and {n_behind} different commits each, respectively.",
                '  (use "sno pull" to merge the remote branch into yours)',
            ]
        elif n_ahead > 0:
            message += [
                f"Your branch is ahead of '{branch.upstream.shorthand}' by {n_ahead} {_pc(n_ahead)}.",
                '  (use "sno push" to publish your local commits)',
            ]
        elif n_behind > 0:
            message += [
                f"Your branch is behind '{branch.upstream.shorthand}' by {n_behind} {_pc(n_behind)}, "
                "and can be fast-forwarded.",
                '  (use "sno pull" to update your local branch)',
            ]

    return "\n".join(message)


def get_diff_status_message(wc_changes):
    message = []
    for dataset_path, status in wc_changes.items():
        if sum(status.values()):
            message.append(f"  {dataset_path}/")
            if status["META"]:
                message.append(f"    meta")
            if status["U"]:
                message.append(f"    modified:  {status['U']} {_pf(status['U'])}")
            if status["I"]:
                message.append(f"    new:       {status['I']} {_pf(status['I'])}")
            if status["D"]:
                message.append(f"    deleted:   {status['D']} {_pf(status['D'])}")
    return "\n".join(message)


def _pf(count):
    """ Simple pluraliser for feature/features """
    if count == 1:
        return "feature"
    else:
        return "features"


def _pc(count):
    """ Simple pluraliser for commit/commits """
    if count == 1:
        return "commit"
    else:
        return "commits"
