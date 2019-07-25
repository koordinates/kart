import click
import pygit2

from . import core, gpkg


@click.command()
@click.pass_context
def status(ctx):
    """ Show the working copy status """
    repo_dir = ctx.obj["repo_dir"] or "."
    repo = pygit2.Repository(repo_dir)
    if not repo or not repo.is_bare:
        raise click.BadParameter("Not an existing repository", param_hint="--repo")

    commit = repo.head.peel(pygit2.Commit)

    if repo.head_is_detached:
        click.echo(f"{click.style('HEAD detached at', fg='red')} {commit.short_id}")
    else:
        branch = repo.branches[repo.head.shorthand]
        click.echo(f"On branch {branch.shorthand}")

        if branch.upstream:
            upstream_head = branch.upstream.peel(pygit2.Commit)
            n_ahead, n_behind = repo.ahead_behind(commit.id, upstream_head.id)
            if n_ahead == n_behind == 0:
                click.echo(
                    f"Your branch is up to date with '{branch.upstream.shorthand}'."
                )
            elif n_ahead > 0 and n_behind > 0:
                click.echo(
                    (
                        f"Your branch and '{branch.upstream.shorthand}' have diverged,\n"
                        f"and have {n_ahead} and {n_behind} different commits each, respectively.\n"
                        '  (use "snow pull" to merge the remote branch into yours)'
                    )
                )
            elif n_ahead > 0:
                click.echo(
                    (
                        f"Your branch is ahead of '{branch.upstream.shorthand}' by {n_ahead} {_pc(n_ahead)}.\n"
                        '  (use "snow push" to publish your local commits)'
                    )
                )
            elif n_behind > 0:
                click.echo(
                    (
                        f"Your branch is behind '{branch.upstream.shorthand}' by {n_behind} {_pc(n_behind)}, "
                        "and can be fast-forwarded.\n"
                        '  (use "snow pull" to update your local branch)'
                    )
                )

    # working copy state
    working_copy = core.get_working_copy(repo)
    if not working_copy:
        click.echo(
            '\nNo working copy.\n  (use "snow checkout" to create a working copy)'
        )
        return

    db = gpkg.db(working_copy.path, isolation_level="DEFERRED")
    with db:
        dbcur = db.cursor()

        sql = """
            SELECT state, COUNT(feature_key) AS mod, COUNT(*) AS count
            FROM __kxg_map
            WHERE
                table_name = ?
                AND state != 0
                AND NOT (feature_key IS NULL AND state < 0)  -- ignore INSERT then DELETE
            GROUP BY state;
        """
        dbcur.execute(sql, [working_copy.layer])
        change_counts = {
            r["state"]: (r["mod"], r["count"])
            for r in dbcur.fetchall()
            if r["state"] is not None
        }

        # TODO: check meta/ tree

        if not change_counts:
            click.echo("\nNothing to commit, working copy clean")
        else:
            click.echo(
                (
                    "\nChanges in working copy:\n"
                    '  (use "snow commit" to commit)\n'
                    '  (use "snow reset" to discard changes)\n'
                )
            )

            if 1 in change_counts:
                n_mod = change_counts[1][0]
                n_add = change_counts[1][1] - n_mod
                if n_mod:
                    click.echo(f"    modified:   {n_mod} {_pf(n_mod)}")
                if n_add:
                    click.echo(f"    new:        {n_add} {_pf(n_add)}")

            if -1 in change_counts:
                n_del = change_counts[-1][1]
                click.echo(f"    deleted:    {n_del} {_pf(n_del)}")


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
