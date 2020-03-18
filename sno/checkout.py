import os
from pathlib import Path

import click
import pygit2

from .structure import RepositoryStructure
from .working_copy import WorkingCopy


@click.command()
@click.pass_context
@click.option("branch", "-b", help="Name for new branch")
@click.option("fmt", "--format", type=click.Choice(["GPKG"]), default="GPKG")
@click.option("--force", "-f", is_flag=True)
@click.option("--path", type=click.Path(writable=True, dir_okay=False))
@click.option("datasets", "--dataset", "-d", multiple=True)
@click.argument("refish", default=None, required=False)
def checkout(ctx, branch, fmt, force, path, datasets, refish):
    """ Switch branches or restore working tree files """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    # refish could be:
    # - branch name
    # - tag name
    # - remote branch
    # - HEAD
    # - HEAD~1/etc
    # - 'c0ffee' commit ref
    # - 'refs/tags/1.2.3' some other refspec

    base_commit = repo.head.peel(pygit2.Commit)
    head_ref = None

    if refish:
        commit, ref = repo.resolve_refish(refish)
        head_ref = ref.name if ref else commit.id
    else:
        commit = base_commit
        head_ref = repo.head.name

    if branch:
        if branch in repo.branches:
            raise click.BadParameter(
                f"A branch named '{branch}' already exists.", param_hint="branch"
            )

        if refish and refish in repo.branches.remote:
            print(f"Creating new branch '{branch}' to track '{refish}'...")
            new_branch = repo.create_branch(branch, commit, force)
            new_branch.upstream = repo.branches.remote[refish]
        elif refish and refish in repo.branches:
            print(f"Creating new branch '{branch}' from '{refish}'...")
            new_branch = repo.create_branch(branch, commit, force)
        else:
            print(f"Creating new branch '{branch}'...")
            new_branch = repo.create_branch(branch, commit, force)

        head_ref = new_branch.name

    repo_structure = RepositoryStructure(repo)

    repo.set_head(head_ref)

    wc = repo_structure.working_copy
    if wc:
        if path is not None:
            raise click.ClickException(
                f"This repository already has a working copy at: {wc.path}",
            )

        click.echo(f"Updating {wc.path} ...")
        print(f"commit={commit.id} head_ref={head_ref}")
        wc.reset(commit, repo_structure, force=force)

        if not repo.head_is_detached:
            repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)

    else:
        if path is None:
            path = f"{repo_path.resolve().stem}.gpkg"

        # new working-copy path
        click.echo(f'Checkout {refish or "HEAD"} to {path} as {fmt} ...')
        repo.reset(commit.id, pygit2.GIT_RESET_SOFT)

        checkout_new(repo_structure, path, datasets=datasets, commit=commit)


def checkout_new(repo_structure, path, *, datasets=None, commit=None):
    if not datasets:
        datasets = list(repo_structure)
    else:
        datasets = [repo_structure[ds_path] for ds_path in datasets]

    if not commit:
        commit = repo_structure.repo.head.peel(pygit2.Commit)

    click.echo(f"Commit: {commit.hex}")

    wc = WorkingCopy.new(repo_structure.repo, path)
    wc.create()
    for dataset in datasets:
        wc.write_full(commit, dataset, safe=False)
    wc.save_config()


@click.command()
@click.pass_context
@click.option("--create", "-c", help="Create a new branch")
@click.option("--force-create", "-C", help="Similar to --create except that if <new-branch> already exists, it will be reset to <start-point>")
@click.option("--discard-changes", is_flag=True, help="Discard local changes")
@click.argument("refish", default=None, required=False)
def switch(ctx, create, force_create, discard_changes, refish):
    """
    Switch branches

    Switch to a specified branch. The working copy and the index are updated
    to match the branch. All new commits will be added to the tip of this
    branch.

    Optionally a new branch could be created with either -c, -C, automatically
    from a remote branch of same name.

    REFISH is either the branch name to switch to, or start-point of new branch for -c/--create.
    """
    from .structure import RepositoryStructure

    repo = ctx.obj.repo

    if create and force_create:
        raise click.BadParameter("-c/--create and -C/--force-create are incompatible")

    elif create or force_create:
        # New Branch
        new_branch = force_create or create
        is_force = bool(force_create)

        # refish could be:
        # - branch name
        # - tag name
        # - remote branch
        # - HEAD
        # - HEAD~1/etc
        # - 'c0ffee' commit ref
        # - 'refs/tags/1.2.3' some other refspec
        start_point = refish
        if start_point:
            commit, ref = repo.resolve_refish(start_point)
        else:
            commit = repo.head.peel(pygit2.Commit)

        if new_branch in repo.branches and not force_create:
            raise click.BadParameter(
                f"A branch named '{new_branch}' already exists.", param_hint="create"
            )

        if start_point and start_point in repo.branches.remote:
            print(f"Creating new branch '{new_branch}' to track '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
            b_new.upstream = repo.branches.remote[start_point]
        elif start_point and start_point in repo.branches:
            print(f"Creating new branch '{new_branch}' from '{start_point}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)
        else:
            print(f"Creating new branch '{new_branch}'...")
            b_new = repo.create_branch(new_branch, commit, is_force)

        head_ref = b_new.name

    else:
        # Switch to existing branch
        #
        # refish could be:
        # - branch name
        try:
            branch = repo.branches[refish]
        except KeyError:
            raise click.BadParameter(
                f"Branch '{refish}' not found."
            )

        commit = branch.peel(pygit2.Commit)
        head_ref = branch.name

    repo.set_head(head_ref)

    repo_structure = RepositoryStructure(repo)
    working_copy = repo_structure.working_copy
    if working_copy:
        click.echo(f"Updating {working_copy.path} ...")
        working_copy.reset(commit, repo_structure, force=discard_changes)

    repo.reset(commit.oid, pygit2.GIT_RESET_SOFT)


@click.command()
@click.pass_context
@click.option(
    "--source", "-s",
    help=(
        "Restore the working tree files with the content from the given tree. "
        "It is common to specify the source tree by naming a commit, branch or "
        "tag associated with it."),
    default="HEAD"
)
@click.argument("pathspec", nargs=-1)
def restore(ctx, source, pathspec):
    """
    Restore specified paths in the working tree with some contents from a restore source.
    """
    from .structure import RepositoryStructure

    repo = ctx.obj.repo

    repo_structure = RepositoryStructure(repo)
    working_copy = repo_structure.working_copy
    if not working_copy:
        raise click.ClickException("You don't have a working copy")

    head_commit = repo.head.peel(pygit2.Commit)

    commit, ref = repo.resolve_refish(source)

    working_copy.reset(
        commit,
        repo_structure,
        force=True,
        update_meta=(head_commit.id == commit.id),
        paths=pathspec
    )


@click.command("workingcopy-set-path")
@click.pass_context
@click.argument("new", nargs=1, type=click.Path(exists=True, dir_okay=False))
def workingcopy_set_path(ctx, new):
    """ Change the path to the working-copy """
    repo_path = ctx.obj.repo_path
    repo = ctx.obj.repo

    repo_cfg = repo.config
    if "sno.workingcopy.path" not in repo_cfg:
        raise click.ClickException("No working copy? Try `sno checkout`")

    new = Path(new)
    # TODO(olsen): This doesn't seem to do anything?
    if not new.is_absolute():
        new = os.path.relpath(os.path.join(repo_path, new), repo_path)

    repo.config["sno.workingcopy.path"] = str(new)
