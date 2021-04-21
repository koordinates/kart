import json
import io

import click
import pygit2

from .apply import apply_patch
from .cli_util import (
    StringFromFile,
    add_help_subcommand,
    value_optionally_from_text_file,
    value_optionally_from_binary_file,
)
from .checkout import reset_wc_if_needed
from .exceptions import InvalidOperation, NotYetImplemented, NotFound, NO_CHANGES
from .rich_tree_builder import RichTreeBuilder
from .output_util import (
    dump_json_output,
    format_json_for_output,
    format_wkt_for_output,
    write_with_indent,
    resolve_output_path,
    wrap_text_to_terminal,
)

# Changing these items would generally break the repo;
# we disallow that.
READONLY_ITEMS = {
    "primary_key",
    "sqlite_table_info",
    "fields",
}


@add_help_subcommand
@click.group()
@click.pass_context
def meta(ctx, **kwargs):
    """
    Read and update meta values for a dataset.
    """


@meta.command(name="get")
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json"]),
    default="text",
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the JSON output. Only used with -o json",
)
@click.option("--ref", default="HEAD")
@click.argument("dataset", required=False)
@click.argument("keys", required=False, nargs=-1)
@click.pass_context
def meta_get(ctx, output_format, json_style, ref, dataset, keys):
    """
    Prints the value of meta keys.

    Optionally, output can be filtered to a dataset and a particular key.
    """
    repo = ctx.obj.repo

    if dataset:
        try:
            datasets = [repo.datasets(ref)[dataset]]
        except KeyError:
            raise click.UsageError(f"No such dataset: {dataset}")
    else:
        datasets = repo.datasets(ref)

    fp = resolve_output_path("-")

    all_items = {}
    for ds in datasets:
        if keys:
            all_items[ds.path] = get_meta_items(ds, keys)
        else:
            all_items[ds.path] = dict(ds.meta_items())

    if output_format == "text":
        for ds_path, items in all_items.items():
            click.secho(ds_path, bold=True)
            for key, value in items.items():
                click.secho(f"    {key}", bold=True)
                value_indent = "        "
                if key.endswith(".json") or not isinstance(value, str):
                    value = format_json_for_output(value, fp, json_style=json_style)
                    write_with_indent(fp, value, indent=value_indent)
                elif key.endswith(".wkt"):
                    value = format_wkt_for_output(value, fp)
                    write_with_indent(fp, value, indent=value_indent)
                else:
                    fp.write(wrap_text_to_terminal(value, indent=value_indent))
    else:
        dump_json_output(all_items, fp, json_style=json_style)


def get_meta_items(ds, keys):
    items = {}
    for key in keys:
        try:
            items[key] = ds.get_meta_item(key)
        except KeyError:
            pass

    missing_keys = list(keys - items.keys())
    if missing_keys:
        raise click.UsageError(
            f"Couldn't find items: {', '.join(sorted(missing_keys))}"
        )
    return items


class KeyValueType(click.ParamType):
    name = "key=value"

    def convert(self, value, param, ctx):
        value = tuple(value.split("=", 1))
        if len(value) != 2:
            self.fail(f"{value} should be of the form KEY=VALUE", param, ctx)

        key, value = value
        if not key:
            self.fail(f"Key should not be empty", param, ctx)

        return key, value


@meta.command(name="set")
@click.option(
    "--message",
    "-m",
    help="Use the given message as the commit message",
    type=StringFromFile(encoding="utf-8"),
)
@click.argument("dataset")
@click.argument(
    "items",
    type=KeyValueType(),
    required=True,
    nargs=-1,
    metavar="KEY=VALUE [KEY=VALUE...]",
)
@click.pass_context
def meta_set(ctx, message, dataset, items):
    """
    Sets multiple meta items for a dataset, and creates a commit.
    """
    repo = ctx.obj.repo

    if repo.version < 2:
        raise InvalidOperation(
            "This repo doesn't support meta changes, use `kart upgrade`"
        )

    if message is None:
        message = f"Update metadata for {dataset}"

    def _parse(key, value):
        value = value_optionally_from_text_file(value, key, ctx, encoding="utf-8")
        if key.endswith(".json"):
            try:
                return json.loads(value)
            except json.decoder.JSONDecodeError as e:
                raise click.BadParameter(f"{key} is not valid JSON:\n{e}")
        else:
            return value

    patch = {
        "kart.diff/v1+hexwkb": {
            dataset: {
                "meta": {key: {"+": _parse(key, value)} for (key, value) in items}
            }
        },
        "kart.patch/v1": {"message": message},
    }
    patch_file = io.StringIO()
    json.dump(patch, patch_file)
    patch_file.seek(0)
    apply_patch(
        repo=ctx.obj.repo,
        do_commit=True,
        patch_file=patch_file,
        allow_empty=False,
        allow_missing_old_values=True,
    )


@click.command("commit-files", hidden=True)
@click.option(
    "--message",
    "-m",
    required=False,
    help="Use the given message as the commit message",
    type=StringFromFile(encoding="utf-8"),
)
@click.option("--ref", default="HEAD")
@click.option(
    "--amend",
    default=False,
    is_flag=True,
    help="Amend the previous commit instead of adding a new commit",
)
@click.option(
    "--remove-empty-files",
    default=False,
    is_flag=True,
    help="If the content of any files specified is empty, remove the file.",
)
@click.option(
    "--allow-empty",
    is_flag=True,
    default=False,
    help=(
        "Usually recording a commit that has the exact same tree as its sole "
        "parent commit is a mistake, and the command prevents you from making "
        "such a commit. This option bypasses the safety"
    ),
)
@click.argument(
    "items",
    type=KeyValueType(),
    required=False,
    nargs=-1,
    metavar="KEY=VALUE [KEY=VALUE...]",
)
@click.pass_context
def commit_files(ctx, message, ref, amend, allow_empty, remove_empty_files, items):
    """Usage: kart commit-files -m MESSAGE KEY=VALUE [KEY=VALUE...]"""
    repo = ctx.obj.repo
    ctx.obj.check_not_dirty()

    if not message and not amend:
        raise click.UsageError("Aborting commit due to empty commit message.")

    if ref == "HEAD":
        parent_commit = repo.head_commit
    else:
        parent_commit = repo.references[ref].peel(pygit2.Commit)

    if not parent_commit:
        raise NotYetImplemented(
            "Sorry, using `kart commit-files` to create the initial commit is not yet supported"
        )

    if amend and not message:
        message = parent_commit.message

    original_tree = parent_commit.peel(pygit2.Tree)
    tree_builder = RichTreeBuilder(repo, original_tree)
    for key, value in items:
        value = value_optionally_from_binary_file(value, key, ctx, encoding="utf-8")
        if remove_empty_files and not value:
            tree_builder.remove(key)
        else:
            tree_builder.insert(key, value)

    new_tree = tree_builder.flush()
    if new_tree == original_tree and not amend and not allow_empty:
        raise NotFound("No changes to commit", exit_code=NO_CHANGES)

    click.echo("Committing...")
    parents = (
        [parent_commit.id] if not amend else [gp.id for gp in parent_commit.parents]
    )
    commit_to_ref = ref if not amend else None
    author = repo.author_signature() if not amend else parent_commit.author

    # This will also update the ref (branch) to point to the new commit,
    # (if commit_to_ref is not None).
    new_commit_id = repo.create_commit(
        commit_to_ref,
        author,
        repo.committer_signature(),
        message,
        new_tree.id,
        parents,
    )
    new_commit = repo[new_commit_id]

    if amend:
        if ref == "HEAD" and repo.head_branch is None:
            repo.head.set_target(new_commit.id)
        elif ref == "HEAD" and repo.head_branch is not None:
            repo.references[repo.head_branch].set_target(new_commit.id)
        else:
            repo.references[ref].set_target(new_commit.id)

    click.echo(f"Committed as: {new_commit.hex}")
    reset_wc_if_needed(repo, new_commit)
