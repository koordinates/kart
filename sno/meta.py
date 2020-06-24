import click

from .output_util import dump_json_output, format_json_for_output, resolve_output_path
from .structure import RepositoryStructure

# Changing these items would generally break the repo;
# we disallow that.
READONLY_ITEMS = {
    'primary_key',
    'sqlite_table_info',
    'fields',
}


@click.group()
@click.pass_context
def meta(ctx, **kwargs):
    """
    Read and update meta values for a dataset.
    """


@meta.command(name='get')
@click.option(
    "--output-format", "-o", type=click.Choice(["text", "json"]), default="text",
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the JSON output. Only used with -o json",
)
@click.option(
    "--include-readonly/--exclude-readonly",
    is_flag=True,
    default=True,
    help="Include readonly meta items",
)
@click.argument('dataset')
@click.argument('keys', required=False, nargs=-1)
@click.pass_context
def meta_get(ctx, output_format, json_style, include_readonly, dataset, keys):
    """
    Prints the value of meta keys for the given dataset.
    If no keys are given, all available values are printed.
    """

    rs = RepositoryStructure(ctx.obj.repo)

    try:
        ds = rs[dataset]
    except KeyError:
        raise click.UsageError(f"No such dataset: {dataset}")

    exclude = () if include_readonly else READONLY_ITEMS
    items = ds.iter_meta_items(exclude=exclude)
    if keys:
        items = [(k, v) for (k, v) in items if k in keys]
        if len(items) != len(keys):
            missing_keys = set(keys) - set(dict(items).keys())
            raise click.UsageError(
                f"Couldn't find items: {', '.join(sorted(missing_keys))}"
            )

    fp = resolve_output_path('-')
    if output_format == 'text':
        indent = '    '
        for k, value in items:
            click.secho(k, bold=True)
            serialized = format_json_for_output(value, fp, json_style=json_style)
            lines = serialized.splitlines()
            for i, line in enumerate(lines):
                fp.write(f"{indent}{line}\n")
    else:
        dump_json_output(dict(items), fp, json_style=json_style)
