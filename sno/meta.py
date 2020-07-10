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
@click.argument('dataset')
@click.argument('keys', required=False, nargs=-1)
@click.pass_context
def meta_get(ctx, output_format, json_style, dataset, keys):
    """
    Prints the value of meta keys for the given dataset.
    """
    rs = RepositoryStructure(ctx.obj.repo)

    try:
        ds = rs[dataset]
    except KeyError:
        raise click.UsageError(f"No such dataset: {dataset}")

    if keys:
        items = {}
        missing_keys = []
        for key in keys:
            try:
                items[key] = ds.get_meta_item(key)
            except KeyError:
                missing_keys.append(key)

        if missing_keys:
            raise click.UsageError(
                f"Couldn't find items: {', '.join(sorted(missing_keys))}"
            )
    else:
        items = dict(ds.iter_meta_items())

    fp = resolve_output_path('-')
    if output_format == 'text':
        indent = '    '
        for key, value in items.items():
            click.secho(key, bold=True)
            serialized = format_json_for_output(value, fp, json_style=json_style)
            lines = serialized.splitlines()
            for i, line in enumerate(lines):
                fp.write(f"{indent}{line}\n")
    else:
        dump_json_output(items, fp, json_style=json_style)
