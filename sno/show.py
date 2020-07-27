import contextlib
from datetime import datetime, timezone, timedelta

import click

from .repo_files import RepoState
from .output_util import dump_json_output, resolve_output_path
from .structs import CommitWithReference
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz
from . import diff


EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


@click.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["text", "json", "patch"]),
    default="text",
    help="Output format. 'patch' is a synonym for 'json'",
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with --output-format=json",
)
@click.argument("refish", default='HEAD', required=False)
def show(ctx, *, refish, output_format, json_style, **kwargs):
    """
    Show the given commit, or HEAD
    """
    if output_format == 'patch':
        output_format = 'json'

    repo = ctx.obj.get_repo(allowed_states=RepoState.ALL_STATES)
    # Ensures we were given a reference to a commit, and not a tree or something
    commit = CommitWithReference.resolve(repo, refish).commit

    try:
        parents = commit.parents
    except KeyError:
        # one or more parents doesn't exist.
        # This is okay if this is the first commit of a shallow clone.
        # (how to tell?)
        parent = EMPTY_TREE_SHA
    else:
        if parents:
            parent = f"{refish}^"
        else:
            parent = EMPTY_TREE_SHA
    patch_writer = globals()[f"patch_output_{output_format}"]

    return diff.diff_with_writer(
        ctx,
        patch_writer,
        exit_code=False,
        commit_spec=f"{parent}...{refish}",
        filters=[],
        json_style=json_style,
    )


@contextlib.contextmanager
def patch_output_text(*, target, output_path, **kwargs):
    """
    Contextmanager.

    Arguments:
        target: a pygit2.Commit instance to show a patch for
        output_path:   where the output should go; a path, file-like object or '-'

    All other kwargs are passed to sno.diff.diff_output_text.

    Yields a callable which can be called with dataset diffs.
    The callable takes two arguments:
        dataset: A sno.structure.DatasetStructure instance representing
                 either the old or new version of the dataset.
        diff:    The sno.diff.Diff instance to serialize

    On exit, writes a human-readable patch as text to the given output file.

    This patch may not be apply-able; it is intended for human readability.
    In particular, geometry WKT is abbreviated and null values are represented
    by a unicode "␀" character.
    """
    commit = target.head_commit
    fp = resolve_output_path(output_path)
    pecho = {'file': fp, 'color': fp.isatty()}
    with diff.diff_output_text(output_path=fp, **kwargs) as diff_writer:
        author = commit.author
        author_time_utc = datetime.fromtimestamp(author.time, timezone.utc)
        author_timezone = timezone(timedelta(minutes=author.offset))
        author_time_in_author_timezone = author_time_utc.astimezone(author_timezone)

        click.secho(f'commit {commit.hex}', fg='yellow')
        click.secho(f'Author: {author.name} <{author.email}>', **pecho)
        click.secho(
            f'Date:   {author_time_in_author_timezone.strftime("%c %z")}', **pecho
        )
        click.secho(**pecho)
        for line in commit.message.splitlines():
            click.secho(f'    {line}', **pecho)
        click.secho(**pecho)
        yield diff_writer


@contextlib.contextmanager
def patch_output_json(*, target, output_path, json_style, **kwargs):
    """
    Contextmanager.

    Same arguments and usage as `patch_output_text`; see that docstring for usage.

    On exit, writes the patch as JSON to the given output file.
    If the output file is stdout and isn't piped anywhere,
    the json is prettified first.

    The patch JSON contains two top-level keys:
        "sno.diff/v1+hexwkb": contains a JSON diff. See `sno.diff.diff_output_json` docstring.
        "sno.patch/v1": contains metadata about the commit this patch represents:
          {
            "authorEmail": "joe@example.com",
            "authorName": "Joe Bloggs",
            "authorTime": "2020-04-15T01:19:16Z",
            "authorTimeOffset": "+12:00",
            "message": "Commit title\n\nThis commit makes some changes\n"
          }

    authorTime is always returned in UTC, in Z-suffixed ISO8601 format.
    """

    commit = target.head_commit
    author = commit.author
    author_time = datetime.fromtimestamp(author.time, timezone.utc)
    author_time_offset = timedelta(minutes=author.offset)

    def dump_function(data, *args, **kwargs):
        data['sno.patch/v1'] = {
            'authorName': author.name,
            'authorEmail': author.email,
            "authorTime": datetime_to_iso8601_utc(author_time),
            "authorTimeOffset": timedelta_to_iso8601_tz(author_time_offset),
            "message": commit.message,
        }
        dump_json_output(data, *args, **kwargs)

    with diff.diff_output_json(
        output_path=output_path,
        json_style=json_style,
        dump_function=dump_function,
        **kwargs,
    ) as diff_writer:
        yield diff_writer
