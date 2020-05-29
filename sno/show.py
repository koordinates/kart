import contextlib
import json
from datetime import datetime, timezone, timedelta
from io import StringIO

import click

from .cli_util import MutexOption
from .output_util import dump_json_output, resolve_output_path
from .structs import CommitWithReference
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz
from . import diff


EMPTY_TREE_SHA = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"


@click.command()
@click.pass_context
@click.option(
    "--text",
    "output_format",
    flag_value="text",
    default=True,
    help="Show commit in text format",
    cls=MutexOption,
    exclusive_with=["json"],
)
@click.option(
    "--json",
    "--patch",
    "-p",
    "output_format",
    flag_value="json",
    help="Show commit in JSON patch format",
    cls=MutexOption,
    exclusive_with=["text"],
)
@click.option(
    "--json-style",
    type=click.Choice(["extracompact", "compact", "pretty"]),
    default="pretty",
    help="How to format the output. Only used with --json",
    cls=MutexOption,
    exclusive_with=["text"],
)
@click.argument("refish", default='HEAD', required=False)
def show(ctx, *, refish, output_format, json_style, **kwargs):
    """
    Show the given commit, or HEAD
    """
    repo = ctx.obj.repo
    # Ensures we were given a reference to a commit, and not a tree or something
    commit = CommitWithReference.resolve(repo, refish).commit

    if commit.parents:
        parent = f"{refish}^"
    else:
        parent = EMPTY_TREE_SHA
    patch_writer = globals()[f"patch_output_{output_format}"]

    return diff.diff_with_writer(
        ctx,
        patch_writer,
        exit_code=False,
        args=[f"{parent}..{refish}"],
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
    with resolve_output_path(output_path) as fp:
        pecho = {'file': fp}
        with diff.diff_output_text(output_path=fp, **kwargs) as diff_writer:
            author = commit.author
            author_time_utc = datetime.fromtimestamp(author.time, timezone.utc)
            author_timezone = timezone(timedelta(minutes=author.offset))
            author_time_in_author_timezone = author_time_utc.astimezone(author_timezone)

            click.secho(f'commit {commit.hex}', fg='yellow', **pecho)
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
    buf = StringIO()

    output_path, original_output_path = buf, output_path
    with diff.diff_output_json(
        output_path=output_path, json_style=json_style, **kwargs
    ) as diff_writer:
        yield diff_writer

    # At this point, the diff_writer has been used, meaning the StringIO has
    # the diff output in it. Now we can add some patch info
    buf.seek(0)
    output = json.load(buf)
    commit = target.head_commit
    author = commit.author
    author_time = datetime.fromtimestamp(author.time, timezone.utc)
    author_time_offset = timedelta(minutes=author.offset)

    output['sno.patch/v1'] = {
        'authorName': author.name,
        'authorEmail': author.email,
        "authorTime": datetime_to_iso8601_utc(author_time),
        "authorTimeOffset": timedelta_to_iso8601_tz(author_time_offset),
        "message": commit.message,
    }

    dump_json_output(output, original_output_path, json_style=json_style)
