import contextlib
import json
from datetime import datetime, timezone, timedelta
from io import StringIO

import click
import pygit2

from .cli_util import MutexOption
from .exceptions import NotFound, NO_COMMIT
from .output_util import dump_json_output, resolve_output_path
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz
from . import diff


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
@click.argument("refish", default='HEAD', required=False)
def show(ctx, *, refish, output_format, **kwargs):
    """
    Show the given commit, or HEAD
    """
    # Ensure we were given a reference to a commit, and not a tree or something
    repo = ctx.obj.repo
    try:
        obj = repo.revparse_single(refish)
        obj.peel(pygit2.Commit)
    except (KeyError, pygit2.InvalidSpecError):
        raise NotFound(f"{refish} is not a commit", exit_code=NO_COMMIT)

    patch_writer = globals()[f"patch_output_{output_format}"]

    return diff.diff_with_writer(
        ctx, patch_writer, exit_code=False, args=[f"{refish}^..{refish}"],
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
def patch_output_json(*, target, output_path, **kwargs):
    """
    Contextmanager.

    Same arguments and usage as `patch_output_text`; see that docstring for usage.

    On exit, writes the patch as JSON to the given output file.
    If the output file is stdout and isn't piped anywhere,
    the json is prettified first.

    The patch JSON contains two top-level keys:
        "sno.diff/v1": contains a JSON diff. See `sno.diff.diff_output_json` docstring.
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
    with diff.diff_output_json(output_path=output_path, **kwargs) as diff_writer:
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

    dump_json_output(output, original_output_path)
