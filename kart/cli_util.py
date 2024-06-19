import io
import json
import logging
import os
from pathlib import Path
import platform
import shutil
import signal
import sys


import click
from click.core import Argument
from click.shell_completion import CompletionItem

from kart import subprocess_util as subprocess

L = logging.getLogger("kart.cli_util")


class KartCommand(click.Command):
    def parse_args(self, ctx, args):
        ctx.unparsed_args = list(args)
        super().parse_args(ctx, args)

    def format_help(self, ctx, formatter):
        try:
            render(ctx.command_path)
        except Exception as e:
            L.debug(f"Failed rendering help page: {e}")
            return super().format_help(ctx, formatter)


class KartGroup(click.Group):
    command_class = KartCommand

    def get_command(self, ctx, cmd_name):
        rv = super().get_command(ctx, cmd_name)
        if rv is not None:
            return rv

        # typo? Suggest similar commands.
        import difflib

        matches = difflib.get_close_matches(
            cmd_name, list(self.list_commands(ctx)), n=3
        )

        fail_message = f"kart: '{cmd_name}' is not a kart command. See 'kart --help'.\n"
        if matches:
            if len(matches) == 1:
                fail_message += "\nThe most similar command is\n"
            else:
                fail_message += "\nThe most similar commands are\n"
            for m in matches:
                fail_message += f"\t{m}\n"
        ctx.fail(fail_message)

    def invoke(self, ctx):
        if ctx.params.get("post_mortem"):
            try:
                return super().invoke(ctx)
            except Exception:
                try:
                    import ipdb as pdb
                except ImportError:
                    # ipdb is only installed in dev venvs, not releases
                    import pdb
                pdb.post_mortem()
                raise
        else:
            return super().invoke(ctx)

    def format_help(self, ctx, formatter):
        try:
            render(ctx.command_path)
        except Exception as e:
            return super().format_help(ctx, formatter)


def render(command_path: str):
    """Sends output to pager depending on current platform"""
    if platform.system() == "Windows":
        return render_windows(command_path)

    return render_posix(command_path)


def render_posix(command_path: str) -> None:
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise click.ClickException("Not a tty, printing raw help.")

    from kart import prefix

    man_page = Path(prefix) / "help" / f'{command_path.replace(" ", "-")}.1'
    if not man_page.exists():
        raise FileNotFoundError(f"{man_page} not found at given path")
    cmdline = ["man", str(man_page)]
    if not shutil.which(cmdline[0]):
        raise click.ClickException(
            f"{cmdline[0]} not found in PATH, printing raw help."
        )
    L.debug("Running command: %s", cmdline)

    # When launching `man`, we have to ignore SIGINT - if we do our usual kill everything on SIGINT, it messes up the terminal.
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    # This is a signal to the kart-helper-caller (if there is one) to ignore SIGINT too:
    kart_caller_pid = os.environ.get("KART_CALLER_PID")
    if kart_caller_pid:
        os.kill(int(kart_caller_pid), signal.SIGUSR1)

    p = subprocess.Popen(cmdline)
    p.communicate()


def render_windows(command_path: str) -> bytes:
    from kart import prefix

    text_page = Path(prefix) / "help" / f'{command_path.replace(" ", "-")}'
    if not text_page.exists():
        raise FileNotFoundError(f"{text_page} not found at given path")
    click.echo_via_pager(text_page.read_text())


def add_help_subcommand(group):
    @group.command(add_help_option=False, hidden=True)
    @click.argument("topic", default=None, required=False, nargs=1)
    @click.pass_context
    def help(ctx, topic, **kw):
        # https://www.burgundywall.com/post/having-click-help-subcommand
        if topic is None:
            click.echo(ctx.parent.get_help())
        else:
            try:
                command_path = " ".join(ctx.command_path.split()[:-1])
                render(f"{command_path} {topic}")
            except Exception:
                click.echo(group.get_command(ctx, topic).get_help(ctx))

    return group


class MutexOption(click.Option):
    """
    Mutually exclusive options
    Source: merge of solutions from https://github.com/pallets/click/issues/257

    Usage:
        @click.group()
        @click.option("--username", prompt=True, cls=MutexOption, exclusive_with=["token"])
        @click.option("--password", prompt=True, hide_input=True, cls=MutexOption, exclusive_with=["token"])
        @click.option("--token", cls=MutexOption, exclusive_with=["username","password"])
        def login(ctx=None, username:str=None, password:str=None, token:str=None) -> None:
            print("...do what you like with the params you got...")
    """

    def __init__(self, *args, **kwargs):
        self.exclusive_with: list = kwargs.pop("exclusive_with")

        assert self.exclusive_with, "'exclusive_with' parameter required"
        kwargs["help"] = (
            kwargs.get("help", "")
            + "\nOption is mutually exclusive with "
            + ", ".join(self.exclusive_with)
            + "."
        ).strip()
        super().__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        current_opt: bool = self.name in opts
        for other_name in self.exclusive_with:
            if other_name in opts:
                if current_opt:
                    other = [x for x in ctx.command.params if x.name == other_name][0]
                    if not other.value_is_missing(opts[other_name]):
                        raise click.UsageError(
                            f"Illegal usage: {self.get_error_hint(ctx)} "
                            f"is mutually exclusive with {other.get_error_hint(ctx)}."
                        )
                else:
                    self.prompt = None
        return super().handle_parse_result(ctx, opts, args)


class StringFromFile(click.types.StringParamType):
    """
    Like a regular string option, but if the string starts with '@',
    the string will actually be read from the filename that follows.

    The special value "-" is a synonym for "@-", i.e. read from stdin.

    Usage:
        --param=value
            --> "value"
        --param=@filename.txt
            --> "This is the contents of filename.txt"
        --param=-
            --> "This is the contents of stdin"
    """

    def __init__(self, **file_kwargs):
        self.file_kwargs = file_kwargs

    def convert(self, value, param, ctx, as_file=False):
        value = super().convert(value, param, ctx)
        return value_optionally_from_text_file(
            value, param, ctx, as_file=as_file, **self.file_kwargs
        )


class IdsFromFile(StringFromFile):
    """Like StringFromFile, but returns a generator that yields an ID for each line of the file."""

    name = "ids"

    def convert(self, value, param, ctx):
        fp = super().convert(
            value,
            param,
            ctx,
            # Get the file object, so we don't have to read the whole thing
            as_file=True,
        )
        return (line.rstrip("\n") for line in fp)


def _resolve_file(path):
    return str(Path(path).expanduser())


def value_optionally_from_text_file(value, param, ctx, as_file=False, **file_kwargs):
    """
    Given a string, interprets it either as:
    * a filename prefixed with '@', and returns the contents of the file
    * just a string, and returns the string itself.

    By default, returns the value as a string.
    If as_file=True, returns a StringIO or a file object. Use this when dealing with
    large files to save memory.
    """
    if isinstance(param, str):
        # Not a real param, just a "param_hint". Make an equivalent param.
        param = Argument(param_decls=[param])

    if value == "-" or value.startswith("@"):
        filetype = click.File(**file_kwargs)
        filename = _resolve_file(value[1:]) if value.startswith("@") else value
        fp = filetype.convert(filename, param, ctx)
        if as_file:
            return fp
        else:
            return fp.read()
    if as_file:
        return io.StringIO(value)
    else:
        return value


def value_optionally_from_binary_file(
    value, param, ctx, encoding="utf-8", **file_kwargs
):
    if isinstance(param, str):
        # Not a real param, just a "param_hint". Make an equivalent param.
        param = Argument(param_decls=[param])

    if value == "-" or value.startswith("@"):
        filetype = click.File(mode="rb", **file_kwargs)
        filename = _resolve_file(value[1:]) if value.startswith("@") else value
        fp = filetype.convert(filename, param, ctx)
        return fp.read()

    return value.encode(encoding)


class JsonFromFile(StringFromFile):
    name = "json"

    def __init__(self, schema=None, **file_kwargs):
        super().__init__(**file_kwargs)
        self.schema = schema

    def convert(self, value, param, ctx):
        value = super().convert(value, param, ctx)
        try:
            value = json.loads(value)
        except json.JSONDecodeError as e:
            self.fail(
                f"Invalid JSON: {e}",
                param,
                ctx,
            )
        if self.schema:
            # jsonschema is quite a heavyweight import
            import jsonschema

            try:
                jsonschema.validate(instance=value, schema=self.schema)
            except jsonschema.ValidationError as e:
                self.fail(str(e), param, ctx)
        return value


def call_and_exit_flag(*args, callback, is_eager=True, **kwargs):
    """
    Add an is_flag option that, when set, eagerly calls the given callback with only the context as a parameter.
    The process exits once the callback is finished.
    Usage:
    @call_and_exit_flag("--version", callback=print_version, help="Print the version number")
    """

    def actual_callback(ctx, param, value):
        if value and not ctx.resilient_parsing:
            callback(ctx)
            ctx.exit()

    return click.option(
        *args,
        is_flag=True,
        callback=actual_callback,
        expose_value=False,
        is_eager=is_eager,
        **kwargs,
    )


class OutputFormatType(click.ParamType):
    """
    An output format. Loosely divided into two parts: '<type>[:<format>]'
    e.g.
        json
        text
        geojson
        html
        json:compact
        text:%H
        "text:%H %s"

    For text formatstrings, see the 'PRETTY FORMATS' section of `git help log`
    """

    name = "string"

    JSON_STYLE_CHOICES = ("compact", "extracompact", "pretty")

    def __init__(self, *, output_types, allow_text_formatstring):
        self.output_types = tuple(output_types)
        self.allow_text_formatstring = allow_text_formatstring

    def convert(self, value, param, ctx):
        if isinstance(value, tuple):
            return value
        if ":" in value:
            output_type, fmt = value.split(":", 1)
        else:
            output_type = value
            fmt = None

        if output_type not in self.output_types:
            self.fail(
                "invalid choice: {}. (choose from {})".format(
                    output_type, ", ".join(self.output_types)
                ),
                param,
                ctx,
            )
        fmt = self.validate_fmt(ctx, param, output_type, fmt)
        return output_type, fmt

    def validate_fmt(self, ctx, param, output_type, fmt):
        fmt = fmt or None
        if output_type in ("json", "json-lines", "geojson"):
            fmt = fmt or "pretty"
            if fmt not in self.JSON_STYLE_CHOICES:
                self.fail(
                    "invalid choice: {}. (choose from {})".format(
                        fmt, ", ".join(self.JSON_STYLE_CHOICES)
                    ),
                    ctx=ctx,
                    param=param,
                )
            return fmt
        elif output_type == "text" and self.allow_text_formatstring:
            return fmt
        if fmt:
            self.fail(
                f"Output format '{output_type}' doesn't currently accept any qualifiers (got: ':{fmt}'",
                ctx=ctx,
                param=param,
            )

    def shell_complete(self, ctx=None, param=None, incomplete=""):
        return [
            CompletionItem(type)
            for type in self.output_types
            if type.startswith(incomplete)
        ]


class DeltaFilterType(click.ParamType):
    """
    Filters parts of individual deltas - new or old values for inserts, updates, or deletes.
    "--" is the key for old values of deletes
    "-" is the key for old values of updates
    "+" is the key for new values of updates
    "++" is they key for new values of inserts
    """

    name = "string"

    ALLOWED_KEYS = ("--", "-", "+", "++")

    def convert(self, value, param, ctx):
        from kart.key_filters import DeltaFilter

        if value is None:
            return None
        if value.lower() == "all":
            return DeltaFilter.MATCH_ALL
        pieces = value.split(",")
        if any(p for p in pieces if p not in self.ALLOWED_KEYS):
            self.fail(
                "Delta filter only accepts any subset of the following comma separated keys: --,-,+,++"
            )
        return DeltaFilter(pieces)


def find_param(ctx_or_params, name):
    """Given the click context / command / list of params - find the param with the given name."""
    ctx = ctx_or_params
    if isinstance(ctx, click.core.Context):
        ctx = ctx.command
    if isinstance(ctx, click.core.Command):
        params = ctx.params
    else:
        params = ctx_or_params

    for param in params:
        if param.name == name:
            return param
    raise RuntimeError(f"Couldn't find param: {name}")


def forward_context_to_command(ctx, command):
    """
    Given the current context and a new command (not the current command), run the new command but
    reusing the same overall context including command line arguments.
    The new command should accept arguments in much the same form as the current command.
    If any arguments cannot be parsed by the new command, it will fail with a UsageError as per usual.
    This could be acceptable so long as it is clear to the user what is unsupported and why.
    """
    subctx = command.make_context(command.name, ctx.unparsed_args)
    subctx.obj = ctx.obj
    subctx.forward(command)
