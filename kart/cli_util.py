import functools
import io
import json
import os
import logging
import platform
import warnings
from pathlib import Path

import click
import jsonschema
import pygit2
from click.core import Argument
from click.shell_completion import CompletionItem

from kart.help import kart_help


L = logging.getLogger("kart.cli_util")


class KartCommand(click.Command):
    def format_help(self, ctx, formatter):
        try:
            return kart_help(ctx)
        except Exception as e:
            import pdb

            pdb.set_trace()
            L.debug(f"Failed rendering help page: {e}")
            return super().format_help(ctx, formatter)


def _pygit2_configs():
    """
    Yields pygit2.Config objects in order of decreasing specificity.
    """
    try:
        # ~/.gitconfig
        yield pygit2.Config.get_global_config()
    except OSError:
        pass
    try:
        # ~/.config/git/config
        yield pygit2.Config.get_xdg_config()
    except OSError:
        pass

    if "GIT_CONFIG_NOSYSTEM" not in os.environ:
        # /etc/gitconfig
        try:
            yield pygit2.Config.get_system_config()
        except OSError:
            pass


# These are all the Kart defaults that differ from git's defaults.
# (all of these can still be overridden by setting them in a git config file.)
GIT_CONFIG_DEFAULT_OVERRIDES = {
    # git will change to this branch sooner or later, but hasn't yet.
    "init.defaultBranch": "main",
    # Deltified objects seem to affect clone and diff performance really badly
    # for Kart repos. So we disable them by default.
    "pack.depth": 0,
    "pack.window": 0,
}
if platform.system() == "Linux":
    import certifi

    GIT_CONFIG_DEFAULT_OVERRIDES["http.sslCAInfo"] = certifi.where()

# These are the settings that Kart always *overrides* in git config.
# i.e. regardless of your local git settings, kart will use these settings instead.
GIT_CONFIG_FORCE_OVERRIDES = {
    # We use base64 for feature paths.
    # "kcya" and "kcyA" are *not* the same feature; that way lies data loss
    "core.ignoreCase": "false",
}


# from https://github.com/git/git/blob/ebf3c04b262aa27fbb97f8a0156c2347fecafafb/quote.c#L12-L44
def _git_sq_quote_buf(src):
    dst = src.replace("'", r"'\''").replace("!", r"'\!'")
    return f"'{dst}'"


@functools.lru_cache()
def init_git_config():
    """
    Initialises default config values that differ from git's defaults.
    """
    configs = list(_pygit2_configs())
    new_config_params = []
    for k, v in GIT_CONFIG_DEFAULT_OVERRIDES.items():
        for config in configs:
            if k in config:
                break
        else:
            new_config_params.append(_git_sq_quote_buf(f"{k}={v}"))
    for k, v in GIT_CONFIG_FORCE_OVERRIDES.items():
        new_config_params.append(_git_sq_quote_buf(f"{k}={v}"))

    if new_config_params:
        existing = ""
        if "GIT_CONFIG_PARAMETERS" in os.environ:
            existing = f" {os.environ['GIT_CONFIG_PARAMETERS']}"
        os.environ["GIT_CONFIG_PARAMETERS"] = f'{" ".join(new_config_params)}{existing}'


def tool_environment(env=None):
    """
    Returns a dict of environment for launching an external process
    """
    init_git_config()
    env = (env or os.environ).copy()
    if platform.system() == "Linux":
        # https://pyinstaller.readthedocs.io/en/stable/runtime-information.html#ld-library-path-libpath-considerations
        if "LD_LIBRARY_PATH_ORIG" in env:
            env["LD_LIBRARY_PATH"] = env["LD_LIBRARY_PATH_ORIG"]
        else:
            env.pop("LD_LIBRARY_PATH", None)
    return env


def add_help_subcommand(group):
    @group.command(add_help_option=False, hidden=True)
    @click.argument("topic", default=None, required=False, nargs=1)
    @click.pass_context
    def help(ctx, topic, **kw):
        # https://www.burgundywall.com/post/having-click-help-subcommand
        if topic is None:
            click.echo(ctx.parent.get_help())
        else:
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


def parse_output_format(output_format, json_style):
    output_type, fmt = output_format
    if json_style is not None:
        warnings.warn(
            f"--json-style is deprecated and will be removed in Kart 0.12. use --output-format={output_type}:{json_style} instead",
            RemovalInKart012Warning,
        )
        if output_type in ("json", "json-lines", "geojson"):
            fmt = json_style
    return output_type, fmt


class RemovalInKart012Warning(UserWarning):
    pass


class RemovalInKart013Warning(UserWarning):
    pass
