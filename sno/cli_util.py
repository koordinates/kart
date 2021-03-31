import click
import io
import json
import jsonschema
import os
import platform

import pygit2


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


def startup_load_git_init_config():
    """
    Initialises the default value for the `init.defaultBranch` config variable.
    Call this before calling git init, clone, or config.
    """
    for config in _pygit2_configs():
        if "init.defaultBranch" in config:
            break
    else:
        existing = ""
        if "GIT_CONFIG_PARAMETERS" in os.environ:
            existing = f" {os.environ['GIT_CONFIG_PARAMETERS']}"
        os.environ["GIT_CONFIG_PARAMETERS"] = f"'init.defaultBranch=main'{existing}"


def git_remote_environment():
    """
    Returns a dict of environment for launching a git process
    for interacting with a remote.
    """
    env = os.environ.copy()
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


def value_optionally_from_text_file(value, param, ctx, as_file=False, **file_kwargs):
    """
    Given a string, interprets it either as:
    * a filename prefixed with '@', and returns the contents of the file
    * just a string, and returns the string itself.

    By default, returns the value as a string.
    If as_file=True, returns a StringIO or a file object. Use this when dealing with
    large files to save memory.
    """
    if value == "-" or value.startswith("@"):
        filetype = click.File(**file_kwargs)
        filename = value[1:] if value.startswith("@") else value
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
    if value == "-" or value.startswith("@"):
        filetype = click.File(mode="rb", **file_kwargs)
        filename = value[1:] if value.startswith("@") else value
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
