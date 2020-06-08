from functools import wraps
import uuid

import click


def one_of(*option_decorators, name):
    """
    Pass a list of options which are mutually exclusive.
    Only one of them will be allowed.
    Each option should be a `click.option` call.

    Pass the name as a separate kwarg.
    The value will be passed to the function as that kwarg.

        @one_of(
            click.option(
                "--text", flag_value="text", default=True
            ),
            click.option(
                "--json", flag_value="json"
            ),
            name="format"
        )
        def mycommand(ctx, format="text"):
            ...
    """

    def dec(cmd_func):
        @wraps(cmd_func)
        def wrapper(*args, **kwargs):
            return cmd_func(*args, **kwargs)

        # apply all the arguments to a dummy function so we can figure out
        # what options we have.
        def dummy():
            pass

        for option_dec in reversed(option_decorators):
            # apply all the options to a dummy function so we can figure out
            # what options we have.
            dummy = option_dec(dummy)
            # But then also apply them to the main command
            wrapper = option_dec(wrapper)

        # now, this holds a list of Option instances:
        option_names = set()
        for opt in dummy.__click_params__:
            if opt.name in option_names:
                raise ValueError("options in `one_of` must have distinct names")
            option_names.add(opt.name)

        option_instances = {}
        for opt in wrapper.__click_params__:
            if opt.name in option_names:
                option_instances[opt.name] = opt

        def check_mutually_exclusive_options(ctx, param, value):
            seen_name = None
            value = None
            for opt_name in option_names:
                # prevent the option from being passed to the command function
                option_instances[opt_name].expose_value = False
                if opt_name in ctx.params:
                    if seen_name is not None:
                        raise click.UsageError(
                            f"Illegal usage: {opt_name} can't be used together with {seen_name}."
                        )
                    value = ctx.params.pop(opt_name)
                    seen_name = opt_name
            return value

        # Add a callback for a parameter that will never actually
        # be present on the command line.
        # This means the callback is called *after* all the options that
        # *are* present have been added to the context.
        # https://click.palletsprojects.com/en/7.x/advanced/#callback-evaluation-order
        decorator = click.option(
            f'--missing-{uuid.uuid4()}',
            name,
            hidden=True,
            callback=check_mutually_exclusive_options,
        )
        return decorator(wrapper)

    return dec


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
        for mutex_opt in self.exclusive_with:
            if mutex_opt in opts:
                if current_opt:
                    raise click.UsageError(
                        "Illegal usage: '"
                        + str(self.name)
                        + "' is mutually exclusive with "
                        + str(mutex_opt)
                        + "."
                    )
                else:
                    self.prompt = None
        return super().handle_parse_result(ctx, opts, args)


def do_json_option(func):
    """Apply --json/--text output format options to a Click command """
    return click.option(
        "--json/--text",
        "do_json",
        is_flag=True,
        default=False,
        help="Whether to format the out output as JSON instead the default text output.",
    )(func)


def call_and_exit_flag(*args, callback, **kwargs):
    """
    Add an is_flag option that, when set, eagerly calls the given callback with only the context as a parameter.
    The callback may want to exit the program once it has completed, using ctx.exit(0)
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
        is_eager=True,
        **kwargs,
    )
