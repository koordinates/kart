import click


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
