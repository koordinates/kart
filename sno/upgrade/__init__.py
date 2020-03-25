import importlib.util
from pathlib import Path

import click


class UpgradeCommand(click.MultiCommand):
    plugin_path = Path(__file__).parent

    def list_commands(self, ctx):
        rv = []
        for filename in self.plugin_path.glob("upgrade_*.py"):
            rv.append(filename.stem[8:].replace("_", "-"))
        rv.sort()
        return rv

    def get_command(self, ctx, name):
        module_name = f"sno.upgrade.upgrade_{name.replace('-', '_')}"

        module = importlib.import_module(module_name)
        return module.upgrade


@click.command(cls=UpgradeCommand)
def upgrade():
    """ Upgrade repositories between versions of Sno """
    pass
