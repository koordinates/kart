import click

from . import upgrade_00_02, upgrade_02_05  # noqa


@click.group()
def upgrade():
    """ Upgrade repositories between versions of Sno """
    pass


upgrade.add_command(upgrade_00_02.upgrade, name='00-02')
upgrade.add_command(upgrade_02_05.upgrade, name='02-05')
