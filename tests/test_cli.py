import re

import pytest

from snowdrop import cli


H = pytest.helpers.helpers()


def test_version(cli_runner):
    r = cli_runner.invoke(["--version"])
    assert r.exit_code == 0, r
    assert re.match(
        r"^Project Snowdrop v(\d.\d.*?)\nGDAL v\d\.\d+\.\d+.*?\nPyGit2 v\d\.\d+\.\d+[^;]*; Libgit2 v\d\.\d+\.\d+.*$",
        r.stdout,
    )


def test_cli_help():
    click_app = cli.cli
    for name, cmd in click_app.commands.items():
        assert cmd.help, f"`{name}` command has no help text"
