import pytest
import os

from kart.help import get_renderer


@pytest.mark.parametrize("command", [["--help"], ["init", "--help"]])
def test_help_page_render(cli_runner, command):
    r = cli_runner.invoke(command)
    assert r.exit_code == 0, r.stderr


def test_pager_with_no_env():
    renderer = get_renderer()
    assert renderer.get_pager_cmdline()[0] == renderer.PAGER.split()[0]


@pytest.mark.parametrize(
    "pager_cmd", ["less", "less -X --clearscreen", "more", "foobar"]
)
def test_pager_with_env(pager_cmd):
    os.environ["PAGER"] = pager_cmd
    renderer = get_renderer()
    assert renderer.get_pager_cmdline()[0] == os.environ["PAGER"].split(" ")[0]
