import os
import re

import pygit2
import pytest

from sno import cli


H = pytest.helpers.helpers()


def test_version(cli_runner):
    r = cli_runner.invoke(["--version"])
    assert r.exit_code == 0, r
    assert re.match(
        r"^Sno v(\d+\.\d+.*?)\n» GDAL v",
        r.stdout,
    )


def test_cli_help():
    click_app = cli.cli
    for name, cmd in click_app.commands.items():
        if name == "help":
            continue
        assert cmd.help, f"`{name}` command has no help text"


@pytest.fixture
def empty_gitconfig(monkeypatch, tmpdir):
    old = os.environ["HOME"]
    (tmpdir / ".gitconfig").write_text("", encoding="utf8")
    monkeypatch.setenv("HOME", str(tmpdir))
    pygit2.option(
        pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, str(tmpdir)
    )
    yield
    pygit2.option(
        pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, str(old)
    )


def test_config(empty_gitconfig, cli_runner):
    # don't load the ~/.gitconfig file from conftest.py
    # (because it sets init.defaultBranch and we're trying to test what
    # happens without that set)
    # note: merely changing os.environ['HOME'] doesn't help here;
    # once libgit has seen one HOME it never notices if we change it.

    # The default init.defaultBranch in git is still 'master' as of 2.30.0
    # but we override it to 'main'. Let's check that works properly
    r = cli_runner.invoke(["config", "init.defaultBranch"])
    assert r.exit_code == 0, r.stderr
    assert r.stdout == "main\n"
