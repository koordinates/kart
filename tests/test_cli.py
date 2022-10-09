import contextlib
import json
import os
import platform
import re
import sys
from pathlib import Path

import pygit2
import pytest

from kart import cli
from kart.cli_util import tool_environment


H = pytest.helpers.helpers()


def test_version(cli_runner):
    r = cli_runner.invoke(["--version"])
    assert r.exit_code == 0, r
    assert re.match(
        r"^Kart v(\d+\.\d+.*?)\n» GDAL v",
        r.stdout,
    )


def test_cli_help():
    click_app = cli.cli
    for name, cmd in click_app.commands.items():
        if name == "help":
            continue
        assert cmd.help, f"`{name}` command has no help text"


@pytest.mark.parametrize("command", [["--help"], ["init", "--help"]])
def test_help_page_render(cli_runner, command):
    r = cli_runner.invoke(command)
    assert r.exit_code == 0, r.stderr


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


@pytest.fixture
def sys_path_reset(monkeypatch):
    """A context manager to save & reset after code that changes sys.path"""

    @contextlib.contextmanager
    def _sys_path_reset():
        with monkeypatch.context() as m:
            m.setattr("sys.path", sys.path[:])
            yield

    return _sys_path_reset


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


def test_cli_tool_environment():
    env_exec = tool_environment()
    assert len(env_exec)
    assert env_exec is not os.environ
    assert sys.executable.startswith(env_exec["PATH"].split(os.pathsep)[0])

    if platform.system() == "Linux":
        env_in = {"LD_LIBRARY_PATH": "bob", "LD_LIBRARY_PATH_ORIG": "alex", "my": "me"}
        env_exec = tool_environment(env_in)
        assert env_exec is not env_in
        assert env_exec["LD_LIBRARY_PATH"] == "alex"
        assert env_exec["my"] == "me"

        env_in = {"LD_LIBRARY_PATH": "bob", "my": "me"}
        env_exec = tool_environment(env_in)
        assert "LD_LIBRARY_PATH" not in env_exec
    else:
        env_in = {"my": "me"}
        env_exec = tool_environment(env_in)
        assert env_exec is not env_in
        env_exec.pop("PATH", None)
        assert env_exec == env_in


def test_ext_run(tmp_path, cli_runner, sys_path_reset):
    # missing script
    with sys_path_reset():
        r = cli_runner.invoke(["ext-run", tmp_path / "zero.py"])
    assert r.exit_code == 2, r

    # invalid syntax
    with open(tmp_path / "one.py", "wt") as fs:
        fs.write("def nope")
    with sys_path_reset():
        r = cli_runner.invoke(["ext-run", tmp_path / "one.py"])
    assert r.exit_code == 1, r
    assert "Error: loading " in r.stderr
    assert "SyntaxError" in r.stderr
    assert "line 1" in r.stderr

    # main() with wrong argspec
    with open(tmp_path / "two.py", "wt") as fs:
        fs.write("def main():\n  print('nope')")
    with sys_path_reset():
        r = cli_runner.invoke(["ext-run", tmp_path / "two.py"])
    assert r.exit_code == 1, r
    assert "requires a main(ctx, args) function" in r.stderr

    # no main()
    with open(tmp_path / "three_a.py", "wt") as fs:
        fs.write("A = 3")
    with sys_path_reset():
        r = cli_runner.invoke(["ext-run", tmp_path / "three_a.py"])
    assert r.exit_code == 1, r
    assert "does not have a main(ctx, args) function" in r.stderr

    # working example
    with open(tmp_path / "three.py", "wt") as fs:
        fs.write(
            "\n".join(
                [
                    "import json",
                    "import kart",
                    "import three_a",
                    "def main(ctx, args):",
                    "  print(json.dumps([",
                    "    repr(ctx), args,",
                    "    bool(kart.is_frozen), three_a.A,",
                    "    __file__, __name__",
                    "  ]))",
                ]
            )
        )
    with sys_path_reset():
        r = cli_runner.invoke(["ext-run", tmp_path / "three.py", "arg1", "arg2"])
    print(r.stdout)
    print(r.stderr)
    assert r.exit_code == 0, r

    sctx, sargs, val1, val2, sfile, sname = json.loads(r.stdout)
    assert sctx.startswith("<click.core.Context object")
    assert sargs == ["arg1", "arg2"]
    assert (val1, val2) == (False, 3)
    assert Path(sfile) == (tmp_path / "three.py")
    assert sname == "kart.ext_run.three"
