import os
import platform
import pytest
import sys

import pygit2

from kart import subprocess_util


def test_subprocess_tool_environment():
    env_exec = subprocess_util.tool_environment()
    assert len(env_exec)
    assert env_exec is not os.environ
    assert sys.executable.startswith(env_exec["PATH"].split(os.pathsep)[0])

    if platform.system() == "Linux":
        base_env = {
            "LD_LIBRARY_PATH": "bob",
            "LD_LIBRARY_PATH_ORIG": "alex",
            "my": "me",
        }
        env_exec = subprocess_util.tool_environment(base_env=base_env)
        assert env_exec is not base_env
        assert env_exec["LD_LIBRARY_PATH"] == "alex"
        assert env_exec["my"] == "me"

        base_env = {"LD_LIBRARY_PATH": "bob", "my": "me"}
        env_exec = subprocess_util.tool_environment(base_env=base_env)
        assert "LD_LIBRARY_PATH" not in env_exec
    else:
        base_env = {"my": "me"}
        env_exec = subprocess_util.tool_environment(base_env=base_env)
        assert env_exec is not base_env
        env_exec.pop("PATH", None)
        env_exec.pop("GIT_CONFIG_PARAMETERS", None)
        assert env_exec == base_env


@pytest.fixture
def empty_gitconfig(monkeypatch, tmpdir):
    old = os.environ["HOME"]
    (tmpdir / ".gitconfig").write_text("", encoding="utf8")
    monkeypatch.setenv("HOME", str(tmpdir))
    pygit2.option(
        pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, str(tmpdir)
    )
    subprocess_util.get_tool_environment_overrides.cache_clear()
    yield
    subprocess_util.get_tool_environment_overrides.cache_clear()
    pygit2.option(
        pygit2.GIT_OPT_SET_SEARCH_PATH, pygit2.GIT_CONFIG_LEVEL_GLOBAL, str(old)
    )


def test_git_config(empty_gitconfig, cli_runner):
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
