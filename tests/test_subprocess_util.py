import os
import platform
import sys

from kart import subprocess_util


def test_subprocess_tool_environment():
    env_exec = subprocess_util.tool_environment()
    assert len(env_exec)
    assert env_exec is not os.environ
    assert sys.executable.startswith(env_exec["PATH"].split(os.pathsep)[0])

    if platform.system() == "Linux":
        env_in = {"LD_LIBRARY_PATH": "bob", "LD_LIBRARY_PATH_ORIG": "alex", "my": "me"}
        env_exec = subprocess_util.tool_environment(env_in)
        assert env_exec is not env_in
        assert env_exec["LD_LIBRARY_PATH"] == "alex"
        assert env_exec["my"] == "me"

        env_in = {"LD_LIBRARY_PATH": "bob", "my": "me"}
        env_exec = subprocess_util.tool_environment(env_in)
        assert "LD_LIBRARY_PATH" not in env_exec
    else:
        env_in = {"my": "me"}
        env_exec = subprocess_util.tool_environment(env_in)
        assert env_exec is not env_in
        env_exec.pop("PATH", None)
        assert env_exec == env_in
