import contextlib
import json
import os
import re
from pathlib import Path
import signal
import sys
from time import sleep

import pytest

from kart import cli, is_windows


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
def sys_path_reset(monkeypatch):
    """A context manager to save & reset after code that changes sys.path"""

    @contextlib.contextmanager
    def _sys_path_reset():
        with monkeypatch.context() as m:
            m.setattr("sys.path", sys.path[:])
            yield

    return _sys_path_reset


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


@pytest.mark.parametrize("use_helper", [False, True])
def test_sigint_handling_unix(use_helper, tmp_path):
    if is_windows:
        return

    import subprocess

    kart_bin_dir = Path(sys.executable).parent
    kart_exe = kart_bin_dir / "kart"
    kart_cli_exe = kart_bin_dir / "kart_cli"

    kart_with_helper_mode = kart_exe if kart_cli_exe.is_file() else kart_cli_exe
    kart_without_helper = kart_cli_exe if kart_cli_exe.is_file() else kart_exe

    if use_helper and not kart_with_helper_mode.is_file():
        raise pytest.skip(f"Couldn't find kart helper mode in {kart_bin_dir}")

    kart_to_use = kart_with_helper_mode if use_helper else kart_without_helper
    assert kart_to_use.is_file(), "Couldn't find kart"

    # working example
    with open(tmp_path / "test.py", "wt") as fs:
        fs.write(
            "\n".join(
                [
                    "import os",
                    "import sys",
                    "from time import sleep",
                    "",
                    "def main(ctx, args):",
                    "  print(os.getpid())",
                    "  fork_id = os.fork()",
                    "  if fork_id == 0:",
                    "    sleep(100)",
                    "  else:",
                    "    print(fork_id)",
                    "    sys.stdout.flush()",
                    "    os.wait()",
                    "",
                ]
            )
        )

    env = os.environ.copy()
    env.pop("_KART_PGID_SET", None)
    env.pop("NO_CONFIGURE_PROCESS_CLEANUP", None)

    p = subprocess.Popen(
        [str(kart_to_use), "ext-run", str(tmp_path / "test.py")],
        encoding="utf8",
        env=env,
        stdout=subprocess.PIPE,
    )
    sleep(1)
    child_pid = int(p.stdout.readline())
    grandchild_pid = int(p.stdout.readline())

    # The new kart process should be in a new process group.
    assert os.getpgid(0) != os.getpgid(child_pid)
    # And its subprocess should be in the same process group.
    assert os.getpgid(child_pid) == os.getpgid(grandchild_pid)
    orig_pgid = os.getpgid(child_pid)

    assert p.poll() == None
    os.kill(child_pid, signal.SIGINT)

    sleep(1)
    assert p.poll() != None

    def safe_get_pgid(pid):
        try:
            return os.getpgid(pid)
        except Exception:
            return -1

    # The child and grandchildren should now both be dead.
    # Their PIDs may now belong to other processes, but at least, they won't be in the same process group as before.
    assert safe_get_pgid(child_pid) != orig_pgid
    assert safe_get_pgid(grandchild_pid) != orig_pgid
