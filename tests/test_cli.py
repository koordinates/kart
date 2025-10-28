import contextlib
import json
import os
import re
from pathlib import Path
import signal
import subprocess
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


TEST_SIGINT_PY = r"""
import datetime
import os
import sys
from time import sleep


def main(ctx, args):
    print(os.getpid())
    fork_id = os.fork()
    if fork_id == 0:
        with open(args[0], 'w') as output:
            while True:
                output.write(datetime.datetime.now().isoformat() + '\n')
                output.flush()
                sleep(0.01)
    else:
        print(fork_id)
        sys.stdout.flush()
        os.wait()
"""


def find_kart_exe(use_helper: bool) -> Path:
    kart_bin_dir = Path(sys.executable).parent
    kart_exe = kart_bin_dir / "kart"
    kart_cli_exe = kart_bin_dir / "kart_cli"

    kart_with_helper_mode = kart_exe if kart_cli_exe.is_file() else kart_cli_exe
    kart_without_helper = kart_cli_exe if kart_cli_exe.is_file() else kart_exe

    if use_helper and not kart_with_helper_mode.is_file():
        raise pytest.skip(f"Couldn't find kart helper mode in {kart_bin_dir}")

    kart_to_use = kart_with_helper_mode if use_helper else kart_without_helper
    assert kart_to_use.is_file(), "Couldn't find kart"
    return kart_to_use


@pytest.mark.skipif(is_windows, reason="No SIGINT on windows")
@pytest.mark.parametrize("use_helper", [False, True])
def test_sigint_handling_unix(use_helper, tmp_path):
    import subprocess

    # working example
    test_sigint_py_path = tmp_path / "test_sigint.py"
    with open(test_sigint_py_path, "wt") as fs:
        fs.write(TEST_SIGINT_PY)

    subprocess_output_path = tmp_path / "output"

    env = os.environ.copy()
    env.pop("_KART_PGID_SET", None)
    env.pop("NO_CONFIGURE_PROCESS_CLEANUP", None)

    kart_to_use = find_kart_exe(use_helper)

    p = subprocess.Popen(
        [
            str(kart_to_use),
            "ext-run",
            str(test_sigint_py_path),
            str(subprocess_output_path),
        ],
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

    # Time goes past and grandchild keeps writing output
    output_size_1 = subprocess_output_path.stat().st_size
    sleep(1)
    assert p.poll() == None  # Grandchild subprocess keeps running...
    output_size_2 = subprocess_output_path.stat().st_size
    assert output_size_2 > output_size_1  # Grandchild output keeps growing...

    os.kill(child_pid, signal.SIGINT)
    sleep(1)
    assert p.poll() != None

    # Time goes past but granchild's output has stopped.
    output_size_3 = subprocess_output_path.stat().st_size
    sleep(1)
    output_size_4 = subprocess_output_path.stat().st_size
    assert output_size_3 == output_size_4


def list_zombie_children(pid: int) -> list[str]:
    # Check for zombie children using ps
    ps_output = subprocess.run(
        [
            "ps",
            "-o",
            "pid,ppid,stat,command",
            "-p",
            str(pid),
            "-g",
            str(pid),
        ],
        capture_output=True,
        text=True,
    )
    return [
        line
        for line in ps_output.stdout.splitlines()
        if len(line.split()) >= 3 and "Z" in line.split()[2]
    ]


@pytest.mark.skipif(is_windows, reason="No helper on windows")
@pytest.mark.parametrize("use_helper", [True])
def test_helper_no_zombie_processes(use_helper, tmp_path, data_archive):
    """
    Test that kart helper doesn't accumulate zombie processes.

    When the helper forks children that spawn subprocesses, those grandchildren
    can become zombies if they outlive their parent. This test verifies the
    helper properly reaps all child processes.
    """
    import subprocess

    with data_archive("points") as repo_path:
        # Start helper in background

        kart_helper = find_kart_exe(use_helper=False)
        socket_path = tmp_path / "helper.socket"
        helper = subprocess.Popen(
            [str(kart_helper), "helper", "--socket", str(socket_path)],
            env={**os.environ, "KART_USE_HELPER": "1"},
        )

        try:
            # Wait for helper to start
            sleep(0.5)
            zombie_children = list_zombie_children(helper.pid)
            assert len(zombie_children) == 0, (
                f"Found {len(zombie_children)} zombie processes:\n"
                + "\n".join(zombie_children)
            )

            # Run multiple commands through the helper
            # These will spawn git subprocesses
            for _ in range(5):
                subprocess.run(
                    [str(kart_helper), "diff", "HEAD~1..HEAD"],
                    cwd=repo_path,
                    env={**os.environ, "KART_USE_HELPER": "1"},
                    capture_output=True,
                )

            # Give processes time to complete
            sleep(1)

            zombie_children = list_zombie_children(helper.pid)
            assert len(zombie_children) == 0, (
                f"Found {len(zombie_children)} zombie processes:\n"
                + "\n".join(zombie_children)
            )

        finally:
            helper.terminate()
            helper.wait(timeout=5)
