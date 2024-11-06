from datetime import datetime
import os
from pathlib import Path
import platform
import re
import shlex
import sys

def win_quote(arg, force=False):
    if force or re.search(r'["\s^&|<>]', arg):
        arg = arg.replace('"', '""')
        return f'"{arg}"'
    return arg

def get_executable_path():
    def good_executable_path(exe_path):
        return ("kart" in os.path.basename(exe_path), os.path.isabs(exe_path))

    return max(sys.executable, sys.argv[0], key=good_executable_path)

def print_diagnostics():
    if platform.system() == "Windows":
        quote = win_quote
    else:
        quote = shlex.quote

    output = ["==== KART DIAGNOSTICS ===="]

    try:
        from kart.version import get_version_info_text

        output += get_version_info_text()
    except:
        raise

    cmd = [get_executable_path()] + sys.argv[1:]
    cmd = " ".join(quote(c) for c in cmd)
    output.append("\n==== COMMAND ====")
    output.append(cmd)

    output.append("\n==== PROCESS ====")
    other_info = {
        "now": str(datetime.now()),
        "ppid": os.getppid(),
        "sid": os.getsid(0),
        "kart_helper_pid": os.environ.get("KART_HELPER_PID"),
        "pid": os.getpid(),
    }
    output.append(repr(other_info))

    environ = dict(sorted(os.environ.items()))
    output.append("\n==== ENVIRONMENT ====")
    output.append("(as python dict)")
    output.append(repr(environ))

    # Manually setting this variable crashes zsh for some reason, so, we won't put it in the standalone command
    environ.pop("XPC_SERVICE_NAME", None)

    if platform.system() == "Windows":
        # Powershell syntax
        ps_env_vars = "; ".join(
            f"${{env:{key}}}={win_quote(value, force=True)}"
            for key, value in environ.items()
        )
        output.append("\n==== STANDALONE POWERSHELL COMMAND ====")
        output.append(f"{ps_env_vars}; & {cmd}")

        # CMD syntax
        cmd_env_vars = " && ".join(
            "set " + win_quote(f"{key}={value}", force=True)
            for key, value in environ.items()
        )
        output.append("\n==== STANDALONE CMD COMMAND ====")
        output.append(f"{cmd_env_vars} && {cmd}")
    else:
        # Linux / macOS syntax
        env_vars = " ".join(
            f"{shlex.quote(key)}={quote(value)}"
            for key, value in environ.items()
        )
        standalone_cmd = f"{env_vars} {cmd}"
        output.append("\n==== STANDALONE COMMAND ====")
        output.append(standalone_cmd)

    output.append("\n==== END DIAGNOSTICS ====\n\n")

    output = "\n".join(output)
    print(output, file=sys.stderr)
    try:
        Path(os.path.expanduser("~"), "kart-diagnostics.txt").write_text(output)
    except:
        pass
