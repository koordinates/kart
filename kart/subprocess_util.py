import asyncio
import functools
import os
from pathlib import Path
import platform
import subprocess
import sys
from asyncio import IncompleteReadError, LimitOverrunError
from functools import partial

# Package kart.subprocess_util is a drop-in replacement for subprocess which handles some things
# that kart will generally want to do when calling a subprocess, as well as having some extra
# functionality that the subprocess module does not. Here are the things it can do:

# 1. tool_environment() - this makes sure that when we call git, git-lfs, pdal etc, we have the right
# environment including the right PATH that includes the kart bin/ directory.

# Every method here - run, call, check_call, check_output, run_and_tee_output, run_then_exit -
# sets the environment to be the tool_environment() if no other environment is supplied.

# 2. sys.stdin, sys.stdout, sys.stderr: When Kart is run in helper mode, these variables are updated
# each time a new kart process connects to the helper daemon to be the same as the file-descriptors
# from the calling process. This is due to the following flow:

# User opens a new terminal, which has its stdin,stdout,stderr connected to eg /dev/tty1
# User runs kart - actually the lightweight kart.c executable. It too is connected to /dev/tty1
# kart.c executable connects to *this process* - a longer running `kart helper` python daemon.
# Its stdin,stdout,stderr will already be connected to *something* - depending on what it did last -
# but now its stdin,stdout,stderr need to be updated to /dev/tty1 to match the kart.c executable.
# These file-descriptors are sent to the `kart helper` daemon using `sendmsg`.
# The variables sys.stdin, sys.stdout and sys.stderr and updated using these new values.
# This means that the python process can now use these variables as normal, and everything -
# click.echo, print, etc - works exactly as if the user had just run the kart python process directly.

# Except: Somewhere deep inside python, the original file-descriptors for stdin, stdout and stderr
# are still stored. This shows up if we do a call such as subprocess.run(["pwd"]) without explicitly
# setting stdin, stdout, stderr - these parameters don't default to sys.stdin, sys.stdout and
# sys.stderr (which is what we would like) but instead default to the original value of stdin,
# stdout, and stderr. This would mean that the subprocess that Kart runs could end up connected
# to a different tty to Kart itself, which is obviously not what we want. So, in this module,
# we simply fix the defaults of these parameters to be sys.stdin, sys.stdout, and sys.stderr,
# anytime that this process is run in helper mode.

# Every method here - run, call, check_call, check_output, run_and_tee_output, run_then_exit -
# makes sure to set these parameters explicitly when run in helper mode.

# 3. Capturing output during testing: similar to #2, the click test harness updates sys.stdout
# and sys.stderr to special values that capture the output so that it can be checked in asserts.
# Theoretically, the code from #2 would handle this perfectly too, except that unfortunately
# these special values of sys.stdin and sys.stderr don't have file-descriptors attached and
# so can't be used in calls to subprocess.run. Since this is only an issue during testing, we
# instead just set the stdout and stderr to PIPE, capture the subprocess output, and then write
# it to the special values of sys.stdout and sys.stderr. This breaks real-time progress output
# for the subprocess but this is not important during testing.

# This fix is only applied to run_and_tee_output and run_then_exit. For correctness, it should be
# applied to other subprocess calls, but in practice, all of our tests that make asserts about
# subprocess output rely only on run_then_exit - in the other cases, the test will be unaware
# of the output from the subprocess, but we don't make any asserts based on it.
# We could if needed apply this fix to the other methods (with a little more complexity - other
# methods might have set the stdout / stderr parameters already).

# 4. Two extra functions: run_and_tee_output, run_and_then_exit.


CalledProcessError = subprocess.CalledProcessError
DEVNULL = subprocess.DEVNULL
PIPE = subprocess.PIPE


def run(cmd, **kwargs):
    return subprocess.run(cmd, **add_default_kwargs(kwargs))


def call(cmd, **kwargs):
    return subprocess.call(cmd, **add_default_kwargs(kwargs))


def check_call(cmd, **kwargs):
    return subprocess.check_call(cmd, **add_default_kwargs(kwargs))


def check_output(cmd, **kwargs):
    return subprocess.check_output(cmd, **add_default_kwargs(kwargs, check_output=True))


def Popen(cmd, **kwargs):
    return subprocess.Popen(cmd, **add_default_kwargs(kwargs))


def add_default_kwargs(kwargs_dict, check_output=False):
    # We could allow the caller to supply the env, but env_overrides is generally more useful.
    # You can disable this assert if you are sure you need this (and not env_overrides).
    assert "env" not in kwargs_dict

    if "env_overrides" in kwargs_dict:
        env = tool_environment(env_overrides=kwargs_dict.pop("env_overrides"))
    else:
        env = tool_environment()

    kwargs_dict["env"] = env

    # Explicitly set sys.stdin, sys.stderr, sys.stdout if this is running via helper mode.
    # See the explanation for why we need this at the top of the file.
    if os.environ.get("KART_HELPER_PID"):
        if "input" not in kwargs_dict:
            kwargs_dict.setdefault("stdin", sys.stdin)

        capture_output = kwargs_dict.get("capture_output", False)
        if not check_output and not capture_output:
            kwargs_dict.setdefault("stdout", sys.stdout)
        if not capture_output:
            kwargs_dict.setdefault("stderr", sys.stderr)

    return kwargs_dict


async def read_stream_and_display(stream, display):
    """Read from stream line by line until EOF, display, and capture the lines."""
    output = []
    while True:
        line = await read_universal_line(stream)
        if not line:
            break
        output.append(line)
        display(line)  # assume it doesn't block
    return b"".join(output)


async def read_and_display(cmd, tee_stdout=False, tee_stderr=False, **kwargs):
    """Capture cmd's stdout and/or stderr while displaying them as they arrive (line by line)."""
    if tee_stdout:
        kwargs["stdout"] = PIPE
    if tee_stderr:
        kwargs["stderr"] = PIPE
    process = await asyncio.create_subprocess_exec(*cmd, **kwargs)

    def display(stream, output):
        stream.buffer.write(output)
        stream.flush()

    # Read child's stdout/stderr concurrently (capture and display)
    try:
        stream_coroutines = []
        if tee_stdout:
            stream_coroutines.append(
                read_stream_and_display(process.stdout, partial(display, sys.stdout))
            )
        if tee_stderr:
            stream_coroutines.append(
                read_stream_and_display(process.stderr, partial(display, sys.stderr))
            )
        outputs = list(await asyncio.gather(*stream_coroutines))
    except Exception:
        process.kill()
        raise
    finally:
        # Wait for the process to exit
        await process.wait()
    if tee_stdout:
        process.stdout = outputs.pop(0)
    if tee_stderr:
        process.stderr = outputs.pop(0)
    return process


async def read_universal_line(stream):
    """Read chunk of data from the stream until a newline char '\r' or '\n' is found."""
    separators = b"\r\n"
    try:
        line = await read_until_any_of(stream, separators)
    except IncompleteReadError as e:
        return e.partial
    except LimitOverrunError as e:
        if stream._buffer[e.consumed] in separators:
            del stream._buffer[: e.consumed + 1]
        else:
            stream._buffer.clear()
        stream._maybe_resume_transport()
        raise ValueError(e.args[0])
    return line


async def read_until_any_of(stream, separators=b"\n"):
    """Read data from the stream until any of the separator chars are found."""
    if len(separators) < 1:
        raise ValueError("separators should be at least one-byte string")

    if stream._exception is not None:
        raise stream._exception

    offset = 0

    # Loop until we find `separator` in the buffer, exceed the buffer size,
    # or an EOF has happened.
    while True:
        buflen = len(stream._buffer)

        # Check if we now have enough data in the buffer for `separator` to
        # fit.
        if buflen - offset >= 1:
            isep = min(
                (
                    i
                    for i in (stream._buffer.find(s, offset) for s in separators)
                    if i >= 0
                ),
                default=-1,
            )

            if isep != -1:
                # `separator` is in the buffer. `isep` will be used later to retrieve the data.
                break

            offset = buflen
            if offset > stream._limit:
                raise LimitOverrunError(
                    "Separator is not found, and chunk exceed the limit", offset
                )

        # Complete message (with full separator) may be present in buffer
        # even when EOF flag is set. This may happen when the last chunk
        # adds data which makes separator be found. That's why we check for
        # EOF *ater* inspecting the buffer.
        if stream._eof:
            chunk = bytes(stream._buffer)
            stream._buffer.clear()
            raise IncompleteReadError(chunk, None)

        # _wait_for_data() will resume reading if stream was paused.
        await stream._wait_for_data("readuntil")

    if isep > stream._limit:
        raise LimitOverrunError(
            "Separator is found, but chunk is longer than limit", isep
        )

    chunk = stream._buffer[: isep + 1]
    del stream._buffer[: isep + 1]
    stream._maybe_resume_transport()
    return bytes(chunk)


def run_and_tee_output(cmd, tee_stdout=False, tee_stderr=False, **kwargs):
    """
    Run a subprocess and *don't* capture its output - let stdout and stderr display as per usual -
    - but also *do* capture its output so that we can inspect it.
    Returns a tuple of (exit-code, stdout output string, stderr output string).
    """
    if "_KART_RUN_WITH_CAPTURE" in os.environ:
        tee_stdout = True
        tee_stderr = True
    proc = asyncio.run(
        read_and_display(
            cmd,
            tee_stdout=tee_stdout,
            tee_stderr=tee_stderr,
            **add_default_kwargs(kwargs),
        )
    )
    return proc


def run_then_exit(cmd):
    """
    Works like subprocess.run, but has the following three differences.
    1. Simplified - unlike subprocess.run, you can't configure streams, env, encoding, etc.
       The environment used is tool_environment(), which is generally the right one.
    2. Kart exits as soon as the subprocess exits, with the same return code as the subprocess.
    3. Changes behaviour during testing to buffer output using PIPEs instead of connecting stdout and
       stderr directly. This means that the test harness can read the subprocess stdout and stderr exactly
       as if Kart had written directly. The downside (the reason we don't run like this always) is that
       it buffers all the output until the process has finished, so the user wouldn't see progress.
    """
    if "_KART_RUN_WITH_CAPTURE" in os.environ:
        _run_with_capture_then_exit(cmd)
    else:
        p = subprocess.run(
            cmd,
            encoding="utf-8",
            env=tool_environment(),
            stdin=sys.stdin,
            stdout=sys.stdout,
            stderr=sys.stderr,
        )
        sys.exit(p.returncode)


def _run_with_capture_then_exit(cmd):
    # In testing, .run must be set to capture_output and so use PIPEs to communicate
    # with the process to run whereas in normal operation the standard streams of
    # this process are passed into subprocess.run.
    # Capturing the output in a PIPE and then writing to sys.stdout is compatible
    # with click.testing which sets sys.stdout and sys.stderr to a custom
    # io wrapper.
    # This io wrapper is not compatible with the stdin= kwarg to .run - in that case
    # it gets treated as a file like object and fails.
    p = subprocess.run(
        cmd,
        capture_output=True,
        encoding="utf-8",
        env=tool_environment(),
    )
    sys.stdout.write(p.stdout)
    sys.stdout.flush()
    sys.stderr.write(p.stderr)
    sys.stderr.flush()
    sys.exit(p.returncode)


def tool_environment(*, base_env=None, env_overrides=None):
    """
    Returns a dict of environment variables for launching an external process.
    Sets the PATH, GIT_CONFIG_PARAMETERS, etc appropriately.

    base_env - the environment to start from, defaults to os.environ if not set.
    env_overrides - any caller-provided extra environment variables to add after the tool-environment
        is configured.
    """
    env = (base_env or os.environ).copy()

    tool_env_overrides = get_tool_environment_overrides()
    _merge_env_variable(env, "PATH", tool_env_overrides, env, os.pathsep)
    _merge_env_variable(env, "GIT_CONFIG_PARAMETERS", tool_env_overrides, env, " ")

    if platform.system() == "Linux":
        # https://pyinstaller.readthedocs.io/en/stable/runtime-information.html#ld-library-path-libpath-considerations
        if "LD_LIBRARY_PATH_ORIG" in env:
            env["LD_LIBRARY_PATH"] = env["LD_LIBRARY_PATH_ORIG"]
        else:
            env.pop("LD_LIBRARY_PATH", None)

    if env_overrides:
        env.update(env_overrides)
        # Handle {key: None} to unset env variables:
        for key, value in env_overrides.items():
            if value is None:
                env.pop(key)
    return env


def _merge_env_variable(output_dict, key, lhs_dict, rhs_dict, separator):
    lhs_value = lhs_dict.get(key)
    rhs_value = rhs_dict.get(key)
    if lhs_value or rhs_value:
        output_dict[key] = _merge_env_variable_value(lhs_value, rhs_value, separator)


def _merge_env_variable_value(lhs_value, rhs_value, separator):
    if not lhs_value:
        return rhs_value
    if not rhs_value:
        return lhs_value
    return f"{lhs_value}{separator}{rhs_value}"


@functools.lru_cache(maxsize=1)
def get_tool_environment_overrides():
    return {
        # Add kart bin directory to the start of the path:
        "PATH": str(Path(sys.executable).parents[0]),
        # Modify git config:
        "GIT_CONFIG_PARAMETERS": get_git_config_parameters(),
    }


# These are all the Kart defaults that differ from git's defaults.
# (all of these can still be overridden by setting them in a git config file.)
GIT_CONFIG_DEFAULT_OVERRIDES = {
    # git will change to this branch sooner or later, but hasn't yet.
    "init.defaultBranch": "main",
    # Deltified objects seem to affect clone and diff performance really badly
    # for Kart repos. So we disable them by default.
    "pack.depth": 0,
    "pack.window": 0,
}
if platform.system() == "Linux":
    import certifi

    GIT_CONFIG_DEFAULT_OVERRIDES["http.sslCAInfo"] = certifi.where()

# These are the settings that Kart always *overrides* in git config.
# i.e. regardless of your local git settings, kart will use these settings instead.
GIT_CONFIG_FORCE_OVERRIDES = {
    # We use base64 for feature paths.
    # "kcya" and "kcyA" are *not* the same feature; that way lies data loss
    "core.ignoreCase": "false",
}


def get_git_config_parameters():
    """
    Initialises default config values that differ from git's defaults.
    """
    configs = list(_pygit2_configs())
    new_config_params = []
    for k, v in GIT_CONFIG_DEFAULT_OVERRIDES.items():
        for config in configs:
            if k in config:
                break
        else:
            new_config_params.append(_git_sq_quote_buf(f"{k}={v}"))

    for k, v in GIT_CONFIG_FORCE_OVERRIDES.items():
        new_config_params.append(_git_sq_quote_buf(f"{k}={v}"))

    return " ".join(new_config_params)


# from https://github.com/git/git/blob/ebf3c04b262aa27fbb97f8a0156c2347fecafafb/quote.c#L12-L44
def _git_sq_quote_buf(src):
    dst = src.replace("'", r"'\''").replace("!", r"'\!'")
    return f"'{dst}'"


def _pygit2_configs():
    """
    Yields pygit2.Config objects in order of decreasing specificity.
    """
    import pygit2

    try:
        # ~/.gitconfig
        yield pygit2.Config.get_global_config()
    except OSError:
        pass
    try:
        # ~/.config/git/config
        yield pygit2.Config.get_xdg_config()
    except OSError:
        pass

    if "GIT_CONFIG_NOSYSTEM" not in os.environ:
        # /etc/gitconfig
        try:
            yield pygit2.Config.get_system_config()
        except OSError:
            pass
