import os
import re
import sys
import platform
import subprocess
from enum import Enum
from pathlib import Path
from typing import Optional, Tuple, Optional, Any, Dict, List

import click
from click.shell_completion import _SOURCE_BASH, _SOURCE_ZSH, _SOURCE_FISH

try:
    import shellingham
except ImportError:
    shellingham = None


class Shells(str, Enum):
    bash = "bash"
    zsh = "zsh"
    fish = "fish"
    powershell = "powershell"
    pwsh = "pwsh"


_SOURCE_POWERSHELL = """
Import-Module PSReadLine
Set-PSReadLineKeyHandler -Chord Tab -Function MenuComplete
$scriptblock = {
    param($wordToComplete, $commandAst, $cursorPosition)
    $Env:%(complete_var)s= "powershell_complete"
    $Env:_KART_COMPLETE_ARGS = $commandAst.ToString()
    $Env:_KART_COMPLETE_WORD_TO_COMPLETE = $wordToComplete
    %(prog_name)s | ForEach-Object {
        $commandArray = $_ -Split " -- "
        $command = $commandArray[0]
        $helpString = $commandArray[1]
        [System.Management.Automation.CompletionResult]::new(
            $command, $command, 'ParameterValue', $helpString)
    }
    $Env:%(complete_var)s = ""
    $Env:_KART_COMPLETE_ARGS = ""
    $Env:_KART_COMPLETE_WORD_TO_COMPLETE = ""
}
Register-ArgumentCompleter -Native -CommandName %(prog_name)s -ScriptBlock $scriptblock
"""

_completion_scripts = {
    "bash": _SOURCE_BASH,
    "zsh": _SOURCE_ZSH,
    "fish": _SOURCE_FISH,
    "powershell": _SOURCE_POWERSHELL,
    "pwsh": _SOURCE_POWERSHELL,
}

_invalid_ident_char_re = re.compile(r"[^a-zA-Z0-9_]")


def get_completion_script(*, prog_name: str, complete_var: str, shell: str) -> str:
    cf_name = _invalid_ident_char_re.sub("", prog_name.replace("-", "_"))
    script = _completion_scripts.get(shell)
    if script is None:
        click.echo(f"Shell {shell} not supported.", err=True)
        sys.exit(1)
    return (
        script
        % dict(
            complete_func="_{}_completion".format(cf_name),
            complete_var=complete_var,
            prog_name=prog_name,
        )
    ).strip()


def install_helper(
    prog_name, complete_var, shell, completion_path, rc_path, completion_init_lines
):
    rc_path.parent.mkdir(parents=True, exist_ok=True)
    rc_content = ""
    if rc_path.is_file():
        rc_content = rc_path.read_text()
    for line in completion_init_lines:
        if line not in rc_content:
            rc_content += f"\n{line}"
    rc_content += "\n"
    rc_path.write_text(rc_content)
    # Install completion
    completion_path.parent.mkdir(parents=True, exist_ok=True)
    script_content = get_completion_script(
        prog_name=prog_name, complete_var=complete_var, shell=shell
    )
    completion_path.write_text(script_content)
    return completion_path


def install_bash(*, prog_name: str, complete_var: str, shell: str) -> Path:
    # Ref: https://github.com/scop/bash-completion#faq
    # It seems bash-completion is the official completion system for bash:
    # Ref: https://www.gnu.org/software/bash/manual/html_node/A-Programmable-Completion-Example.html
    # But installing in the locations from the docs doesn't seem to have effect
    completion_path = Path.home() / f".bash_completions/{prog_name}.sh"
    bashrc_path = Path.home() / ".bashrc"
    bash_profile = Path.home() / ".bash_profile"
    if platform.system() == "Darwin" and bash_profile.exists():
        bashrc_path = bash_profile
    completion_init_lines = [f"source {completion_path}"]
    return install_helper(
        prog_name,
        complete_var,
        shell,
        completion_path,
        bashrc_path,
        completion_init_lines,
    )


def install_zsh(*, prog_name: str, complete_var: str, shell: str) -> Path:
    # Setup Zsh and load ~/.zfunc
    completion_path = Path.home() / f".zfunc/_{prog_name}"
    zshrc_path = Path.home() / ".zshrc"
    completion_init_lines = [
        "autoload -Uz compinit",
        "zstyle ':completion:*' menu select",
        "fpath+=~/.zfunc; compinit",
    ]
    return install_helper(
        prog_name,
        complete_var,
        shell,
        completion_path,
        zshrc_path,
        completion_init_lines,
    )


def install_fish(*, prog_name: str, complete_var: str, shell: str) -> Path:
    path_obj = Path.home() / f".config/fish/completions/{prog_name}.fish"
    parent_dir: Path = path_obj.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    script_content = get_completion_script(
        prog_name=prog_name, complete_var=complete_var, shell=shell
    )
    path_obj.write_text(f"{script_content}\n")
    return path_obj


def install_powershell(*, prog_name: str, complete_var: str, shell: str) -> Path:
    subprocess.run(
        [
            shell,
            "-Command",
            "Set-ExecutionPolicy",
            "Unrestricted",
            "-Scope",
            "CurrentUser",
        ]
    )
    result = subprocess.run(
        [shell, "-NoProfile", "-Command", "echo", "$profile"],
        text=True,
        stdout=subprocess.PIPE,
    )
    if result.returncode != 0:
        click.echo("Couldn't get PowerShell user profile", err=True)
        raise click.exceptions.Exit(result.returncode)
    path_str = result.stdout
    path_obj = Path(path_str.strip())
    parent_dir: Path = path_obj.parent
    parent_dir.mkdir(parents=True, exist_ok=True)
    script_content = get_completion_script(
        prog_name=prog_name, complete_var=complete_var, shell=shell
    )
    path_obj.write_text(f"{script_content}\n")
    with path_obj.open(mode="a") as f:
        f.write(f"{script_content}\n")
    return path_obj


def install(
    shell: Optional[str] = None,
    prog_name: Optional[str] = None,
    complete_var: Optional[str] = None,
) -> Tuple[str, Path]:
    prog_name = prog_name or click.get_current_context().find_root().info_name
    assert prog_name
    if complete_var is None:
        complete_var = "_{}_COMPLETE".format(prog_name.replace("-", "_").upper())
    if shell is None and shellingham is not None:
        shell, _ = shellingham.detect_shell()
    if shell == "bash":
        installed_path = install_bash(
            prog_name=prog_name, complete_var=complete_var, shell=shell
        )
        return shell, installed_path
    elif shell == "zsh":
        installed_path = install_zsh(
            prog_name=prog_name, complete_var=complete_var, shell=shell
        )
        return shell, installed_path
    elif shell == "fish":
        installed_path = install_fish(
            prog_name=prog_name, complete_var=complete_var, shell=shell
        )
        return shell, installed_path
    elif shell in {"powershell", "pwsh"}:
        installed_path = install_powershell(
            prog_name=prog_name, complete_var=complete_var, shell=shell
        )
        return shell, installed_path
    else:
        click.echo(f"Shell {shell} is not supported.")
        raise click.exceptions.Exit(1)


def install_callback(ctx: click.Context, param: click.Parameter, value: Any) -> Any:
    if not value or ctx.resilient_parsing:
        return value
    if value == "auto":
        shell, path = install()
    else:
        shell, path = install(shell=value)
    click.secho(f"{shell} completion installed in {path}", fg="green")
    click.echo("Completion will take effect once you restart the terminal")
    sys.exit(0)


@click.shell_completion.add_completion_class
class PowerShellComplete(click.shell_completion.ShellComplete):
    name = Shells.powershell.value
    source_template = _SOURCE_POWERSHELL

    def source_vars(self) -> Dict[str, Any]:
        return {
            "complete_func": self.func_name,
            "complete_var": self.complete_var,
            "prog_name": self.prog_name,
        }

    def get_completion_args(self) -> Tuple[List[str], str]:
        completion_args = os.getenv("_KART_COMPLETE_ARGS", "")
        incomplete = os.getenv("_KART_COMPLETE_WORD_TO_COMPLETE", "")
        cwords = click.parser.split_arg_string(completion_args)
        args = cwords[1:]
        return args, incomplete

    def format_completion(self, item: click.shell_completion.CompletionItem) -> str:
        return f"{item.value} -- {item.help or ' '}"
