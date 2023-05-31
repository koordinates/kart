import os
import platform
from pathlib import Path
import subprocess

import shellingham

from kart.cli_util import OutputFormatType
from kart.completion_shared import (
    conflict_completer,
    ref_completer,
    repo_path_completer,
)

DIFF_OUTPUT_FORMATS = ["text", "geojson", "json", "json-lines", "quiet", "html"]
SHOW_OUTPUT_FORMATS = DIFF_OUTPUT_FORMATS


def test_completion_install_bash(cli_runner):
    bash_completion_path = Path.home() / ".bashrc"
    bash_profile = Path.home() / ".bash_profile"
    if platform.system() == "Darwin" and bash_profile.exists():
        bash_completion_path = bash_profile
    text = ""
    if bash_completion_path.is_file():
        text = bash_completion_path.read_text()
    r = cli_runner.invoke(["install", "tab-completion", "--shell", "bash"])
    new_text = bash_completion_path.read_text()
    bash_completion_path.write_text(text)
    install_source = os.path.join(".bash_completions", "kart.sh")
    assert install_source not in text
    assert install_source in new_text
    assert "completion installed in" in r.stdout
    assert "Completion will take effect once you restart the terminal" in r.stdout
    install_source_path = Path.home() / install_source
    assert install_source_path.is_file()
    install_content = install_source_path.read_text()
    install_source_path.unlink()
    assert "complete -o nosort -F _kart_completion kart" in install_content


def test_completion_install_zsh(cli_runner):
    completion_path: Path = Path.home() / ".zshrc"
    text = ""
    if not completion_path.is_file():
        completion_path.write_text('echo "custom .zshrc"')
    if completion_path.is_file():
        text = completion_path.read_text()
    r = cli_runner.invoke(["install", "tab-completion", "--shell", "zsh"])
    new_text = completion_path.read_text()
    completion_path.write_text(text)
    zfunc_fragment = "fpath+=~/.zfunc"
    assert zfunc_fragment in new_text
    assert "completion installed in" in r.stdout
    assert "Completion will take effect once you restart the terminal" in r.stdout
    install_source_path = Path.home() / os.path.join(".zfunc", "_kart")
    assert install_source_path.is_file()
    install_content = install_source_path.read_text()
    install_source_path.unlink()
    assert "compdef _kart_completion kart" in install_content


def test_completion_install_fish(cli_runner):
    completion_path: Path = Path.home() / os.path.join(
        ".config", "fish", "completions", "kart.fish"
    )
    r = cli_runner.invoke(["install", "tab-completion", "--shell", "fish"])
    new_text = completion_path.read_text()
    completion_path.unlink()
    assert "complete --no-files --command kart" in new_text
    assert "completion installed in" in r.stdout
    assert "Completion will take effect once you restart the terminal" in r.stdout


def test_ref_completer(data_archive, cli_runner):
    with data_archive("points") as _:
        r = cli_runner.invoke(["checkout", "-b", "one"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "-b", "two"])
        assert r.exit_code == 0, r.stderr

        assert ref_completer() == set(
            [
                "one",
                "two",
                "main",
            ]
        )
        assert ref_completer(incomplete="r") == set(
            [
                "refs/heads/one",
                "refs/heads/two",
                "refs/heads/main",
            ]
        )


def test_path_completer(data_archive, cli_runner):
    with data_archive("points") as _:
        assert repo_path_completer() == set(["nz_pa_points_topo_150k"])
        assert repo_path_completer(incomplete="nz") == set(["nz_pa_points_topo_150k"])


def test_conflict_completer(data_archive, cli_runner):
    with data_archive("conflicts/points.tgz") as _:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r.stderr
        assert conflict_completer() == set(
            [
                "nz_pa_points_topo_150k:feature:3",
                "nz_pa_points_topo_150k:feature:4",
                "nz_pa_points_topo_150k:feature:5",
                "nz_pa_points_topo_150k:feature:98001",
            ]
        )
        assert conflict_completer(incomplete="nz_pa_points_topo_150k") == set(
            [
                "nz_pa_points_topo_150k:feature:3",
                "nz_pa_points_topo_150k:feature:4",
                "nz_pa_points_topo_150k:feature:5",
                "nz_pa_points_topo_150k:feature:98001",
            ]
        )
        assert conflict_completer(incomplete="nz_pa_points_topo_150k:feature:9") == set(
            [
                "nz_pa_points_topo_150k:feature:98001",
            ]
        )


def test_show_output_format_completer(data_archive_readonly):
    with data_archive_readonly("polygons"):
        output_type = OutputFormatType(
            output_types=SHOW_OUTPUT_FORMATS, allow_text_formatstring=False
        )
        assert [
            type.value for type in output_type.shell_complete()
        ] == SHOW_OUTPUT_FORMATS
        assert [type.value for type in output_type.shell_complete(incomplete="j")] == [
            "json",
            "json-lines",
        ]


def test_completion_install_powershell(cli_runner, mocker):
    completion_path: Path = Path.home() / os.path.join(
        ".config", "powershell", "Microsoft.PowerShell_profile.ps1"
    )
    text = ""
    if completion_path.is_file():
        text = completion_path.read_text()

    mocker.patch.object(
        shellingham, "detect_shell", return_value=("pwsh", "/usr/bin/pwsh")
    )
    mocker.patch.object(
        subprocess,
        "run",
        return_value=subprocess.CompletedProcess(
            ["pwsh"], returncode=0, stdout=str(completion_path)
        ),
    )
    result = cli_runner.invoke(["install", "tab-completion"])
    install_script = "Register-ArgumentCompleter -Native -CommandName mocked-typer-testing-app -ScriptBlock $scriptblock"
    parent: Path = completion_path.parent
    parent.mkdir(parents=True, exist_ok=True)
    completion_path.write_text(install_script)
    new_text = completion_path.read_text()
    completion_path.write_text(text)
    assert install_script not in text
    assert install_script in new_text
    assert "completion installed in" in result.stdout
    assert "Completion will take effect once you restart the terminal" in result.stdout
