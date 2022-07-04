import os
from pathlib import Path

from kart.cli_util import OutputFormatType
from kart.completion_shared import conflict_completer, ref_completer, path_completer

DIFF_OUTPUT_FORMATS = ["text", "geojson", "json", "json-lines", "quiet", "html"]
SHOW_OUTPUT_FORMATS = DIFF_OUTPUT_FORMATS


def test_completion_install_no_shell(cli_runner):
    r = cli_runner.invoke(["config", "--install-tab-completion"])
    assert "Error: Option '--install-tab-completion' requires an argument" in r.stderr


def test_completion_install_bash(cli_runner):
    bash_completion_path: Path = Path.home() / ".bashrc"
    text = ""
    if bash_completion_path.is_file():
        text = bash_completion_path.read_text()
    r = cli_runner.invoke(["config", "--install-tab-completion", "bash"])
    new_text = bash_completion_path.read_text()
    bash_completion_path.write_text(text)
    install_source = os.path.join(".bash_completions", "cli.sh")
    assert install_source not in text
    assert install_source in new_text
    assert "completion installed in" in r.stdout
    assert "Completion will take effect once you restart the terminal" in r.stdout
    install_source_path = Path.home() / install_source
    assert install_source_path.is_file()
    install_content = install_source_path.read_text()
    install_source_path.unlink()
    assert "complete -o nosort -F _cli_completion cli" in install_content


def test_completion_install_zsh(cli_runner):
    completion_path: Path = Path.home() / ".zshrc"
    text = ""
    if not completion_path.is_file():
        completion_path.write_text('echo "custom .zshrc"')
    if completion_path.is_file():
        text = completion_path.read_text()
    r = cli_runner.invoke(["config", "--install-tab-completion", "zsh"])
    new_text = completion_path.read_text()
    completion_path.write_text(text)
    zfunc_fragment = "fpath+=~/.zfunc"
    assert zfunc_fragment in new_text
    assert "completion installed in" in r.stdout
    assert "Completion will take effect once you restart the terminal" in r.stdout
    install_source_path = Path.home() / os.path.join(".zfunc", "_cli")
    assert install_source_path.is_file()
    install_content = install_source_path.read_text()
    install_source_path.unlink()
    assert "compdef _cli_completion cli" in install_content


def test_completion_install_fish(cli_runner):
    completion_path: Path = Path.home() / os.path.join(
        ".config", "fish", "completions", "cli.fish"
    )
    r = cli_runner.invoke(["config", "--install-tab-completion", "fish"])
    new_text = completion_path.read_text()
    completion_path.unlink()
    assert "complete --no-files --command cli" in new_text
    assert "completion installed in" in r.stdout
    assert "Completion will take effect once you restart the terminal" in r.stdout


def test_ref_completer(data_archive, cli_runner):
    with data_archive("points") as _:
        r = cli_runner.invoke(["checkout", "-b", "one"])
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["checkout", "-b", "two"])
        assert r.exit_code == 0, r.stderr

        assert ref_completer() == [
            "one",
            "two",
            "main",
        ]
        assert ref_completer(incomplete="r") == [
            "refs/heads/one",
            "refs/heads/two",
            "refs/heads/main",
        ]


def test_path_completer(data_archive, cli_runner):
    with data_archive("points") as _:
        assert path_completer() == [
            "nz_pa_points_topo_150k",
        ]
        assert path_completer(incomplete="nz") == [
            "nz_pa_points_topo_150k",
        ]


def test_conflict_completer(data_archive, cli_runner):
    with data_archive("conflicts/points.tgz") as _:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r.stderr
        assert conflict_completer() == [
            "nz_pa_points_topo_150k",
        ]
        assert conflict_completer(incomplete="nz_pa_points_topo_150k") == [
            "nz_pa_points_topo_150k:feature:3",
            "nz_pa_points_topo_150k:feature:4",
            "nz_pa_points_topo_150k:feature:5",
            "nz_pa_points_topo_150k:feature:98001",
        ]
        assert conflict_completer(incomplete="nz_pa_points_topo_150k:feature:9") == [
            "nz_pa_points_topo_150k:feature:98001",
        ]


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
