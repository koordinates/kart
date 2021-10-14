import json
from pathlib import Path
import pytest


@pytest.mark.parametrize("output_format", ("text", "json"))
def test_data_ls(output_format, data_archive_readonly, cli_runner):
    # All datasets now support getting metadata in either V1 or V2 format,
    # but if you don't specify a particular item, they will show all V2 items -
    # these are more self-explanatory to an end-user.
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["data", "ls", "-o", output_format])
        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.splitlines() == ["nz_pa_points_topo_150k"]
        else:
            output = json.loads(r.stdout)
            assert output == {"kart.data.ls/v1": ["nz_pa_points_topo_150k"]}


@pytest.mark.parametrize("output_format", ("text", "json"))
def test_data_ls_empty(output_format, tmp_path, cli_runner, chdir):
    repo_path = tmp_path / "emptydir"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0
    with chdir(repo_path):
        r = cli_runner.invoke(["data", "ls", "-o", output_format])
        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.splitlines() == [
                "Empty repository.",
                '  (use "kart import" to add some data)',
            ]
        else:
            output = json.loads(r.stdout)
            assert output == {"kart.data.ls/v1": []}


def test_data_ls_with_ref(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["data", "ls", "-o", "json", "HEAD^"])
        assert r.exit_code == 0, r

        output = json.loads(r.stdout)
        assert output == {"kart.data.ls/v1": ["nz_pa_points_topo_150k"]}


def test_data_rm(data_archive, cli_runner):
    with data_archive("points"):
        r = cli_runner.invoke(["data", "ls"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["nz_pa_points_topo_150k"]

        r = cli_runner.invoke(
            ["data", "rm", "nz_pa_points_topo_150k", "-m", "deletion"]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["data", "ls"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            'The commit at HEAD has no datasets.',
            '  (use "kart import" to add some data)',
        ]

        r = cli_runner.invoke(["reset", "HEAD^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["data", "ls"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["nz_pa_points_topo_150k"]


@pytest.mark.parametrize("output_format", ("text", "json"))
@pytest.mark.parametrize("version", (0, 1, 2, 3))
def test_data_version(version, output_format, data_archive_readonly, cli_runner):
    archive_paths = {
        0: Path("upgrade") / "v0" / "points0.snow.tgz",
        1: Path("upgrade") / "v1" / "points.tgz",
        2: Path("upgrade") / "v2.kart" / "points.tgz",
        3: Path("points.tgz"),
    }
    branding = {0: "sno", 1: "sno", 2: "kart", 3: "kart"}[version]

    with data_archive_readonly(archive_paths[version]):
        r = cli_runner.invoke(["data", "version", "-o", output_format])
        assert r.exit_code == 0, r
        if output_format == "text":
            assert r.stdout.splitlines()[0].endswith(str(version))
        else:
            output = json.loads(r.stdout)
            assert output == {
                "repostructure.version": version,
                "localconfig.branding": branding,
            }
