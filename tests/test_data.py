import json
from pathlib import Path
from kart.exceptions import NO_REPOSITORY
from kart import subprocess_util as subprocess
import pytest


@pytest.mark.parametrize("output_format", ("text", "json"))
@pytest.mark.parametrize(
    "extra_flag", ("--with-dataset-types", "--without-dataset-types")
)
def test_data_ls(output_format, extra_flag, data_archive_readonly, cli_runner):
    # All datasets now support getting metadata in either V1 or V2 format,
    # but if you don't specify a particular item, they will show all V2 items -
    # these are more self-explanatory to an end-user.
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["data", "ls", "-o", output_format, extra_flag])
        assert r.exit_code == 0, r
        if extra_flag == "--with-dataset-types":
            if output_format == "text":
                assert r.stdout.splitlines() == ["nz_pa_points_topo_150k\t(table.v3)"]
            else:
                output = json.loads(r.stdout)
                assert output == {
                    "kart.data.ls/v2": [
                        {
                            "path": "nz_pa_points_topo_150k",
                            "type": "table",
                            "version": 3,
                        }
                    ]
                }
        else:
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
            "The commit at HEAD has no datasets.",
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


def test_nonkart_git_repo(cli_runner, tmp_path, chdir):
    repo_path = tmp_path / "nonkart-git"
    subprocess.check_call(["git", "init", str(repo_path)])
    with chdir(repo_path):
        # Kart should recognize that an empty Git repo is not a Kart repo.
        r = cli_runner.invoke(["log"])
        assert r.exit_code == NO_REPOSITORY, r.stderr
        assert r.stderr.splitlines() == [
            "Error: Current directory is not an existing Kart repository"
        ]

        # Create an empty commit, just so that there's something in the ODB.
        subprocess.check_call(
            ["git", "commit", "--allow-empty", "-m", "empty-commit"],
            env_overrides={"GIT_INDEX_FILE": None},
        )

        # Kart should recognize that a non-empty Git repo is also not a Kart repo.
        r = cli_runner.invoke(["log"])
        assert r.exit_code == NO_REPOSITORY, r.stderr


def test_nonkart_kart_repo(cli_runner, tmp_path, chdir):
    repo_path = tmp_path / "nonkart-kart"
    r = cli_runner.invoke(["init", repo_path])
    assert r.exit_code == 0
    with chdir(repo_path):
        # At this point we have a valid empty Kart repo - it has a .kart folder and nothing in the ODB.
        # But if we add an empty commit, then the ODB is now populated with "non-Kart" data -
        # it contains neither Kart datasets nor the ".kart.repostructure.version" marker.
        # You can get similar repos with a .kart folder but no Kart data by using Kart to clone a git repo
        # (see https://github.com/koordinates/kart/issues/918)

        r = cli_runner.invoke(["git", "commit", "--allow-empty", "-m", "empty-commit"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["log"])
        assert r.exit_code == NO_REPOSITORY
        assert r.stderr.splitlines() == [
            "Error: Current directory is not a Kart repository (no Kart datasets found at HEAD commit)"
        ]
