from glob import glob
import subprocess
import pytest

from kart.exceptions import INVALID_FILE_FORMAT
from kart.repo import KartRepo

DUMMY_REPO = "git@example.com/example.git"

# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def requires_pdal():
    has_pdal = False
    try:
        import pdal

        assert pdal.Pipeline
        has_pdal = True
    except ModuleNotFoundError:
        pass

    pytest.helpers.feature_assert_or_skip(
        "pdal package installed", "KART_EXPECT_PDAL", has_pdal, ci_require=False
    )


@pytest.fixture(scope="session")
def requires_git_lfs():
    r = subprocess.run(["git", "lfs", "--version"])
    has_git_lfs = r.returncode == 0

    pytest.helpers.feature_assert_or_skip(
        "Git LFS installed", "KART_EXPECT_GIT_LFS", has_git_lfs, ci_require=False
    )


def test_import_single_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/autzen.tgz") as autzen:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(["point-cloud-import", f"{autzen}/autzen.las"])
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            stdout = subprocess.check_output(
                ["kart", "lfs", "push", "origin", "--all", "--dry-run"], encoding="utf8"
            )
            assert stdout.splitlines() == [
                "push 068a349959a45957184606a0442f8dd69aef24543e11963bc63835301df532f5 => autzen/.point-cloud-dataset.v1/tiles/0d/autzen.las"
            ]


def test_import_several_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/auckland.tgz") as auckland:
        repo_path = tmp_path / "point-cloud-repo"
        r = cli_runner.invoke(["init", repo_path])
        assert r.exit_code == 0

        repo = KartRepo(repo_path)
        with chdir(repo_path):
            r = cli_runner.invoke(
                ["point-cloud-import", *glob(f"{auckland}/auckland_*.laz")]
            )
            assert r.exit_code == 0, r.stderr

            r = cli_runner.invoke(["remote", "add", "origin", DUMMY_REPO])
            assert r.exit_code == 0, r.stderr
            repo.config[f"lfs.{DUMMY_REPO}/info/lfs.locksverify"] = False

            stdout = subprocess.check_output(
                ["kart", "lfs", "push", "origin", "--all", "--dry-run"], encoding="utf8"
            )
            assert stdout.splitlines() == [
                "push 6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c => auckland/.point-cloud-dataset.v1/tiles/14/auckland_0_0.laz",
                "push 46d84ea32dddeb29e4189febb2d2ab22b285e568fb8179ee96d17615502a7f3b => auckland/.point-cloud-dataset.v1/tiles/23/auckland_2_1.laz",
                "push 1a4b8ac69123725e705657b9fd8000cd0ad33cc92f57992cc5902081700a697d => auckland/.point-cloud-dataset.v1/tiles/35/auckland_1_0.laz",
                "push 06bd15fbb6616cf63a4a410c5ba4666dab76177a58cb99c3fa2afb46c9dd6379 => auckland/.point-cloud-dataset.v1/tiles/35/auckland_1_3.laz",
                "push 09701813661e369395d088a9a44f1201200155e652a8b6e291e71904f45e32a6 => auckland/.point-cloud-dataset.v1/tiles/44/auckland_3_0.laz",
                "push 2b54321de47d48c399a679c647cba20798399d604f3b350e6dcd1ce395d61031 => auckland/.point-cloud-dataset.v1/tiles/4c/auckland_0_1.laz",
                "push 111579edfe022ebfd3388cc47d911c16c72c7ebd84c32a7a0c1dab6ed9ec896a => auckland/.point-cloud-dataset.v1/tiles/52/auckland_0_2.laz",
                "push d89966fb10b30d6987955ae1b97c752ba875de89da1881e2b05820878d17eab9 => auckland/.point-cloud-dataset.v1/tiles/69/auckland_1_1.laz",
                "push 74f144617acd46b95c02b3e4f3030fd2029476fab795b5a9a99a13c1ee184e36 => auckland/.point-cloud-dataset.v1/tiles/8b/auckland_2_3.laz",
                "push 82563ccbbc55ba4b063ef6e8a41c031e8af508a6be6fec400565b88096dd1501 => auckland/.point-cloud-dataset.v1/tiles/96/auckland_2_0.laz",
                "push a4acd08ca3763823df67fc0d4e45ce0e39525b49e31d8f20babc74d208e481a5 => auckland/.point-cloud-dataset.v1/tiles/9d/auckland_0_3.laz",
                "push 1ca14275bbd4b74fedc00b64687f85776e6ebd32aceda413566ab7d6694ccff7 => auckland/.point-cloud-dataset.v1/tiles/b5/auckland_3_1.laz",
                "push 4190c9056b732fadd6e86500e93047a787d88812f7a4af21c7759d92d1d48954 => auckland/.point-cloud-dataset.v1/tiles/ba/auckland_3_3.laz",
                "push d47dad83c4259e4ff2b6efbb8f1262dbd903c70794d928c43e98c45edbcd927c => auckland/.point-cloud-dataset.v1/tiles/d5/auckland_1_2.laz",
                "push 03e3d4dc6fc8e75c65ffdb39b630ffe26e4b95982b9765c919e34fb940e66fc0 => auckland/.point-cloud-dataset.v1/tiles/d5/auckland_3_2.laz",
                "push 7fdde415acb376f5dcad93fcb6a9ef9cb9b1378edeb7f0c5ec6fd8beacdabedd => auckland/.point-cloud-dataset.v1/tiles/fc/auckland_2_2.laz",
            ]


def test_import_mismatched_las(
    tmp_path, chdir, cli_runner, data_archive_readonly, requires_pdal, requires_git_lfs
):
    # Using postgres here because it has the best type preservation
    with data_archive_readonly("point-cloud/auckland.tgz") as auckland:
        with data_archive_readonly("point-cloud/autzen.tgz") as autzen:
            repo_path = tmp_path / "point-cloud-repo"
            r = cli_runner.invoke(["init", repo_path])
            assert r.exit_code == 0
            with chdir(repo_path):
                r = cli_runner.invoke(
                    [
                        "point-cloud-import",
                        *glob(f"{auckland}/auckland_*.laz"),
                        f"{autzen}/autzen.las",
                    ]
                )
                assert r.exit_code == INVALID_FILE_FORMAT
                assert "Non-homogenous" in r.stderr
