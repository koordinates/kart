import subprocess

from sno.sno_repo import SnoRepo, LockedGitIndex
from sno.repository_version import DEFAULT_REPO_VERSION


def test_init_repository(tmp_path):
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()

    sno_repo = SnoRepo.init_repository(repo_path, DEFAULT_REPO_VERSION)

    assert (repo_path / ".git").is_file()
    assert (repo_path / ".git").read_text() == "gitdir: .sno\n"
    assert (repo_path / ".sno").is_dir()
    assert (repo_path / ".sno" / "HEAD").exists()

    assert (
        repo_path / ".sno" / "index"
    ).read_bytes() == LockedGitIndex.LOCKED_EMPTY_GIT_INDEX

    assert sno_repo.config.get_int("sno.repository.version") == 2
    assert sno_repo.config["sno.workingcopy.path"] == "test_repo.gpkg"
    assert sno_repo.config.get_bool("core.bare") is False


def test_git_disabled(tmp_path, cli_runner, chdir):
    """ Create an empty Sno repository. """
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()

    # empty dir
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r
    assert (repo_path / ".sno" / "HEAD").exists()

    repo = SnoRepo(repo_path)

    with chdir(repo_path):
        # env={} means we don't inherit the environment of this process,
        # so it behaves as it would if the user typed it at the command line.
        r = subprocess.run(["git", "gc"], capture_output=True, encoding="utf-8", env={})
        assert r.returncode != 0
        assert "index uses .sno extension, which we do not understand" in r.stderr
        assert "fatal:" in r.stderr

        # Whereas this runs with our custom environment, including GIT_INDEX_FILE
        r = subprocess.run(["git", "gc"], capture_output=True, encoding="utf-8")
        assert r.returncode == 0, r.stderr

        r = subprocess.run(["git", "gc"], capture_output=True, encoding="utf-8", env={})
        assert r.returncode != 0
        assert "index uses .sno extension, which we do not understand" in r.stderr
        assert "fatal:" in r.stderr

    # Internally, this runs git-gc with the unlocked git index.
    repo.gc()

    # git-gc shouldn't create an index where there wasn't one already.
    assert not (repo_path / ".sno" / "unlocked_index").exists()
