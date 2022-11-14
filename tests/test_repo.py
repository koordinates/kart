import os
import subprocess

from kart.repo import KartRepo, LOCKED_GIT_INDEX_CONTENTS


def test_init_repository(tmp_path):
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()

    repo = KartRepo.init_repository(repo_path)

    assert (repo_path / ".git").is_file()
    assert (repo_path / ".git").read_text() == "gitdir: .kart\n"
    assert (repo_path / ".kart").is_dir()
    assert (repo_path / ".kart" / "HEAD").exists()

    assert (repo_path / ".kart" / "index").read_bytes() == LOCKED_GIT_INDEX_CONTENTS[
        "kart"
    ]

    assert repo.config.get_int("kart.repostructure.version") == 3
    assert repo.config["kart.workingcopy.location"] == "test_repo.gpkg"
    assert repo.config.get_bool("core.bare") is False


def test_git_disabled(tmp_path, cli_runner, chdir):
    """ Create an empty Kart repository. """
    repo_path = tmp_path / "test_repo"
    repo_path.mkdir()

    # empty dir
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r
    assert (repo_path / ".kart" / "HEAD").exists()

    repo = KartRepo(repo_path)

    with chdir(repo_path):
        # env={} means don't inherit the environment of this process so it behaves as it would if
        # the user typed git at the command line without our GIT_* settings in place.
        from kart import git_bin_path
        git_bin = os.path.join(git_bin_path, "git")

        r = subprocess.run([git_bin, "gc"], capture_output=True, encoding="utf-8", env={})
        assert r.returncode != 0
        assert "index uses kart extension, which we do not understand" in r.stderr
        assert "fatal:" in r.stderr

        # Whereas this runs with our custom environment, including GIT_INDEX_FILE
        r = subprocess.run([git_bin, "gc"], capture_output=True, encoding="utf-8")
        assert r.returncode == 0, r.stderr

        r = subprocess.run([git_bin, "gc"], capture_output=True, encoding="utf-8", env={})
        assert r.returncode != 0
        assert "index uses kart extension, which we do not understand" in r.stderr
        assert "fatal:" in r.stderr

    # Internally, this runs git-gc with the unlocked git index.
    repo.gc()

    # git-gc shouldn't create an index where there wasn't one already.
    assert not (repo_path / ".kart" / "unlocked_index").exists()
