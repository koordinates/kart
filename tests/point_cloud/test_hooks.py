from kart.repo import KartRepo
from kart.lfs_util import install_lfs_hooks


def test_install_lfs_hooks(tmp_path, cli_runner, chdir):
    """Create an empty Kart repository."""
    repo_path = tmp_path / "repo"
    repo_path.mkdir()

    # empty dir
    r = cli_runner.invoke(["init", str(repo_path)])
    assert r.exit_code == 0, r

    repo = KartRepo(repo_path)
    install_lfs_hooks(repo)
