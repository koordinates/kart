import re
import shutil

from kart.lfs_util import get_oid_and_size_of_file
from kart.repo import KartRepo


def test_merge_and_resolve_conflicts(cli_runner, data_archive, requires_pdal):
    with data_archive("point-cloud/conflicts.tgz") as repo_path:
        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            'Merging branch "theirs_branch" into ours_branch',
            "Conflicts found:",
            "",
            "auckland:",
            "    auckland:tile: 1 conflicts",
            "",
            'Repository is now in "merging" state.',
            "View conflicts with `kart conflicts` and resolve them with `kart resolve`.",
            "Once no conflicts remain, complete this merge with `kart merge --continue`.",
            "Or use `kart merge --abort` to return to the previous state.",
        ]

        r = cli_runner.invoke(["conflicts"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "auckland:",
            "    auckland:tile:",
            "        auckland:tile:auckland_0_0:",
            "            auckland:tile:auckland_0_0:ancestor:",
            "                                    name = auckland_0_0.copc.laz",
            "                             crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
            "                                  format = laz-1.4/copc-1.0",
            "                            nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "                              pointCount = 4231",
            "                               sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "                                     oid = sha256:adbc1dc7fc99c88fcb627b9c40cdb56c211b791fe9cf83fe066b1a9932c12569",
            "                                    size = 54396",
            "            auckland:tile:auckland_0_0:ours:",
            "                                    name = auckland_0_0.copc.laz",
            "                             crs84Extent = POLYGON((174.7604586 -36.8328845,174.7602522 -36.8238787,174.7714478 -36.8237124,174.7716555 -36.8327181,174.7604586 -36.8328845))",
            "                                  format = laz-1.4/copc-1.0",
            "                            nativeExtent = 1756987.83,1757986.74,5922219.95,5923219.43,-1.54,10.59",
            "                              pointCount = 1599",
            "                                     oid = sha256:583789bcea43177dbba446574f00f817b2f89782fcf71709d911b2ad10872d0e",
            "                                    size = 18317",
            "            auckland:tile:auckland_0_0:theirs:",
            "                                    name = auckland_0_0.copc.laz",
            "                             crs84Extent = POLYGON((174.7494680 -36.8420542,174.7492629 -36.8330539,174.7604509 -36.8328887,174.7606572 -36.8418890,174.7494680 -36.8420542))",
            "                                  format = laz-1.4/copc-1.0",
            "                            nativeExtent = 1755989.03,1756987.13,5921220.62,5922219.49,-1.48,35.15",
            "                              pointCount = 1558",
            "                                     oid = sha256:8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794",
            "                                    size = 19975",
            "",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        lines = r.stdout.splitlines()
        assert "--- auckland:tile:auckland_0_0" in lines
        assert "+++ auckland:tile:auckland_0_0.ancestor" in lines
        assert "+++ auckland:tile:auckland_0_0.ours" in lines
        assert "+++ auckland:tile:auckland_0_0.theirs" in lines

        # Check the conflict versions were written to the working copy for the user to compare:
        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.ancestor.copc.laz"
        ) == ("adbc1dc7fc99c88fcb627b9c40cdb56c211b791fe9cf83fe066b1a9932c12569", 54396)
        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.ours.copc.laz"
        ) == ("583789bcea43177dbba446574f00f817b2f89782fcf71709d911b2ad10872d0e", 18317)
        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.theirs.copc.laz"
        ) == ("8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794", 19975)

        assert not (repo_path / "auckland" / "auckland_0_0.copc.laz").exists()

        r = cli_runner.invoke(
            ["resolve", "auckland:tile:auckland_0_0", "--with=theirs"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Resolved 1 conflict. 0 conflicts to go.",
            "Use `kart merge --continue` to complete the merge",
        ]

        # Check the conflict versions were cleaned up and auckland_0_0 in the workdir is now version "theirs".
        assert not (repo_path / "auckland" / "auckland_0_0.ancestor.copc.laz").exists()
        assert not (repo_path / "auckland" / "auckland_0_0.ours.copc.laz").exists()
        assert not (repo_path / "auckland" / "auckland_0_0.theirs.copc.laz").exists()

        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.copc.laz"
        ) == ("8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794", 19975)

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0, r.stderr
        lines = r.stdout.splitlines()
        assert "--- auckland:tile:auckland_0_0" in lines
        assert "+++ auckland:tile:auckland_0_0" in lines

        assert "+++ auckland:tile:auckland_0_0.ancestor" not in lines
        assert "+++ auckland:tile:auckland_0_0.ours" not in lines
        assert "+++ auckland:tile:auckland_0_0.theirs" not in lines

        r = cli_runner.invoke(
            [
                "merge",
                "--continue",
                "-m",
                'Merge branch "theirs_branch" into ours_branch',
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[:2] == [
            'Merging branch "theirs_branch" into ours_branch',
            "No conflicts!",
        ]
        assert re.fullmatch(
            r"Merge committed as [0-9a-z]{40}", r.stdout.splitlines()[2]
        )
        assert r.stdout.splitlines()[3] == "Updating file-system working copy ..."

        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.copc.laz"
        ) == ("8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794", 19975)


def test_resolve_conflict_with_workingcopy(cli_runner, data_archive, requires_pdal):
    with data_archive("point-cloud/conflicts.tgz") as repo_path:
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["conflicts", "-ss"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "auckland:",
            "    auckland:tile: 1 conflicts",
            "",
        ]

        shutil.copy(
            repo.workdir_path / "auckland" / "auckland_3_3.copc.laz",
            repo.workdir_path / "auckland" / "auckland_0_0.copc.laz",
        )
        r = cli_runner.invoke(
            ["resolve", "auckland:tile:auckland_0_0", "--with=workingcopy"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Resolved 1 conflict. 0 conflicts to go.",
            "Use `kart merge --continue` to complete the merge",
        ]

        r = cli_runner.invoke(
            [
                "merge",
                "--continue",
                "-m",
                'Merge with "theirs_branch"',
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show", "HEAD", "auckland:tile:auckland_0_0"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[4:] == [
            '    Merge with "theirs_branch"',
            "",
            "--- auckland:tile:auckland_0_0",
            "+++ auckland:tile:auckland_0_0",
            "-                              crs84Extent = POLYGON((174.7604586 -36.8328845,174.7602522 -36.8238787,174.7714478 -36.8237124,174.7716555 -36.8327181,174.7604586 -36.8328845))",
            "+                              crs84Extent = POLYGON((174.7726438 -36.8236912,174.7726418 -36.8236049,174.7819653 -36.8234655,174.7819673 -36.8235518,174.7726438 -36.8236912))",
            "-                             nativeExtent = 1756987.83,1757986.74,5922219.95,5923219.43,-1.54,10.59",
            "+                             nativeExtent = 1758093.46,1758925.34,5923219.8,5923229.38,-1.28,9.8",
            "-                               pointCount = 1599",
            "+                               pointCount = 29",
            "-                                      oid = sha256:583789bcea43177dbba446574f00f817b2f89782fcf71709d911b2ad10872d0e",
            "+                                      oid = sha256:0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6",
            "-                                     size = 18317",
            "+                                     size = 2314",
        ]

        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.copc.laz"
        ) == ("0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6", 2314)


def test_resolve_conflict_with_file(cli_runner, data_archive, requires_pdal, tmpdir):
    with data_archive("point-cloud/conflicts.tgz") as repo_path:
        repo = KartRepo(repo_path)

        r = cli_runner.invoke(["merge", "theirs_branch"])
        assert r.exit_code == 0, r.stderr

        shutil.copy(
            repo.workdir_path / "auckland" / "auckland_1_3.copc.laz",
            tmpdir / "resolution.copc.laz",
        )
        r = cli_runner.invoke(
            [
                "resolve",
                "auckland:tile:auckland_0_0",
                f"--with-file={tmpdir / 'resolution.copc.laz'}",
            ]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Resolved 1 conflict. 0 conflicts to go.",
            "Use `kart merge --continue` to complete the merge",
        ]

        r = cli_runner.invoke(
            [
                "merge",
                "--continue",
                "-m",
                'Merge with "theirs_branch"',
            ]
        )
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show", "HEAD", "auckland:tile:auckland_0_0"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[4:] == [
            '    Merge with "theirs_branch"',
            "",
            "--- auckland:tile:auckland_0_0",
            "+++ auckland:tile:auckland_0_0",
            "-                              crs84Extent = POLYGON((174.7604586 -36.8328845,174.7602522 -36.8238787,174.7714478 -36.8237124,174.7716555 -36.8327181,174.7604586 -36.8328845))",
            "+                              crs84Extent = POLYGON((174.7492651 -36.8240306,174.7492633 -36.8239502,174.7591613 -36.8238041,174.7591631 -36.8238846,174.7492651 -36.8240306))",
            "-                             nativeExtent = 1756987.83,1757986.74,5922219.95,5923219.43,-1.54,10.59",
            "+                             nativeExtent = 1756007.55,1756890.68,5923220.57,5923229.5,-1.28,30.4",
            "-                               pointCount = 1599",
            "+                               pointCount = 17",
            "-                                      oid = sha256:583789bcea43177dbba446574f00f817b2f89782fcf71709d911b2ad10872d0e",
            "+                                      oid = sha256:32b5fe23040b236dfe469456dd8f7ebbb4dcb3326305ba3e183714a32e4dd1ac",
            "-                                     size = 18317",
            "+                                     size = 2137",
        ]
        assert get_oid_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.copc.laz"
        ) == ("32b5fe23040b236dfe469456dd8f7ebbb4dcb3326305ba3e183714a32e4dd1ac", 2137)
