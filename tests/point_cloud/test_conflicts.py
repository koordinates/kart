import re

from .fixtures import requires_pdal  # noqa
from kart.lfs_util import get_hash_and_size_of_file


def test_merge_and_resolve_conflicts(
    cli_runner, data_archive, monkeypatch, requires_pdal, capfd
):
    monkeypatch.setenv("X_KART_POINT_CLOUDS", "1")

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
            "                             crs84Extent = 174.7382443,174.7496594,-36.85123712,-36.84206322,-1.66,99.83",
            "                                  format = laz-1.4/copc-1.0",
            "                            nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "                              pointCount = 4231",
            "                               sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "                                     oid = sha256:1ad630a7b3acd8d678984831181688f82471a25ad6e93b2a2a5a253c9ffb1849",
            "                                    size = 69437",
            "            auckland:tile:auckland_0_0:ours:",
            "                                    name = auckland_0_0.copc.laz",
            "                             crs84Extent = 174.7602522,174.7716555,-36.83288446,-36.82371241,-1.54,10.59",
            "                                  format = laz-1.4/copc-1.0",
            "                            nativeExtent = 1756987.83,1757986.74,5922219.95,5923219.43,-1.54,10.59",
            "                              pointCount = 1599",
            "                                     oid = sha256:858757799b09743b4b58627d2cfabd7d2c0359d658c060b195c8ac932c279ef3",
            "                                    size = 22671",
            "            auckland:tile:auckland_0_0:theirs:",
            "                                    name = auckland_0_0.copc.laz",
            "                             crs84Extent = 174.7492629,174.7606572,-36.84205419,-36.83288872,-1.48,35.15",
            "                                  format = laz-1.4/copc-1.0",
            "                            nativeExtent = 1755989.03,1756987.13,5921220.62,5922219.49,-1.48,35.15",
            "                              pointCount = 1558",
            "                                     oid = sha256:9aa44b101a0e3461a25b94d747057b0dd20e737ac2a344f788085f062ac7c312",
            "                                    size = 24480",
            "",
        ]

        r = cli_runner.invoke(
            ["resolve", "auckland:tile:auckland_0_0", "--with=theirs"]
        )
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ["Resolved 1 conflict. 0 conflicts to go."]

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

        assert get_hash_and_size_of_file(
            repo_path / "auckland" / "auckland_0_0.copc.laz"
        ) == ("9aa44b101a0e3461a25b94d747057b0dd20e737ac2a344f788085f062ac7c312", 24480)
