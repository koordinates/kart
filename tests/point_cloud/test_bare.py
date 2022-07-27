from kart.exceptions import NO_WORKING_COPY


def test_show(cli_runner, data_archive):
    with data_archive("point-cloud/auckland-bare.git.tgz") as repo_path:
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == NO_WORKING_COPY

        r = cli_runner.invoke(["show", "HEAD", "auckland:tile:auckland_0_0"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[4:] == [
            "    Importing 16 LAZ tiles as auckland",
            "",
            "+++ auckland:tile:auckland_0_0",
            "+                                     name = auckland_0_0.copc.laz",
            "+                              crs84Extent = 174.7382443,174.7496594,-36.85123712,-36.84206322,-1.66,99.83",
            "+                                   format = laz-1.4/copc-1.0",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "+                               pointCount = 4231",
            "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "+                                      oid = sha256:1ad630a7b3acd8d678984831181688f82471a25ad6e93b2a2a5a253c9ffb1849",
            "+                                     size = 69437",
        ]
