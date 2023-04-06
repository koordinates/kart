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
            "+                              crs84Extent = POLYGON((174.7384483 -36.8512371,174.7382443 -36.8422277,174.7494540 -36.8420632,174.7496594 -36.8510726,174.7384483 -36.8512371))",
            "+                                   format = laz-1.4/copc-1.0",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "+                               pointCount = 4231",
            "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "+                                      oid = sha256:adbc1dc7fc99c88fcb627b9c40cdb56c211b791fe9cf83fe066b1a9932c12569",
            "+                                     size = 54396",
        ]
