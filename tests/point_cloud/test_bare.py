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
            "+                              crs84Extent = 174.73844833207193,174.74945404214898,-36.85123712200056,-36.84206322341377,-1.66,99.83",
            "+                                   format = laz-1.4/copc-1.0",
            "+                             nativeExtent = 1754987.85,1755987.77,5920219.76,5921219.64,-1.66,99.83",
            "+                               pointCount = 4231",
            "+                                sourceOid = sha256:6b980ce4d7f4978afd3b01e39670e2071a792fba441aca45be69be81cb48b08c",
            "+                                      oid = sha256:a1862450841dede2759af665825403e458dfa551c095d9a65ea6e6765aeae0f7",
            "+                                     size = 69590",
        ]
