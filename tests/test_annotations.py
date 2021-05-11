import logging
import pytest


H = pytest.helpers.helpers()


def test_build_annotations(data_archive, cli_runner, caplog):
    with data_archive("points"):
        r = cli_runner.invoke(["build-annotations", "--all-reachable"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Enumerating reachable commits...",
            "Building feature change counts...",
            "(1/2): 0c64d82 Improve naming on Coromandel East coast",
            "(2/2): 7bc3b56 Import from nz-pa-points-topo-150k.gpkg",
            "done.",
        ]

        # Now ensure that the annotation actually gets used by diff command
        caplog.set_level(logging.DEBUG)
        r = cli_runner.invoke(
            ["-vv", "diff", "--only-feature-count=exact", "HEAD^..HEAD"]
        )
        assert r.exit_code == 0, r.stderr
        messages = [
            r.message
            for r in caplog.records
            if "feature-change-counts-exact" in r.message
        ]
        assert messages == [
            "retrieved: feature-change-counts-exact for 8feb827cf21831cc4766345894cd122947bba748...a8fa3347aed53547b194fc2101974b79b7fc337b: {'nz_pa_points_topo_150k': 5}"
        ]


def test_diff_feature_count_populates_annotations(data_archive, cli_runner, caplog):
    with data_archive("points"):
        # Now ensure that the annotation actually gets used by diff command
        caplog.set_level(logging.DEBUG)
        r = cli_runner.invoke(
            ["-vv", "diff", "--only-feature-count=fast", "HEAD^..HEAD"]
        )
        assert r.exit_code == 0, r.stderr
        messages = [
            r.message
            for r in caplog.records
            if "feature-change-counts-fast" in r.message
        ]
        assert messages == [
            "missing: feature-change-counts-fast for 8feb827cf21831cc4766345894cd122947bba748...a8fa3347aed53547b194fc2101974b79b7fc337b",
            'storing: feature-change-counts-fast for 8feb827cf21831cc4766345894cd122947bba748...a8fa3347aed53547b194fc2101974b79b7fc337b: {"nz_pa_points_topo_150k": 5}',
        ]

        # Now do it again and make sure it uses the annotation stored last time
        caplog.clear()
        r = cli_runner.invoke(
            ["-vv", "diff", "--only-feature-count=fast", "HEAD^..HEAD"]
        )
        assert r.exit_code == 0, r.stderr
        messages = [
            r.message
            for r in caplog.records
            if "feature-change-counts-fast" in r.message
        ]
        assert messages == [
            "retrieved: feature-change-counts-fast for "
            "8feb827cf21831cc4766345894cd122947bba748...a8fa3347aed53547b194fc2101974b79b7fc337b: "
            "{'nz_pa_points_topo_150k': 5}",
        ]
