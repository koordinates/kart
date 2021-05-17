import logging
import os
import platform
import shutil
import stat
from contextlib import contextmanager
from pathlib import Path

import pytest


H = pytest.helpers.helpers()

_ANNOTATIONS_DBS_PATH = Path(__file__).parent / "data" / "annotations-dbs"


@contextmanager
def make_immutable(path: Path):
    """
    Contextmanager. Makes the given Path immutable.
    Skips the test on windows (os.chflags isn't available there)
    """
    if platform.system() == "Windows":
        raise pytest.skip("Can't run on Windows due to lack of os.chflags()")
    orig_flags = path.stat().st_flags
    os.chflags(path, orig_flags | stat.UF_IMMUTABLE)
    try:
        yield
    finally:
        os.chflags(path, orig_flags)


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


@pytest.mark.parametrize(
    "existing_db_path",
    [
        None,
        _ANNOTATIONS_DBS_PATH / "empty.db",
        _ANNOTATIONS_DBS_PATH / "empty-with-table.db",
    ],
)
def test_diff_feature_count_populates_annotations(
    data_archive, cli_runner, caplog, existing_db_path
):
    with data_archive("points"):
        if existing_db_path:
            shutil.copy(existing_db_path, ".kart/annotations.db")
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


def test_diff_feature_count_with_readonly_annotations(data_archive, cli_runner, caplog):
    with data_archive("points"):
        target_db = Path(".kart") / "annotations.db"
        shutil.copy(_ANNOTATIONS_DBS_PATH / "empty.db", target_db)

        with make_immutable(target_db):
            caplog.set_level(logging.DEBUG)
            r = cli_runner.invoke(
                ["-vv", "diff", "--only-feature-count=exact", "HEAD^..HEAD"]
            )
            # it works fine
            assert r.exit_code == 0, r.stderr
            assert r.stdout == "nz_pa_points_topo_150k:\n\t5 features changed\n"

            # but it didn't create the tables; the annotations code nooped
            messages = [
                r.message
                for r in caplog.records
                if "no such table: kart_annotations" in r.message
            ]
            assert len(messages) == 1


def test_diff_feature_count_with_readonly_repo_dir(data_archive, cli_runner, caplog):
    with data_archive("points"):
        kart_dir = Path(".kart")

        with make_immutable(kart_dir):
            caplog.set_level(logging.DEBUG)
            r = cli_runner.invoke(
                ["-vv", "diff", "--only-feature-count=exact", "HEAD^..HEAD"]
            )
            # it works fine
            assert r.exit_code == 0, r.stderr
            assert r.stdout == "nz_pa_points_topo_150k:\n\t5 features changed\n"

            # but it didn't create the tables; the annotations code nooped
            messages = [r.message for r in caplog.records]

            assert (
                "Failed to create database file; falling back to in-memory storage"
                in messages
            )
            assert "Can't store annotation; annotations.db is read-only" in messages
