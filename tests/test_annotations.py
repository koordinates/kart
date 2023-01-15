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
    Skips the test on windows
    """
    if platform.system() == "Windows":
        pytest.skip("does not run on windows")
    if os.geteuid() == 0:
        pytest.skip("doesn't work as root")

    mode = path.stat().st_mode
    # remove the 'w' bits
    path.chmod(mode ^ (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH))
    try:
        yield
    finally:
        # put it back the way it was
        path.chmod(mode)


def test_build_annotations(data_archive, cli_runner, caplog):
    with data_archive("points"):
        r = cli_runner.invoke(["build-annotations", "--all-reachable"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "Enumerating reachable commits...",
            "Building feature change counts...",
            "(1/2): 1582725 Improve naming on Coromandel East coast",
            "(2/2): 6e2984a Import from nz-pa-points-topo-150k.gpkg",
            "done.",
        ]

        # Now ensure that the annotation actually gets used by diff command
        caplog.clear()
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
            "retrieved: feature-change-counts-exact for 42b63a2a7c1b5dfe9c21ff9884b59f198e421821...622e7cc3b54cd54493eed6c4c5abe35d4bfa168e: {'nz_pa_points_topo_150k': 5}"
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
            "missing: feature-change-counts-fast for 42b63a2a7c1b5dfe9c21ff9884b59f198e421821...622e7cc3b54cd54493eed6c4c5abe35d4bfa168e",
            'storing: feature-change-counts-fast for 42b63a2a7c1b5dfe9c21ff9884b59f198e421821...622e7cc3b54cd54493eed6c4c5abe35d4bfa168e: {"nz_pa_points_topo_150k": 5}',
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
            "42b63a2a7c1b5dfe9c21ff9884b59f198e421821...622e7cc3b54cd54493eed6c4c5abe35d4bfa168e: "
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
