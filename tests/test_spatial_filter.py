import json
import os
import subprocess
import tempfile

import pytest

from kart.cli_util import tool_environment
from kart.exceptions import (
    INVALID_ARGUMENT,
    NO_SPATIAL_FILTER,
    INVALID_OPERATION,
    SPATIAL_FILTER_PK_CONFLICT,
)
from kart.promisor_utils import FetchPromisedBlobsProcess, LibgitSubcode
from kart.repo import KartRepo

H = pytest.helpers.helpers()


# Feature detection for our custom git that has filter extension support
@pytest.fixture(scope="session")
def git_supports_filter_extensions():
    with tempfile.TemporaryDirectory() as td:
        subprocess.run(
            ["git", "-C", td, "init", "--quiet", "."],
            env=tool_environment(),
            check=True,
        )

        p = subprocess.run(
            ["git", "-C", td, "rev-list", "--filter=extension:z", "--objects"],
            env=tool_environment(),
            stderr=subprocess.PIPE,
            text=True,
        )
        if p.returncode == 0:
            raise ValueError(
                f"git_supports_filter_extensions: unexpected return code {p.returncode}"
            )

        err = p.stderr.strip()
        if err == "fatal: invalid filter-spec 'extension:z'":
            return False
        elif err == "fatal: No filter extension found with name z":
            return True
        else:
            raise ValueError("git_supports_filter_extensions: unexpected output: {err}")


# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def git_with_filter_extension_support(git_supports_filter_extensions):
    pytest.helpers.feature_assert_or_skip(
        "Git with filter extensions",
        "KART_EXPECT_GITFILTEREXTENSION",
        git_supports_filter_extensions,
    )


# Feature detection for our custom git that has a spatial filter extension
@pytest.fixture(scope="session")
def git_supports_spatial_filter(git_supports_filter_extensions):
    if not git_supports_filter_extensions:
        return False

    with tempfile.TemporaryDirectory() as td:
        subprocess.run(
            ["git", "-C", td, "init", "--quiet", "."],
            env=tool_environment(),
            check=True,
        )

        p = subprocess.run(
            [
                "git",
                "rev-list",
                "HEAD",
                "--objects",
                "--max-count=1",
                "--filter=extension:spatial=1,2,3,4",
            ],
            env=tool_environment(),
            stderr=subprocess.PIPE,
            text=True,
        )
        err = p.stderr.strip()
        if err == "fatal: No filter extension found with name spatial":
            return False
        elif p.returncode == 0:
            return True
        else:
            raise ValueError(
                "git_supports_spatial_filter: unexpected output {p.returncode}: {err}"
            )


# using a fixture instead of a skipif decorator means we get one aggregated skip
# message rather than one per test
@pytest.fixture(scope="session")
def git_with_spatial_filter_support(git_supports_spatial_filter):
    pytest.helpers.feature_assert_or_skip(
        "Git with spatial filters",
        "KART_EXPECT_GITSPATIALFILTER",
        git_supports_spatial_filter,
    )


def ring_as_wkt(*points):
    return "(" + ",".join(f"{x} {y}" for x, y in points) + ")"


def bbox_as_wkt_polygon(min_x, max_x, min_y, max_y):
    return (
        "POLYGON("
        + ring_as_wkt(
            (min_x, min_y),
            (max_x, min_y),
            (max_x, max_y),
            (min_x, max_y),
            (min_x, min_y),
        )
        + ")"
    )


SPATIAL_FILTER_GEOMETRY = {
    # A long skinny spatial filter on an angle - makes sure our filter envelopes and filter geometries are working.
    "points": (
        "MULTIPOLYGON(("
        + ring_as_wkt(
            (172.948, -35.1211),
            (173.4355, -35.7368),
            (173.8331, -36.1794),
            (174.1795, -36.545),
            (174.4488, -37.1094),
            (174.7695, -37.8277),
            (174.9876, -37.8983),
            (175.071, -37.7828),
            (174.4296, -36.654),
            (173.955, -35.9549),
            (173.5445, -35.5957),
            (173.3970, -35.2879),
            (172.9929, -35.0441),
            (172.948, -35.1211),
        )
        + "))"
    ),
    # Whereas this spatial filter is targeted on some but not all of the changes of the most recent points commit.
    "points-edit": bbox_as_wkt_polygon(175.8, 175.9, -36.9, -37.1),
    "polygons": (
        "POLYGON("
        + ring_as_wkt(
            (174.879, -37.8277),
            (175.0235, -37.9783),
            (175.2506, -37.9771),
            (175.3853, -37.8399),
            (175.3878, -37.642),
            (175.2396, -37.4999),
            (175.0235, -37.4987),
            (174.8839, -37.6359),
            (174.879, -37.8277),
        )
        + ")"
    ),
    "polygons-with-reprojection": (
        "POLYGON("
        + ring_as_wkt(
            (2675607, 6373321),
            (2687937, 6356327),
            (2707884, 6355974),
            (2720124, 6370883),
            (2720939, 6392831),
            (2708268, 6408939),
            (2689170, 6409537),
            (2676500, 6394592),
            (2675607, 6373321),
        )
        + ")"
    ),
}

SPATIAL_FILTER_CRS = {
    "points": "EPSG:4326",
    "polygons": "EPSG:4167",
    "polygons-with-reprojection": "EPSG:27200",
}


def local_features(dataset):
    # Returns the number of features that are available locally in this dataset.
    local_count = 0
    for blob in dataset.feature_blobs():
        try:
            blob.size
            local_count += 1
        except KeyError:
            continue
    return local_count


def is_local_feature(dataset, pk):
    # Returns True if the feature with the given PK is available locally.
    try:
        path = dataset.encode_1pk_to_path(pk, relative=True)
        blob = dataset.inner_tree / path
        return blob.size is not None
    except KeyError:
        return False


def test_git_filter_extension(git_with_filter_extension_support):
    # makes sure git_with_filter_extension_support gets exercised in CI
    # regardless of how other tests here are defined/changed.

    # will skip/fail in the decorator
    pass


def test_git_spatial_filter_extension(git_with_spatial_filter_support):
    # makes sure git_with_spatial_filter_support gets exercised in CI regardless
    # of how other tests here are defined/changed.

    # will skip/fail in the decorator
    pass


def test_init_with_spatial_filter(cli_runner, tmp_path):
    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    repo_path = tmp_path / "inline_test"
    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter={crs};{geom}"])
    assert r.exit_code == 0, r.stderr

    repo = KartRepo(repo_path)
    assert repo.config["kart.spatialfilter.geometry"].startswith(
        "POLYGON ((174.879 -37.8277,"
    )
    assert repo.config["kart.spatialfilter.crs"] == crs

    repo_path = tmp_path / "file_test"
    file_path = tmp_path / "spatialfilter.txt"
    file_path.write_text(f"{crs}\n\n{geom}\n", encoding="utf-8")
    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter=@{file_path}"])
    assert r.exit_code == 0, r.stderr

    repo = KartRepo(repo_path)
    assert repo.config["kart.spatialfilter.geometry"].startswith(
        "POLYGON ((174.879 -37.8277,"
    )
    assert repo.config["kart.spatialfilter.crs"] == crs

    r = cli_runner.invoke(["-C", repo_path, "status"])
    assert r.exit_code == 0, r.stderr
    assert (
        "A spatial filter is active, limiting repo to a specific region inside [174.879, -37.978, 175.388, -37.499]"
        in r.stdout
    )

    r = cli_runner.invoke(["-C", repo_path, "status", "-o", "json"])
    assert r.exit_code == 0, r.stderr
    spatial_filter = json.loads(r.stdout)["kart.status/v1"]["spatialFilter"]
    assert spatial_filter["geometry"].startswith(
        "01030000000100000009000000E3A59BC420DC65401973D7"
    )
    assert spatial_filter["crs"] == "EPSG:4167"


def test_init_with_invalid_spatial_filter(cli_runner, tmp_path):
    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    # The validity of the geometry and CRS should be checked immediately, before the repo is created:
    repo_path = tmp_path / "invalid_test"
    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter={crs};foobar"])
    assert r.exit_code == INVALID_ARGUMENT
    assert "Invalid geometry" in r.stderr
    assert not repo_path.exists()

    r = cli_runner.invoke(["init", repo_path, f"--spatial-filter=ABCD:1234;{geom}"])
    assert r.exit_code == INVALID_ARGUMENT
    assert "Invalid or unknown coordinate reference system" in r.stderr
    assert not repo_path.exists()

    r = cli_runner.invoke(
        ["init", repo_path, f"--spatial-filter={crs};POINT(174.879 -37.8277)"]
    )
    assert r.exit_code == INVALID_ARGUMENT
    assert "Expected geometry for spatial filter of type POLYGON|MULTIPOLYGON but found: POINT"
    assert not repo_path.exists()


def test_clone_with_spatial_filter(
    git_with_spatial_filter_support, data_archive, cli_runner, tmp_path
):
    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    file_path = (tmp_path / "spatialfilter.txt").resolve()
    file_path.write_text(f"{crs}\n\n{geom}\n", encoding="utf-8")

    with data_archive("polygons-with-feature-envelopes") as repo1_path:
        repo1_url = f"file://{repo1_path.resolve()}"
        # Clone repo using spatial filter
        repo2_path = tmp_path / "repo2"
        r = cli_runner.invoke(
            ["clone", repo1_url, repo2_path, f"--spatial-filter=@{file_path}"]
        )
        assert r.exit_code == 0, r.stderr

        # The resulting repo has the spatial filter configured locally.
        repo2 = KartRepo(repo2_path)
        assert repo2.config["kart.spatialfilter.geometry"].startswith(
            "POLYGON ((174.879 -37.8277,"
        )
        assert repo2.config["kart.spatialfilter.crs"] == crs

        with repo2.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == 44

        # However, the entire polygons layer was cloned.
        # Spatial filters are currently only applied locally... all features are still present.
        assert local_features(repo2.datasets()[H.POLYGONS.LAYER]) == H.POLYGONS.ROWCOUNT

        # Unless you use an experimental environment variable:
        # TODO: Always apply spatial filters during the clone, then clean up this test.
        os.environ["X_KART_SPATIAL_FILTERED_CLONE"] = "1"
        try:
            repo3_path = tmp_path / "repo3"
            r = cli_runner.invoke(
                ["clone", repo1_url, repo3_path, f"--spatial-filter=@{file_path}"]
            )
            assert r.exit_code == 0, r.stderr

            repo3 = KartRepo(repo3_path)
            assert repo3.config["kart.spatialfilter.geometry"].startswith(
                "POLYGON ((174.879 -37.8277,"
            )
            assert repo3.config["kart.spatialfilter.crs"] == crs
            ds = repo3.datasets()[H.POLYGONS.LAYER]

            local_feature_count = local_features(ds)
            assert local_feature_count != H.POLYGONS.ROWCOUNT
            assert local_feature_count == 46

            with repo3.working_copy.session() as sess:
                assert H.row_count(sess, H.POLYGONS.LAYER) == 44

        finally:
            del os.environ["X_KART_SPATIAL_FILTERED_CLONE"]

        # The next test delves further into testing how spatial-filtered clones behave, but
        # it loads the same spatially filtered repo from the test data folder so that we can
        # test spatially filtered cloning separately from spatially filtered clone behaviour.


def test_spatially_filtered_partial_clone(data_archive, cli_runner):
    crs = SPATIAL_FILTER_CRS["polygons"]

    with data_archive("polygons-with-feature-envelopes") as repo1_path:
        repo1_url = f"file://{repo1_path.resolve()}"

        with data_archive("polygons-spatial-filtered") as repo2_path:
            repo2 = KartRepo(repo2_path)
            repo2.config["remote.origin.url"] = repo1_url

            assert repo2.config["kart.spatialfilter.geometry"].startswith(
                "POLYGON ((174.879 -37.8277,"
            )
            assert repo2.config["kart.spatialfilter.crs"] == crs
            ds = repo2.datasets()[H.POLYGONS.LAYER]

            local_feature_count = local_features(ds)
            assert local_feature_count != H.POLYGONS.ROWCOUNT
            assert local_feature_count == 52

            r = cli_runner.invoke(["-C", repo2_path, "create-workingcopy"])
            assert r.exit_code == 0, r.stderr

            with repo2.working_copy.session() as sess:
                assert H.row_count(sess, H.POLYGONS.LAYER) == 44

            def _get_key_error(ds, pk):
                try:
                    ds.get_feature(pk)
                    return None
                except KeyError as e:
                    return e

            assert _get_key_error(ds, 1424927) is None
            assert _get_key_error(ds, 9999999).subcode == LibgitSubcode.ENOSUCHPATH
            assert _get_key_error(ds, 1443053).subcode == LibgitSubcode.EOBJECTPROMISED


def test_spatially_filtered_fetch_promised(
    data_archive, cli_runner, insert, monkeypatch, git_supports_spatial_filter
):

    # Keep track of how many features we fetch lazily after the partial clone.
    orig_fetch_func = FetchPromisedBlobsProcess.fetch
    fetch_count = 0

    def _fetch(*args, **kwargs):
        nonlocal fetch_count
        fetch_count += 1
        return orig_fetch_func(*args, **kwargs)

    monkeypatch.setattr(FetchPromisedBlobsProcess, "fetch", _fetch)

    with data_archive("polygons-with-feature-envelopes") as repo1_path:
        repo1_url = f"file://{repo1_path.resolve()}"

        with data_archive("polygons-spatial-filtered") as repo2_path:
            repo2 = KartRepo(repo2_path)
            repo2.config["remote.origin.url"] = repo1_url

            if not git_supports_spatial_filter:
                # Git doesn't understand the "spatial" filter.
                # But we can do this test without it:
                print("Git doesn't support spatial filters, using blob:none instead")
                repo2.config["remote.origin.partialclonefilter"] = "blob:none"

            orig_config_dict = {c.name: c.value for c in repo2.config}

            ds = repo2.datasets()[H.POLYGONS.LAYER]

            local_feature_count = local_features(ds)
            assert local_feature_count != H.POLYGONS.ROWCOUNT
            assert local_feature_count == 52

            r = cli_runner.invoke(["-C", repo2_path, "create-workingcopy"])
            assert r.exit_code == 0, r.stderr

            with repo2.working_copy.session() as sess:
                assert H.row_count(sess, H.POLYGONS.LAYER) == 44
                # Inserting features that are in the dataset, but don't match the spatial filter,
                # so they are not loaded locally nor written to the working copy.
                for pk in H.POLYGONS.SAMPLE_PKS:
                    if not is_local_feature(ds, pk):
                        insert(sess, with_pk=pk, commit=False)

            r = cli_runner.invoke(["-C", repo2_path, "status"])
            assert r.exit_code == 0, r.stderr
            assert "6 primary key conflicts" in r.stdout
            # All of the 6 featues that are conflicts / were "updated" in the WC have been loaded:
            assert fetch_count == 6
            assert local_features(ds) == 58

            with repo2.working_copy.session() as sess:
                sess.execute(f"DROP TABLE {H.POLYGONS.LAYER};")

            r = cli_runner.invoke(["-C", repo2_path, "status"])
            assert r.exit_code == 0, r.stderr
            assert f"{H.POLYGONS.ROWCOUNT} deletes" in r.stdout
            assert local_features(ds) == 58

            r = cli_runner.invoke(["-C", repo2_path, "diff"])
            assert r.exit_code == 0, r.stderr
            # All of the deleted features have now been loaded to show in the diff output:
            assert local_features(ds) == H.POLYGONS.ROWCOUNT
            assert fetch_count == H.POLYGONS.ROWCOUNT - 52

            final_config_dict = {c.name: c.value for c in repo2.config}
            # Making these fetches shouldn't change any repo config:
            assert final_config_dict == orig_config_dict


def test_clone_with_reference_spatial_filter(data_archive, cli_runner, tmp_path):
    # TODO - this currently tests that the spatial filter is correctly applied locally after
    # the entire repo is cloned. Applying a reference spatial filter remotely to do a
    # partial clone is not yet supported.

    geom = SPATIAL_FILTER_GEOMETRY["polygons"]
    crs = SPATIAL_FILTER_CRS["polygons"]

    file_path = tmp_path / "spatialfilter.txt"
    file_path.write_text(f"{crs}\n\n{geom}\n", encoding="utf-8")

    with data_archive("polygons") as repo1_path:
        r = cli_runner.invoke(
            [
                "commit-files",
                "-m",
                "Add spatial filter",
                f"spatialfilter.txt=@{file_path}",
            ]
        )
        assert r.exit_code == 0, r.stderr
        r = cli_runner.invoke(["git", "hash-object", file_path])
        assert r.exit_code == 0, r.stderr
        blob_sha = r.stdout.strip()
        r = cli_runner.invoke(["git", "update-ref", "refs/filters/octagon", blob_sha])
        assert r.exit_code == 0, r.stderr

        # Spatial filter is now stored with ref "octagon".
        # Test spatial-filter resolve:
        r = cli_runner.invoke(["spatial-filter", "resolve", "octagon"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.startswith(f"{crs}\n\nPOLYGON((174.879 -37.8277,")

        r = cli_runner.invoke(["spatial-filter", "resolve", "octagon", "--envelope"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout == "174.879,-37.9783,175.3878,-37.4987\n"

        r = cli_runner.invoke(["spatial-filter", "resolve", "octagon", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        jdict = json.loads(r.stdout)
        assert jdict["reference"] == "refs/filters/octagon"
        assert jdict["objectId"] == blob_sha
        assert jdict["geometry"].startswith(
            "01030000000100000009000000E3A59BC420DC65401973D7"
        )
        assert jdict["crs"] == crs

        r = cli_runner.invoke(
            ["spatial-filter", "resolve", "octagon", "-o", "json", "--envelope"]
        )
        assert r.exit_code == 0, r.stderr
        envelope = json.loads(r.stdout)
        assert envelope == [174.879, -37.9783, 175.3878, -37.4987]

        # This is disabled by default as it is still not fully supported.
        os.environ["X_KART_SPATIAL_FILTER_REFERENCE"] = "1"
        try:
            # Clone repo using spatial filter reference
            repo2_path = tmp_path / "repo2"
            r = cli_runner.invoke(
                ["clone", repo1_path, repo2_path, "--spatial-filter=octagon"]
            )
            assert r.exit_code == 0, r.stderr

            # The resulting repo has the spatial filter configured locally.
            repo2 = KartRepo(repo2_path)
            assert (
                repo2.config["kart.spatialfilter.reference"] == "refs/filters/octagon"
            )
            assert repo2.config["kart.spatialfilter.objectid"] == blob_sha

            # However, the entire polygons layer was cloned.
            # TODO: Only clone the features that match the spatial filter.
            assert (
                local_features(repo2.datasets()[H.POLYGONS.LAYER])
                == H.POLYGONS.ROWCOUNT
            )

            with repo2.working_copy.session() as sess:
                assert H.row_count(sess, H.POLYGONS.LAYER) == 44

            # Clone repo using spatial filter object ID
            repo3_path = tmp_path / "repo3"
            r = cli_runner.invoke(
                ["clone", repo1_path, repo3_path, f"--spatial-filter={blob_sha}"]
            )
            assert r.exit_code == 0, r.stderr
            repo3 = KartRepo(repo3_path)
            assert repo3.config["kart.spatialfilter.geometry"].startswith(
                "POLYGON ((174.879 -37.8277,"
            )
            assert repo3.config["kart.spatialfilter.crs"] == crs

            with repo3.working_copy.session() as sess:
                assert H.row_count(sess, H.POLYGONS.LAYER) == 44

            # Missing spatial filter:
            repo4_path = tmp_path / "repo4"
            r = cli_runner.invoke(
                ["clone", repo1_path, repo4_path, "--spatial-filter=dodecahedron"]
            )
            assert r.exit_code == NO_SPATIAL_FILTER, r.stderr

        finally:
            del os.environ["X_KART_SPATIAL_FILTER_REFERENCE"]


@pytest.mark.parametrize(
    "archive,table,filter_key",
    [
        pytest.param("points", H.POINTS.LAYER, "points", id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, "polygons", id="polygons"),
        pytest.param(
            "polygons",
            H.POLYGONS.LAYER,
            "polygons-with-reprojection",
            id="polygons-with-reprojection",
        ),
        # Use polygons spatial filter config for table archive too - doesn't matter exactly what it is.
        pytest.param("table", H.TABLE.LAYER, "polygons", id="table"),
    ],
)
def test_spatial_filtered_workingcopy(
    archive, table, filter_key, data_archive, cli_runner
):
    """ Checkout a working copy to edit """
    with data_archive(archive) as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        matching_features = {
            "points": 302,
            "polygons": 44,
            "table": H.TABLE.ROWCOUNT,  # All rows from table.tgz should be present, unaffected by spatial filtering.
        }

        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY[filter_key]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS[filter_key]

        r = cli_runner.invoke(["checkout"])
        assert r.exit_code == 0, r

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, table) == matching_features[archive]


def test_reset_wc_with_spatial_filter(data_archive, cli_runner):
    # This spatial filter matches 2 of the 5 possible changes between main^ and main.

    with data_archive("points.tgz") as repo_path:
        # Without a spatial filter - checking out main^ then restoring main results in 5 uncommitted changes,
        # the difference between main^ and main.
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        r = cli_runner.invoke(["checkout", "main^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["restore", "-s", "main"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        status = json.loads(r.stdout)["kart.status/v1"]
        assert (
            status["workingCopy"]["changes"][H.POINTS.LAYER]["feature"]["updates"] == 5
        )

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == H.POINTS.ROWCOUNT

        # With the spatial filter - checking out main^ then restoring main results in 2 uncommitted changes,
        # the difference between main^ and main that matches the spatial filter.
        H.clear_working_copy()
        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY[
            "points-edit"
        ]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS["points"]

        r = cli_runner.invoke(["checkout", "main^"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["restore", "-s", "main"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        status = json.loads(r.stdout)["kart.status/v1"]
        assert (
            status["workingCopy"]["changes"][H.POINTS.LAYER]["feature"]["updates"] == 2
        )

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == 13


def test_diff_commits_with_spatial_filter(data_archive, cli_runner, insert):
    with data_archive("points.tgz") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()
        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY[
            "points-edit"
        ]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS["points"]

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr

        # 13 of the features in the initial commit match the spatial filter.
        r = cli_runner.invoke(["show", "HEAD^", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 13

        # Of those, 2 have edits in the subsequent commit.
        r = cli_runner.invoke(["show", "HEAD", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 2

        with repo.working_copy.session() as sess:
            for i in range(5):
                insert(sess, commit=False)

        # All 5 WC edits are shown, regardless of whether they match the spatial filter.
        r = cli_runner.invoke(["diff", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 5

        # The 2 commit-commit edits that match the filter plus the 5 WC edits are shown.
        r = cli_runner.invoke(["diff", "HEAD^", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        diff = json.loads(r.stdout)["kart.diff/v1+hexwkb"]
        assert len(diff[H.POINTS.LAYER]["feature"]) == 7


def test_change_spatial_filter(data_archive, cli_runner, insert):
    with data_archive("polygons.tgz") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == H.POLYGONS.ROWCOUNT

        geom = SPATIAL_FILTER_GEOMETRY["polygons"]
        crs = SPATIAL_FILTER_CRS["polygons"]
        r = cli_runner.invoke(["checkout", "main", f"--spatial-filter={crs};{geom}"])
        assert r.exit_code == 0, r.stderr

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == 44

        geom = SPATIAL_FILTER_GEOMETRY["polygons"]
        crs = SPATIAL_FILTER_CRS["polygons"]
        r = cli_runner.invoke(["checkout", "main", "--spatial-filter="])
        assert r.exit_code == 0, r.stderr

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POLYGONS.LAYER) == H.POLYGONS.ROWCOUNT
            insert(sess, commit=False)

        r = cli_runner.invoke(["checkout", "main", f"--spatial-filter={crs};{geom}"])
        assert r.exit_code == INVALID_OPERATION
        assert "You have uncommitted changes in your working copy" in r.stderr


def test_pk_conflict_due_to_spatial_filter(
    data_archive, cli_runner, insert, edit_points
):
    with data_archive("points.tgz") as repo_path:
        repo = KartRepo(repo_path)
        H.clear_working_copy()
        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTER_GEOMETRY["points"]
        repo.config["kart.spatialfilter.crs"] = SPATIAL_FILTER_CRS["points"]

        r = cli_runner.invoke(["checkout", "main"])
        assert r.exit_code == 0, r.stderr
        head_tree_id = repo.head_tree.id

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == 302
            # Both of these new features are outside the spatial filter.
            # One of them - PK=1 - is a conflict with an existing feature (that is outside the spatial filter).
            insert(sess, commit=False, with_pk=1)
            insert(sess, commit=False, with_pk=98001)
            assert H.row_count(sess, H.POINTS.LAYER) == 304

        r = cli_runner.invoke(["status", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        change_status = json.loads(r.stdout)["kart.status/v1"]["workingCopy"]["changes"]
        feature_changes = change_status["nz_pa_points_topo_150k"]["feature"]
        assert feature_changes == {"inserts": 1, "primaryKeyConflicts": 1}

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert "1 inserts" in r.stdout
        assert "1 primary key conflicts" in r.stdout

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        assert "Warning: " in r.stderr
        assert (
            "In dataset nz_pa_points_topo_150k the conflicting primary key values are: 1"
            in r.stderr
        )

        r = cli_runner.invoke(["commit", "-m", "test"])
        assert r.exit_code == SPATIAL_FILTER_PK_CONFLICT
        assert (
            "In dataset nz_pa_points_topo_150k the conflicting primary key values are: 1"
            in r.stderr
        )
        assert "Aborting commit due to conflicting primary key values" in r.stderr
        assert repo.head_tree.id == head_tree_id

        r = cli_runner.invoke(["commit", "-m", "test", "--allow-pk-conflicts"])
        assert r.exit_code == 0
        assert repo.head_tree.id != head_tree_id

        assert (
            "Removing 2 features from the working copy that no longer match the spatial filter..."
            in r.stdout
        )

        with repo.working_copy.session() as sess:
            assert H.row_count(sess, H.POINTS.LAYER) == 302
