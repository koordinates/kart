import json
from contextlib import contextmanager
from pathlib import Path

import pygit2
import pytest
from kart.exceptions import NO_TABLE, PATCH_DOES_NOT_APPLY, NOT_YET_IMPLEMENTED
from kart.repo import KartRepo


H = pytest.helpers.helpers()
patches = Path(__file__).parent / "data" / "patches"


@pytest.mark.parametrize("input", ["{}", "this isnt json"])
def test_apply_invalid_patch(input, data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["apply", "-"], input=input)
        assert r.exit_code == 1, r
        assert "Failed to parse JSON patch file" in r.stderr


def test_apply_empty_patch(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(["apply", patches / "points-empty.kartpatch"])
        assert r.exit_code == 44, r
        assert "No changes to commit" in r.stderr


def test_apply_nonempty_patch_which_makes_no_changes(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        # Despite appearances, this patch is empty because it makes a change
        # which has no actual effect.
        r = cli_runner.invoke(["apply", patches / "points-empty-2.kartpatch"])
        assert r.exit_code == 44, r
        assert "No changes to commit" in r.stderr


def test_apply_with_wrong_dataset_name(data_archive, cli_runner):
    patch_data = json.dumps(
        {
            "kart.diff/v1+hexwkb": {
                "wrong-name": {
                    "featureChanges": [],
                    "metaChanges": [],
                }
            },
            "kart.patch/v1": {"message": "hey"},
        }
    )
    with data_archive("points"):
        r = cli_runner.invoke(["apply", "-"], input=patch_data)
        assert r.exit_code == NO_TABLE, r
        assert (
            "Patch contains changes for dataset 'wrong-name' which is not in this repository"
            in r.stderr
        )


def test_apply_twice(data_archive, cli_runner):
    patch_path = patches / "points-1U-1D-1I.kartpatch"
    with data_archive("points"):
        r = cli_runner.invoke(["apply", str(patch_path)])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["apply", str(patch_path)])
        assert r.exit_code == PATCH_DOES_NOT_APPLY

        assert (
            "nz_pa_points_topo_150k: Trying to delete nonexistent feature: 1241"
            in r.stderr
        )
        assert (
            "nz_pa_points_topo_150k: Trying to create feature that already exists: 9999"
            in r.stderr
        )
        assert (
            "nz_pa_points_topo_150k: Trying to update already-changed feature: 1795"
            in r.stderr
        )
        assert "Patch does not apply" in r.stderr


def test_apply_with_no_working_copy(data_archive, cli_runner):
    patch_filename = "updates-only.kartpatch"
    message = "Change the Coromandel"
    author = {"name": "Someone", "time": 1561040913, "offset": 60}
    with data_archive("points") as repo_dir:
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_dir)
        commit = repo.head_commit

        # the author details all come from the patch, including timestamp
        assert commit.message == message
        assert commit.author.name == author["name"]
        assert commit.author.time == author["time"]
        assert commit.author.offset == author["offset"]

        # the committer timestamp doesn't come from the patch
        assert commit.committer.time > commit.author.time
        bits = r.stdout.split()
        assert bits[0] == "Commit"

        # Check that the `kart create-patch` output is the same as our original patch file had.
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0, r.stderr
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open("r", encoding="utf-8"))

        assert patch["kart.patch/v1"] == original_patch["kart.patch/v1"]
        assert patch["kart.diff/v1+hexwkb"] == original_patch["kart.diff/v1+hexwkb"]


def test_apply_meta_changes(data_archive, cli_runner):
    patch_file = json.dumps(
        {
            "kart.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "meta": {
                        "title": {
                            "-": "NZ Pa Points (Topo, 1:50k)",
                            "+": "new title:",
                        }
                    }
                },
            },
            "kart.patch/v1": {
                "authorEmail": "robert@example.com",
                "authorName": "Robert Coup",
                "authorTime": "2019-06-20T14:28:33Z",
                "authorTimeOffset": "+01:00",
                "message": "Change the title",
            },
        }
    )

    with data_archive("points"):
        r = cli_runner.invoke(
            ["apply", "-"],
            input=patch_file,
        )
        assert r.exit_code == 0, r.stderr

        # Check that the `kart create-patch` output is the same as our original patch file had.
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        meta = patch["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"]
        assert meta == {"title": {"+": "new title:", "-": "NZ Pa Points (Topo, 1:50k)"}}


def test_apply_user_info(data_archive, cli_runner):
    patch_file = json.dumps(
        {
            "kart.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "meta": {
                        "title": {
                            "-": "NZ Pa Points (Topo, 1:50k)",
                            "+": "new title:",
                        }
                    }
                },
            },
            "kart.patch/v1": {
                "authorEmail": "craig@example.com",
                "authorName": "Craig de Stigter",
                "authorTime": "2019-06-20T14:28:33Z",
                "authorTimeOffset": "+12:00",
                "message": "Change the title",
            },
        }
    )
    with data_archive("points"):
        r = cli_runner.invoke(
            ["apply", "-"],
            input=patch_file,
        )
        assert r.exit_code == 0, r.stderr

        # Check that the change was actually applied
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        header = patch["kart.patch/v1"]
        assert header["authorEmail"] == "craig@example.com"
        assert header["authorName"] == "Craig de Stigter"


def test_apply_onto_other_ref(data_working_copy, cli_runner):
    patch_file = json.dumps(
        {
            "kart.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "meta": {
                        "title": {
                            "-": "NZ Pa Points (Topo, 1:50k)",
                            "+": "new title:",
                        }
                    }
                },
            },
            "kart.patch/v1": {
                "authorEmail": "craig@example.com",
                "authorName": "Craig de Stigter",
                "authorTime": "2019-06-20T14:28:33Z",
                "authorTimeOffset": "+12:00",
                "message": "Change the title",
            },
        }
    )
    with data_working_copy("points"):
        # First create another branch.
        r = cli_runner.invoke(["branch", "otherbranch"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(
            ["apply", "--ref=otherbranch", "-"],
            input=patch_file,
        )
        assert r.exit_code == 0, r.stderr
        # "Commit <hash>". Doesn't contain a workingcopy update!
        assert len(r.stdout.strip().splitlines()) == 1

        # Check that the change was applied to otherbranch
        r = cli_runner.invoke(["create-patch", "otherbranch"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        assert patch["kart.patch/v1"]["message"] == "Change the title"

        # But not to HEAD
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        assert (
            patch["kart.patch/v1"]["message"]
            == "Improve naming on Coromandel East coast"
        )


@contextmanager
def write_patch(patch_dict, tmp_path):
    patch_path = tmp_path / "patch"
    with patch_path.open(mode="w") as f:
        f.write(json.dumps(patch_dict, indent=2, sort_keys=True))
    # this is cleaned up by the tmp_path fixture at the end of the test
    yield patch_path


def points_patch(ds_edits):
    return {
        "kart.diff/v1+hexwkb": {
            "nz_pa_points_topo_150k": ds_edits,
        },
        "kart.patch/v1": {
            "authorEmail": "me@example.com",
            "authorName": "Me",
            "authorTime": "2100-01-01T01:01:01Z",
            "authorTimeOffset": "+13:00",
            "message": "a points patch",
            "base": "1582725544d9122251acd4b3fc75b5c88ac3fd17",
        },
    }


def test_apply_minimal_style_patch_without_base(data_archive, cli_runner, tmp_path):
    patch = points_patch(
        {
            "meta": {
                "title": {
                    "+": "new title:",
                }
            }
        }
    )
    patch["kart.patch/v1"].pop("base")
    with data_archive("points"):
        # We can't apply this patch,
        # because the "-" object for the title is missing.
        with write_patch(patch, tmp_path) as patch_path:
            r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == PATCH_DOES_NOT_APPLY, r.stderr


def test_apply_minimal_style_patch(data_archive, cli_runner, tmp_path):
    patch = points_patch(
        {
            "feature": [
                {
                    # one edit (FID 1182 already exists)
                    "*": {
                        "fid": 1182,
                        "geom": "01010000009933726825F76540140C370F236742C0",
                        "name_ascii": "Ko Te Ra Matiti (Wharekaho)",
                        "macronated": "Y",
                        "name": "Ko Te R\u0101 Matiti (Wharekaho)",
                        # (this is the edit)
                        "t50_fid": 9999999,
                    }
                },
            ]
        }
    )

    with data_archive("points"):
        with write_patch(patch, tmp_path) as patch_path:
            r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == NOT_YET_IMPLEMENTED, r.stderr


def test_apply_minimal_style_feature_patch_with_insert(
    data_archive, cli_runner, tmp_path
):
    patch = points_patch(
        {
            "feature": [
                {
                    "+": {
                        "fid": 123456,
                        "geom": None,
                        "name_ascii": "abc",
                        "macronated": "N",
                        "name": "abc",
                        "t50_fid": 123456,
                    }
                },
            ]
        }
    )

    with data_archive("points"):
        with write_patch(patch, tmp_path) as patch_path:
            r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r.stderr

        # Check that the change was actually applied
        r = cli_runner.invoke(["show", "-o", "json", "HEAD"])
        assert r.exit_code == 0
        show = json.loads(r.stdout)
        features = show["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["feature"]
        assert len(features) == 1
        assert features[0] == {
            "+": {
                "fid": 123456,
                "geom": None,
                "name_ascii": "abc",
                "macronated": "N",
                "name": "abc",
                "t50_fid": 123456,
            }
        }


def test_apply_create_dataset(data_archive, cli_runner):
    patch_path = patches / "polygons.kartpatch"
    with data_archive("points"):
        r = cli_runner.invoke(["data", "ls", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout)["kart.data.ls/v1"] == ["nz_pa_points_topo_150k"]

        r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["data", "ls", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        assert json.loads(r.stdout)["kart.data.ls/v1"] == [
            "nz_pa_points_topo_150k",
            "nz_waca_adjustments",
        ]

        # Check that the `kart create-patch` output is the same as our original patch file had.
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)

        original_patch = json.load(patch_path.open("r", encoding="utf-8"))
        a, b = "kart.diff/v1+hexwkb", "nz_waca_adjustments"
        assert patch[a][b] == original_patch[a][b]


def test_add_and_remove_xml_metadata_as_json(data_archive, cli_runner):
    archive_path = Path("upgrade") / "v2.kart" / "points.tgz"
    with data_archive(archive_path):
        r = cli_runner.invoke(["meta", "get", "-o", "json"])
        assert r.exit_code == 0, r.stderr
        o = json.loads(r.stdout)

        assert "metadata/dataset.json" in o["nz_pa_points_topo_150k"]
        assert "metadata.xml" not in o["nz_pa_points_topo_150k"]

        metadata_json = o["nz_pa_points_topo_150k"]["metadata/dataset.json"]
        xml_content = metadata_json["http://www.isotc211.org/2005/gmd"]["text/xml"]
        orig_patch = {
            "kart.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "meta": {
                        "metadata/dataset.json": {
                            "-": {
                                "http://www.isotc211.org/2005/gmd": {
                                    "text/xml": xml_content
                                }
                            }
                        }
                    }
                }
            },
            "kart.patch/v1": {
                "authorEmail": "robert@example.com",
                "authorName": "Robert Coup",
                "authorTime": "2019-06-20T14:28:33Z",
                "authorTimeOffset": "+01:00",
                "message": "Remove XML metadata",
            },
        }
        patch_file = json.dumps(orig_patch)

        r = cli_runner.invoke(
            ["apply", "-"],
            input=patch_file,
        )
        assert r.exit_code == 0, r.stderr

        # Check that the `kart create-patch` output is the same as our original patch file had.
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        assert (
            patch["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"]
            == orig_patch["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"]
        )

        # check we can add it again too
        m = orig_patch["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["meta"][
            "metadata/dataset.json"
        ]
        m["+"] = m.pop("-")
        patch_file = json.dumps(orig_patch)
        r = cli_runner.invoke(
            ["apply", "-"],
            input=patch_file,
        )
        assert r.exit_code == 0, r.stderr


def test_apply_with_working_copy(
    data_working_copy,
    cli_runner,
):
    patch_filename = "updates-only.kartpatch"
    message = "Change the Coromandel"
    author = {"name": "Someone", "time": 1561040913, "offset": 60}
    with data_working_copy("points") as (repo_dir, wc_path):
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_dir)
        commit = repo.head_commit

        # the author details all come from the patch, including timestamp
        assert commit.message == message
        assert commit.author.name == author["name"]
        assert commit.author.time == author["time"]
        assert commit.author.offset == author["offset"]

        # the committer timestamp doesn't come from the patch
        assert commit.committer.time > commit.author.time
        bits = r.stdout.split()
        assert bits[0] == "Commit"
        assert bits[2] == "Updating"

        with repo.working_copy.tabular.session() as sess:
            name = sess.scalar(
                f"""SELECT name FROM {H.POINTS.LAYER} WHERE {H.POINTS.LAYER_PK} = 1095;"""
            )
            assert name is None

        # Check that the `kart create-patch` output is the same as our original patch file had.
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0, r.stderr
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open("r", encoding="utf-8"))

        assert patch["kart.patch/v1"] == original_patch["kart.patch/v1"]
        assert patch["kart.diff/v1+hexwkb"] == original_patch["kart.diff/v1+hexwkb"]


def test_apply_with_no_working_copy_with_no_commit(data_archive_readonly, cli_runner):
    with data_archive_readonly("points"):
        r = cli_runner.invoke(
            ["apply", "--no-commit", patches / "updates-only.kartpatch"]
        )
        assert r.exit_code == 45
        assert "--no-commit requires a working copy" in r.stderr


def test_apply_with_working_copy_with_no_commit(data_working_copy, cli_runner):
    patch_filename = "updates-only.kartpatch"
    message = "Change the Coromandel"
    with data_working_copy("points") as (repo_dir, wc_path):
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", "--no-commit", patch_path])
        assert r.exit_code == 0, r.stderr

        repo = KartRepo(repo_dir)

        # no commit was made
        commit = repo.head_commit
        assert commit.message != message

        bits = r.stdout.split()
        assert bits[0] == "Updating"

        # Check that the working copy diff is the same as the original patch file
        r = cli_runner.invoke(["diff", "-o", "json"])
        assert r.exit_code == 0
        patch = json.loads(r.stdout)
        original_patch = json.load(patch_path.open("r", encoding="utf-8"))

        assert patch["kart.diff/v1+hexwkb"] == original_patch["kart.diff/v1+hexwkb"]


def test_apply_multiple_dataset_patch_roundtrip(data_archive, cli_runner):
    with data_archive("au-census"):
        r = cli_runner.invoke(["create-patch", "main"])
        assert r.exit_code == 0, r.stderr
        patch_text = r.stdout
        patch_json = json.loads(patch_text)
        assert set(patch_json["kart.diff/v1+hexwkb"].keys()) == {
            "census2016_sdhca_ot_ra_short",
            "census2016_sdhca_ot_sos_short",
        }

        # note: repo's current branch is 'branch1' which doesn't have the commit on it,
        # so the patch applies cleanly.
        r = cli_runner.invoke(["apply", "-"], input=patch_text)
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0, r.stderr
        new_patch_json = json.loads(r.stdout)

        assert new_patch_json == patch_json


@pytest.mark.slow
def test_apply_benchmark(data_working_copy, benchmark, cli_runner, monkeypatch):
    from kart import apply

    with data_working_copy("points") as (repo_dir, wc_path):
        # Create a branch we can use later; don't switch to it
        r = cli_runner.invoke(["branch", "-c", "savepoint"])
        assert r.exit_code == 0, r.stderr

        # Generate a large change and commit it
        repo = KartRepo(repo_dir)
        with repo.working_copy.tabular.session() as sess:
            sess.execute(
                "UPDATE nz_pa_points_topo_150k SET name = 'bulk_' || Coalesce(name, 'null')"
            )

        r = cli_runner.invoke(["commit", "-m", "rename everything"])
        assert r.exit_code == 0, r.stderr

        # Make it into a patch
        r = cli_runner.invoke(["create-patch", "HEAD"])
        assert r.exit_code == 0, r.stderr
        patch_text = r.stdout
        patch_json = json.loads(patch_text)
        assert patch_json["kart.patch/v1"]["message"] == "rename everything"

        # Now switch to our savepoint branch and apply the patch
        r = cli_runner.invoke(["checkout", "savepoint"])
        assert r.exit_code == 0, r.stderr

        # wrap the apply command with benchmarking
        orig_apply_patch = apply.apply_patch

        def _benchmark_apply(*args, **kwargs):
            # one round/iteration isn't very statistical, but hopefully crude idea
            return benchmark.pedantic(
                orig_apply_patch, args=args, kwargs=kwargs, rounds=1, iterations=1
            )

        monkeypatch.setattr(apply, "apply_patch", _benchmark_apply)

        cli_runner.invoke(["apply", "-"], input=patch_text)


def test_apply_attach_files(data_working_copy, cli_runner):
    patch_filename = "points-attach-files.kartpatch"
    with data_working_copy("points") as (repo_dir, wc_path):
        patch_path = patches / patch_filename
        r = cli_runner.invoke(["apply", patch_path])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show", "--diff-files"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines()[1:] == [
            "Author: Andrew Olsen <andrew.olsen@koordinates.com>",
            "Date:   Sat Jun 11 23:03:58 2022 +1200",
            "",
            "    Add attachments to nz_pa_points_topo_150k",
            "",
            "+++ LICENSE.txt",
            "+ NZ Pa Points (Topo, 1:50k)",
            "+ https://data.linz.govt.nz/layer/50308-nz-pa-points-topo-150k/",
            "+ Land Information New Zealand",
            "+ CC-BY",
            "+ ",
            "+++ logo.png",
            "+ (binary file f8555b6)",
        ]


def test_apply_partial_feature_with_attribute_changes(data_archive, cli_runner):
    """Test that patches can contain partial features (some fields missing, and no geometry)"""
    with data_archive("points") as repo_dir:
        # Get the original feature from base commit
        repo = KartRepo(repo_dir)
        base_commit = "1582725544d9122251acd4b3fc75b5c88ac3fd17"
        base_tree = repo[base_commit].peel(pygit2.Tree)
        base_ds = repo.structure(base_tree).datasets()["nz_pa_points_topo_150k"]
        original_feature = base_ds.get_feature(1182)

        # Verify original has geometry and other fields
        assert original_feature["geom"] is not None
        original_geom = original_feature["geom"]
        original_macronated = original_feature["macronated"]
        original_t50_fid = original_feature["t50_fid"]

        patch = {
            "kart.patch/v1": {
                "base": base_commit,
                "message": "Partial feature edit",
                "authorName": "Test",
                "authorEmail": "test@example.com",
                "authorTime": "2025-10-20T02:42:58.665Z",
            },
            "kart.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "feature": [
                        {
                            "+": {
                                "fid": 1182,
                                "name_ascii": "Updated Name",
                            }
                        }
                    ]
                }
            },
        }

        r = cli_runner.invoke(["apply", "-"], input=json.dumps(patch))
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["show", "-o", "json", "HEAD"])
        assert r.exit_code == 0
        show = json.loads(r.stdout)
        features = show["kart.diff/v1+hexwkb"]["nz_pa_points_topo_150k"]["feature"]
        assert len(features) == 1
        assert features[0]["+"]["fid"] == 1182
        assert features[0]["+"]["name_ascii"] == "Updated Name"

        # Verify the complete feature was written with all fields preserved (except the one we changed)
        repo = KartRepo(repo_dir)
        head_ds = repo.structure("HEAD").datasets()["nz_pa_points_topo_150k"]
        updated_feature = head_ds.get_feature(1182)
        assert updated_feature["name_ascii"] == "Updated Name"
        assert updated_feature["geom"] == original_geom
        assert updated_feature["macronated"] == original_macronated
        assert updated_feature["t50_fid"] == original_t50_fid


def test_apply_partial_feature_missing_pk_field(data_archive, cli_runner):
    """Test that missing PK field raises clear error."""
    patch = {
        "kart.patch/v1": {
            "base": "1582725544d9122251acd4b3fc75b5c88ac3fd17",
            "message": "Invalid patch",
            "authorName": "Test",
            "authorEmail": "test@example.com",
            "authorTime": "2025-10-20T02:42:58.665Z",
        },
        "kart.diff/v1+hexwkb": {
            "nz_pa_points_topo_150k": {
                "feature": [
                    {
                        "+": {
                            "name_ascii": "No PK Here",
                        }
                    }
                ]
            }
        },
    }

    with data_archive("points"):
        r = cli_runner.invoke(["apply", "-"], input=json.dumps(patch))
        assert r.exit_code != 0
        assert "missing required primary key field" in r.stderr
        assert "'fid'" in r.stderr


def test_apply_partial_feature_insert_with_missing_fields(data_archive, cli_runner):
    """Test that inserting a new feature with missing fields fails with clear error."""
    patch = {
        "kart.patch/v1": {
            "base": "1582725544d9122251acd4b3fc75b5c88ac3fd17",
            "message": "Insert partial feature",
            "authorName": "Test",
            "authorEmail": "test@example.com",
            "authorTime": "2025-10-20T02:42:58.665Z",
        },
        "kart.diff/v1+hexwkb": {
            "nz_pa_points_topo_150k": {
                "feature": [
                    {
                        "+": {
                            "fid": 999999,
                            "name_ascii": "New Feature",
                        }
                    }
                ]
            }
        },
    }

    with data_archive("points"):
        r = cli_runner.invoke(["apply", "-"], input=json.dumps(patch))
        assert r.exit_code != 0
        assert "Cannot insert new feature" in r.stderr
        assert "999999" in r.stderr
        assert "missing fields" in r.stderr


def test_apply_delete_feature_with_only_pk(data_archive, cli_runner):
    """Test that deleting a feature requires only the PK field."""
    with data_archive("points") as repo_dir:
        # Verify feature 1182 exists before deletion
        repo = KartRepo(repo_dir)
        base_commit = "1582725544d9122251acd4b3fc75b5c88ac3fd17"
        base_tree = repo[base_commit].peel(pygit2.Tree)
        base_ds = repo.structure(base_tree).datasets()["nz_pa_points_topo_150k"]
        original_feature = base_ds.get_feature(1182)
        assert original_feature is not None

        # Delete with only PK field specified
        patch = {
            "kart.patch/v1": {
                "base": base_commit,
                "message": "Delete feature",
                "authorName": "Test",
                "authorEmail": "test@example.com",
                "authorTime": "2025-10-20T02:42:58.665Z",
            },
            "kart.diff/v1+hexwkb": {
                "nz_pa_points_topo_150k": {
                    "feature": [
                        {
                            "-": {
                                "fid": 1182,
                            }
                        }
                    ]
                }
            },
        }

        r = cli_runner.invoke(["apply", "-"], input=json.dumps(patch))
        assert r.exit_code == 0, r.stderr

        # Verify feature was deleted
        repo = KartRepo(repo_dir)
        head_ds = repo.structure("HEAD").datasets()["nz_pa_points_topo_150k"]
        with pytest.raises(KeyError):
            head_ds.get_feature(1182)
