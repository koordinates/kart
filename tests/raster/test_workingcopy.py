import pytest

from .fixtures import requires_gdal_info  # noqa

from kart.lfs_util import get_hash_and_size_of_file


def test_working_copy_edit(
    cli_runner, data_archive, requires_gdal_info, requires_git_lfs
):
    with data_archive("raster/aerial.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        from osgeo import gdal

        # Drop the 4th band:
        translate_options = gdal.TranslateOptions(bandList=[1, 2, 3])

        gdal.Translate(
            str(repo_path / "aerial/aerial.new.tif"),
            str(repo_path / "aerial/aerial.tif"),
            options=translate_options,
        )

        (repo_path / "aerial/aerial.tif").unlink()
        (repo_path / "aerial/aerial.new.tif").rename(repo_path / "aerial/aerial.tif")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            "  aerial:",
            "    meta:",
            "      1 updates",
            "    tile:",
            "      1 updates",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        # NOTE - this test might prove brittle - if so, we can relax the hashes.
        EXPECTED_DIFF = [
            "--- aerial:meta:schema.json",
            "+++ aerial:meta:schema.json",
            "  [",
            "    {",
            '      "dataType": "integer",',
            '      "size": 8,',
            '      "interpretation": "red",',
            '      "unsigned": true',
            "    },",
            "    {",
            '      "dataType": "integer",',
            '      "size": 8,',
            '      "interpretation": "green",',
            '      "unsigned": true',
            "    },",
            "    {",
            '      "dataType": "integer",',
            '      "size": 8,',
            '      "interpretation": "blue",',
            '      "unsigned": true',
            "    },",
            "-   {",
            '-     "dataType": "integer",',
            '-     "size": 8,',
            '-     "interpretation": "alpha",',
            '-     "unsigned": true',
            "-   },",
            "  ]",
            "--- aerial:tile:aerial",
            "+++ aerial:tile:aerial",
            "-                                      oid = sha256:e6cbc8210f9cae3c8b72985e553e97af51fb9c20d17f5a06b7579943fed57b2c",
            "+                                      oid = sha256:60d8c02dbff57aaebd5eccd51f7cdf0f0234d6507591a122f1a683817d8f59e3",
            "-                                     size = 516216",
            "+                                     size = 533940",
        ]
        assert r.stdout.splitlines() == EXPECTED_DIFF

        r = cli_runner.invoke(["commit", "-m", "Remove alpha band"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0
        assert (
            r.stdout.splitlines()[4:] == ["    Remove alpha band", ""] + EXPECTED_DIFF
        )

        assert get_hash_and_size_of_file(repo_path / "aerial/aerial.tif") == (
            "60d8c02dbff57aaebd5eccd51f7cdf0f0234d6507591a122f1a683817d8f59e3",
            533940,
        )

        r = cli_runner.invoke(["reset", "HEAD^"])
        assert r.exit_code == 0

        assert get_hash_and_size_of_file(repo_path / "aerial/aerial.tif") == (
            "e6cbc8210f9cae3c8b72985e553e97af51fb9c20d17f5a06b7579943fed57b2c",
            516216,
        )


@pytest.mark.parametrize(
    "pam_filename",
    [
        "erorisk_silcdb4.tif.aux.xml",
        "ERORISK_SILCDB4.tif.aux.xml",
        "erorisk_silcdb4.TIF.AUX.XML",
    ],
)
def test_working_copy_edit_rat(
    pam_filename,
    cli_runner,
    data_archive,
    requires_gdal_info,
    requires_git_lfs,
):
    with data_archive("raster/erosion.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml"
        pam_path.write_text(pam_path.read_text().replace(" risk", " opportunity"))
        pam_path.rename(repo_path / "erorisk_si" / pam_filename)

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            "  erorisk_si:",
            "    meta:",
            "      1 updates",
            "    tile:",
            "      1 updates",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        EXPECTED_DIFF = [
            "--- erorisk_si:meta:band/band-1-categories.json",
            "+++ erorisk_si:meta:band/band-1-categories.json",
            "- {",
            '-   "1": "High landslide risk - delivery to stream",',
            '-   "2": "High landslide risk - non-delivery to steam",',
            '-   "3": "Moderate earthflow risk",',
            '-   "4": "Severe earthflow risk",',
            '-   "5": "Gully risk"',
            "- }",
            "+ {",
            '+   "1": "High landslide opportunity - delivery to stream",',
            '+   "2": "High landslide opportunity - non-delivery to steam",',
            '+   "3": "Moderate earthflow opportunity",',
            '+   "4": "Severe earthflow opportunity",',
            '+   "5": "Gully opportunity"',
            "+ }",
            "--- erorisk_si:tile:erorisk_silcdb4",
            "+++ erorisk_si:tile:erorisk_silcdb4",
            "-                                   pamOid = sha256:d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            "+                                   pamOid = sha256:1829b97c9fb5d8cc574a41b7af729da794ba0b4880182f820cdbf416f0a328f5",
            "-                                  pamSize = 36908",
            "+                                  pamSize = 36943",
        ]
        precommit_expected_diff = list(EXPECTED_DIFF)
        if pam_filename != "erorisk_silcdb4.tif.aux.xml":
            precommit_expected_diff += [
                f"+                            pamSourceName = {pam_filename}"
            ]
        assert r.stdout.splitlines() == precommit_expected_diff

        r = cli_runner.invoke(["commit", "-m", "Use more positive language"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0
        assert (
            r.stdout.splitlines()[4:]
            == ["    Use more positive language", ""] + EXPECTED_DIFF
        )

        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml"
        assert get_hash_and_size_of_file(pam_path) == (
            "1829b97c9fb5d8cc574a41b7af729da794ba0b4880182f820cdbf416f0a328f5",
            36943,
        )

        r = cli_runner.invoke(["reset", "HEAD^"])
        assert r.exit_code == 0

        assert get_hash_and_size_of_file(pam_path) == (
            "d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            36908,
        )
