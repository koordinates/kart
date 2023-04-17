import shutil
import pytest

from .fixtures import requires_gdal_info  # noqa

from kart.lfs_util import get_hash_and_size_of_file
from kart.exceptions import WORKING_COPY_OR_IMPORT_CONFLICT


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


def test_working_copy_add_or_remove_rat(
    cli_runner,
    data_archive,
    requires_gdal_info,
    requires_git_lfs,
):
    with data_archive("raster/erosion.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml"
        pam_path.rename(
            repo_path / "erorisk_si" / "erorisk_silcdb4.tif.aux.xml.obsolete"
        )

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
            "      2 deletes",
            "    tile:",
            "      1 updates",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        EXPECTED_DIFF = [
            "--- erorisk_si:meta:band/band-1-categories.json",
            "- {",
            '-   "1": "High landslide risk - delivery to stream",',
            '-   "2": "High landslide risk - non-delivery to steam",',
            '-   "3": "Moderate earthflow risk",',
            '-   "4": "Severe earthflow risk",',
            '-   "5": "Gully risk"',
            "- }",
            "--- erorisk_si:meta:band/band-1-rat.xml",
            '- <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">',
            '-     <FieldDefn index="0">',
            "-         <Name>Histogram</Name>",
            "-         <Type>1</Type>",
            "-         <Usage>1</Usage>",
            "-     </FieldDefn>",
            '-     <FieldDefn index="1">',
            "-         <Name>Class_Names</Name>",
            "-         <Type>2</Type>",
            "-         <Usage>2</Usage>",
            "-     </FieldDefn>",
            '-     <FieldDefn index="2">',
            "-         <Name>Red</Name>",
            "-         <Type>0</Type>",
            "-         <Usage>6</Usage>",
            "-     </FieldDefn>",
            '-     <FieldDefn index="3">',
            "-         <Name>Green</Name>",
            "-         <Type>0</Type>",
            "-         <Usage>7</Usage>",
            "-     </FieldDefn>",
            '-     <FieldDefn index="4">',
            "-         <Name>Blue</Name>",
            "-         <Type>0</Type>",
            "-         <Usage>8</Usage>",
            "-     </FieldDefn>",
            '-     <FieldDefn index="5">',
            "-         <Name>Opacity</Name>",
            "-         <Type>0</Type>",
            "-         <Usage>9</Usage>",
            "-     </FieldDefn>",
            "- </GDALRasterAttributeTable>",
            "- ",
            "--- erorisk_si:tile:erorisk_silcdb4",
            "+++ erorisk_si:tile:erorisk_silcdb4",
            "-                                  pamName = erorisk_silcdb4.tif.aux.xml",
            "-                                   pamOid = sha256:d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            "-                                  pamSize = 36908",
        ]
        assert r.stdout.splitlines() == EXPECTED_DIFF

        r = cli_runner.invoke(["commit", "-m", "Remove PAM"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0
        assert r.stdout.splitlines()[4:] == ["    Remove PAM", ""] + EXPECTED_DIFF

        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml"
        assert not pam_path.exists()

        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml.obsolete"
        pam_path.rename(repo_path / "erorisk_si" / "erorisk_silcdb4.tif.aux.xml")

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
            "      2 inserts",
            "    tile:",
            "      1 updates",
        ]

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        EXPECTED_DIFF = [
            "+++ erorisk_si:meta:band/band-1-categories.json",
            "+ {",
            '+   "1": "High landslide risk - delivery to stream",',
            '+   "2": "High landslide risk - non-delivery to steam",',
            '+   "3": "Moderate earthflow risk",',
            '+   "4": "Severe earthflow risk",',
            '+   "5": "Gully risk"',
            "+ }",
            "+++ erorisk_si:meta:band/band-1-rat.xml",
            '+ <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">',
            '+     <FieldDefn index="0">',
            "+         <Name>Histogram</Name>",
            "+         <Type>1</Type>",
            "+         <Usage>1</Usage>",
            "+     </FieldDefn>",
            '+     <FieldDefn index="1">',
            "+         <Name>Class_Names</Name>",
            "+         <Type>2</Type>",
            "+         <Usage>2</Usage>",
            "+     </FieldDefn>",
            '+     <FieldDefn index="2">',
            "+         <Name>Red</Name>",
            "+         <Type>0</Type>",
            "+         <Usage>6</Usage>",
            "+     </FieldDefn>",
            '+     <FieldDefn index="3">',
            "+         <Name>Green</Name>",
            "+         <Type>0</Type>",
            "+         <Usage>7</Usage>",
            "+     </FieldDefn>",
            '+     <FieldDefn index="4">',
            "+         <Name>Blue</Name>",
            "+         <Type>0</Type>",
            "+         <Usage>8</Usage>",
            "+     </FieldDefn>",
            '+     <FieldDefn index="5">',
            "+         <Name>Opacity</Name>",
            "+         <Type>0</Type>",
            "+         <Usage>9</Usage>",
            "+     </FieldDefn>",
            "+ </GDALRasterAttributeTable>",
            "+ ",
            "--- erorisk_si:tile:erorisk_silcdb4",
            "+++ erorisk_si:tile:erorisk_silcdb4",
            "+                                  pamName = erorisk_silcdb4.tif.aux.xml",
            "+                                   pamOid = sha256:d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            "+                                  pamSize = 36908",
        ]
        assert r.stdout.splitlines() == EXPECTED_DIFF

        r = cli_runner.invoke(["commit", "-m", "Revert edit: Restore original PAM"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0
        assert (
            r.stdout.splitlines()[4:]
            == ["    Revert edit: Restore original PAM", ""] + EXPECTED_DIFF
        )

        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml"
        assert get_hash_and_size_of_file(pam_path) == (
            "d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            36908,
        )


def test_working_copy_add_similar_rat(
    cli_runner,
    data_archive,
    requires_gdal_info,
    requires_git_lfs,
):
    with data_archive("raster/erosion.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0
        assert r.stdout.splitlines() == []
        tif_path = repo_path / "erorisk_si/erorisk_silcdb4.tif"
        shutil.copy(tif_path, repo_path / "erorisk_si/erorisk_silcdb5.tif")
        pam_path = repo_path / "erorisk_si/erorisk_silcdb4.tif.aux.xml"
        new_pam_path = repo_path / "erorisk_si/erorisk_silcdb5.tif.aux.xml"
        shutil.copy(pam_path, repo_path / new_pam_path)

        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        EXPECTED_TILE_DIFF = [
            "+++ erorisk_si:tile:erorisk_silcdb5",
            "+                                     name = erorisk_silcdb5.tif",
            "+                                   format = geotiff",
            "+                              crs84Extent = POLYGON((172.6754107 -43.7555641,172.6748326 -43.8622096,172.8170036 -43.8625257,172.8173289 -43.755879,172.6754107 -43.7555641,172.6754107 -43.7555641))",
            "+                               dimensions = 762x790",
            "+                             nativeExtent = POLYGON((1573869.73 5155224.347,1573869.73 5143379.674,1585294.591 5143379.674,1585294.591 5155224.347,1573869.73 5155224.347))",
            "+                                      oid = sha256:c4bbea4d7cfd54f4cdbca887a1b358a81710e820a6aed97cdf3337fd3e14f5aa",
            "+                                     size = 604652",
            "+                                  pamName = erorisk_silcdb5.tif.aux.xml",
            "+                                   pamOid = sha256:d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            "+                                  pamSize = 36908",
        ]
        assert r.stdout.splitlines() == EXPECTED_TILE_DIFF

        # Try to add a conflicting category label in the new PAM file - it's not allowed.
        _set_category_labels(new_pam_path, {5: "Seagull risk"})
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        assert r.stdout.splitlines()[0:26] == [
            "--- erorisk_si:meta:band/band-1-categories.json",
            "+++ erorisk_si:meta:band/band-1-categories.json",
            "- {",
            '-   "1": "High landslide risk - delivery to stream",',
            '-   "2": "High landslide risk - non-delivery to steam",',
            '-   "3": "Moderate earthflow risk",',
            '-   "4": "Severe earthflow risk",',
            '-   "5": "Gully risk"',
            "- }",
            "+ <<<<<<< ",
            "+ {",
            '+   "1": "High landslide risk - delivery to stream",',
            '+   "2": "High landslide risk - non-delivery to steam",',
            '+   "3": "Moderate earthflow risk",',
            '+   "4": "Severe earthflow risk",',
            '+   "5": "Gully risk"',
            "+ }",
            "+ ======== ",
            "+ {",
            '+   "1": "High landslide risk - delivery to stream",',
            '+   "2": "High landslide risk - non-delivery to steam",',
            '+   "3": "Moderate earthflow risk",',
            '+   "4": "Severe earthflow risk",',
            '+   "5": "Seagull risk"',
            "+ }",
            "+ >>>>>>> ",
        ]

        r = cli_runner.invoke(["commit", "-m", "Add new tile"])
        assert r.exit_code == WORKING_COPY_OR_IMPORT_CONFLICT
        assert (
            "Committing more than one 'band/band-1-categories.json' for 'erorisk_si' is not supported"
            in r.stderr
        )

        # But we are allowed to add new categories or to not re-specify the old ones in the new PAM file.
        _set_category_labels(new_pam_path, {1: "", 3: "", 5: "", 6: "Meteorite risk"})
        r = cli_runner.invoke(["diff"])
        assert r.exit_code == 0
        EXPECTED_META_DIFF = [
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
            '+   "1": "High landslide risk - delivery to stream",',
            '+   "2": "High landslide risk - non-delivery to steam",',
            '+   "3": "Moderate earthflow risk",',
            '+   "4": "Severe earthflow risk",',
            '+   "5": "Gully risk",',
            '+   "6": "Meteorite risk"',
            "+ }",
        ]
        assert r.stdout.splitlines()[0:17] == EXPECTED_META_DIFF

        r = cli_runner.invoke(["commit", "-m", "Add new tile"])
        assert r.exit_code == 0
        r = cli_runner.invoke(["show"])
        assert r.exit_code == 0
        assert (
            r.stdout.splitlines()[4:23] == ["    Add new tile", ""] + EXPECTED_META_DIFF
        )


def _set_category_labels(pam_path, category_labels):
    from xml.dom import minidom

    with minidom.parse(str(pam_path)) as parsed:
        bands = parsed.getElementsByTagName("PAMRasterBand")
        for band in bands:
            category_column = None
            rats = band.getElementsByTagName("GDALRasterAttributeTable")
            if not rats:
                continue
            rat = rats[0]

            for child in rat.childNodes:
                if getattr(child, "tagName", None) != "FieldDefn":
                    continue
                field_defn = child
                usage = field_defn.getElementsByTagName("Usage")[0]
                if usage:
                    usage_text = usage.firstChild.nodeValue.strip()
                    if usage_text == "2":
                        category_column = int(field_defn.getAttribute("index"))
                        break

            for row in rat.getElementsByTagName("Row"):
                row_id = int(row.getAttribute("index"))
                if row_id in category_labels:
                    category = row.getElementsByTagName("F")[category_column]
                    if not category.hasChildNodes():
                        category.appendChild(parsed.createTextNode(""))
                    category.firstChild.nodeValue = category_labels[row_id]

        pam_path.write_text(parsed.toxml())
