from io import StringIO

from osgeo import gdal

from kart.lfs_util import get_oid_and_size_of_file
from kart.raster import pam_util


ORIGINAL = """
<PAMDataset>
  <PAMRasterBand band="1">
    <Description>erorisk_si</Description>
    <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">
      <FieldDefn index="0">
        <Name>Histogram</Name>
        <Type>1</Type>
        <Usage>1</Usage>
      </FieldDefn>
    </GDALRasterAttributeTable>
  </PAMRasterBand>
</PAMDataset>
""".lstrip()

MODIFIED = """
<PAMDataset>
  <PAMRasterBand band="1">
    <Description>erorisk_si</Description>
    <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">
      <FieldDefn index="0">
        <Name>Histogram</Name>
        <Type>2</Type>
        <Usage>2</Usage>
      </FieldDefn>
    </GDALRasterAttributeTable>
  </PAMRasterBand>
</PAMDataset>
""".lstrip()

WITH_STATS = """
<PAMDataset>
  <PAMRasterBand band="1">
    <Description>erorisk_si</Description>
    <Histograms>
      <HistItem>
        <HistMin>-0.5</HistMin>
        <HistMax>255.5</HistMax>
        <BucketCount>256</BucketCount>
        <IncludeOutOfRange>1</IncludeOutOfRange>
        <Approximate>0</Approximate>
        <HistCounts>1|2|3|4|5|6|7|8|9|10</HistCounts>
      </HistItem>
    </Histograms>
    <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">
      <FieldDefn index="0">
        <Name>Histogram</Name>
        <Type>1</Type>
        <Usage>1</Usage>
      </FieldDefn>
    </GDALRasterAttributeTable>
    <Metadata>
      <MDI key="STATISTICS_MAXIMUM">9</MDI>
      <MDI key="STATISTICS_MEAN">2.943554603143</MDI>
      <MDI key="STATISTICS_MINIMUM">0</MDI>
      <MDI key="STATISTICS_STDDEV">3.1322497995378</MDI>
      <MDI key="STATISTICS_VALID_PERCENT">100</MDI>
    </Metadata>
  </PAMRasterBand>
</PAMDataset>
""".lstrip()

MODIFIED_WITH_STATS = """
<PAMDataset>
  <PAMRasterBand band="1">
    <Description>erorisk_si</Description>
    <Histograms>
      <HistItem>
        <HistMin>-0.5</HistMin>
        <HistMax>255.5</HistMax>
        <BucketCount>256</BucketCount>
        <IncludeOutOfRange>1</IncludeOutOfRange>
        <Approximate>0</Approximate>
        <HistCounts>1|2|3|4|5|6|7|8|9|10</HistCounts>
      </HistItem>
    </Histograms>
    <GDALRasterAttributeTable Row0Min="0" BinSize="1" tableType="thematic">
      <FieldDefn index="0">
        <Name>Histogram</Name>
        <Type>2</Type>
        <Usage>2</Usage>
      </FieldDefn>
    </GDALRasterAttributeTable>
    <Metadata>
      <MDI key="STATISTICS_MAXIMUM">9</MDI>
      <MDI key="STATISTICS_MEAN">2.943554603143</MDI>
      <MDI key="STATISTICS_MINIMUM">0</MDI>
      <MDI key="STATISTICS_STDDEV">3.1322497995378</MDI>
      <MDI key="STATISTICS_VALID_PERCENT">100</MDI>
    </Metadata>
  </PAMRasterBand>
</PAMDataset>
""".lstrip()


def is_same_xml_ignoring_stats(lhs, rhs):
    # pam_util takes string paths or file-like objects, so we wrap our strings in StringIO.
    return pam_util.is_same_xml_ignoring_stats(StringIO(lhs), StringIO(rhs))


def test_is_same_xml_ignoring_stats():
    assert is_same_xml_ignoring_stats(ORIGINAL, ORIGINAL)
    assert is_same_xml_ignoring_stats(ORIGINAL, WITH_STATS)
    assert is_same_xml_ignoring_stats(WITH_STATS, ORIGINAL)

    assert not is_same_xml_ignoring_stats(ORIGINAL, MODIFIED)
    assert not is_same_xml_ignoring_stats(MODIFIED, ORIGINAL)

    assert not is_same_xml_ignoring_stats(ORIGINAL, MODIFIED_WITH_STATS)
    assert not is_same_xml_ignoring_stats(MODIFIED_WITH_STATS, ORIGINAL)


def test_add_stats_to_new_pam(
    cli_runner,
    data_archive,
    requires_git_lfs,
):
    with data_archive("raster/aerial.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        tile_path = repo_path / "aerial" / "aerial.tif"
        pam_path = repo_path / "aerial" / "aerial.tif.aux.xml"

        assert not pam_path.exists()

        # This sort of command causes stats to be generated, but we don't want
        # to show it as a diff to the user unless they make further changes:
        gdal.Info(str(tile_path), options=["-stats", "-hist"])

        assert pam_path.is_file()

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Nothing to commit, working copy clean",
        ]

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        r = cli_runner.invoke(["reset", "--discard-changes"])
        assert r.exit_code == 0

        assert not pam_path.exists()

        # Real changes are not suppressed:
        pam_path.write_text("<whatever />")

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Changes in working copy:",
            '  (use "kart commit" to commit)',
            '  (use "kart restore" to discard changes)',
            "",
            "  aerial:",
            "    tile:",
            "      1 updates",
        ]

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 1

        r = cli_runner.invoke(["reset", "--discard-changes"])
        assert r.exit_code == 0

        assert not pam_path.exists()


def test_add_stats_to_existing_pam(
    cli_runner,
    data_archive,
    requires_git_lfs,
):
    with data_archive("raster/erosion.tgz") as repo_path:
        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        tile_path = repo_path / "erorisk_si" / "erorisk_silcdb4.tif"
        pam_path = repo_path / "erorisk_si" / "erorisk_silcdb4.tif.aux.xml"

        orig_oid_and_size = (
            "d8f514e654a81bdcd7428886a15e300c56b5a5ff92898315d16757562d2968ca",
            36908,
        )
        assert get_oid_and_size_of_file(pam_path) == orig_oid_and_size

        # This sort of command causes stats to be generated, but we don't want
        # to show it as a diff to the user unless they make further changes:
        gdal.Info(str(tile_path), options=["-stats", "-hist"])

        assert get_oid_and_size_of_file(pam_path) != orig_oid_and_size

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == [
            "On branch main",
            "",
            "Nothing to commit, working copy clean",
        ]

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 0

        # Real changes are not suppressed:
        pam_path.write_text(
            pam_path.read_text().replace(" risk", " opportunity"), newline="\n"
        )

        r = cli_runner.invoke(["status"])
        assert r.exit_code == 0, r.stderr
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

        r = cli_runner.invoke(["diff", "--exit-code"])
        assert r.exit_code == 1
