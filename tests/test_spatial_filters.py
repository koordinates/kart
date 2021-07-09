import pytest

from kart.repo import KartRepo


H = pytest.helpers.helpers()


SPATIAL_FILTERS = {
    "points": "0103000000010000000E0000005C94E90C569E65406777FF1A808F41C06BB6213CEFAD65407C23003250DE41C034D22DD5A8BA65402B9F88C2F51642C067EA6918BEC5654058053150C14542C039FDEDDA5CCE654016A3F10F008E42C0A11342F39FD86540D96B72D5F2E942C0DE22F65F9ADF6540917F6AADFAF242C0B128686B45E265404D5F1AA933E442C0E1FBBF3ABFCD6540D1239929B65342C0B8DAFB208FBE65406E60D0E439FA41C009BE7B726CB165400CFC0F8240CC41C0BBB3C5FBB3AC654002A68F76D8A441C07F97FFD7C59F6540E4611F18A68541C05C94E90C569E65406777FF1A808F41C0",
    "polygons": "01060000000100000001030000000100000009000000EC21ADE020DC65403CFAB52CF3E942C0CF9F2289C0E06540F1BFAEBD38FD42C00E41DB9104E86540836341F410FD42C0130676A754EC65409196FB0A81EB42C04BB42C8C68EC6540F8BB59CA2BD242C01531A50CABE765404B7D5EBBFCBF42C0CF9F2289C0E06540DC20F1F1D4BF42C05B7E1AAA48DC6540CDED36DB64D142C0EC21ADE020DC65403CFAB52CF3E942C0",
}


@pytest.mark.parametrize(
    "archive,table",
    [
        pytest.param("points", H.POINTS.LAYER, id="points"),
        pytest.param("polygons", H.POLYGONS.LAYER, id="polygons"),
        pytest.param("table", H.TABLE.LAYER, id="table"),
    ],
)
def test_spatial_filtered_workingcopy(
    archive, table, data_archive, tmp_path, cli_runner
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

        # Use polygons spatial filter for table archive too - doesn't matter exactly what it is.
        key = "polygons" if archive == "table" else archive
        repo.config["kart.spatialfilter.geometry"] = SPATIAL_FILTERS[key]

        r = cli_runner.invoke(["checkout"])
        assert r.exit_code == 0, r
        wc = repo.working_copy

        with wc.session() as sess:
            feature_count = sess.execute(f"SELECT COUNT(*) FROM {table};").scalar()
            assert feature_count == matching_features[archive]
