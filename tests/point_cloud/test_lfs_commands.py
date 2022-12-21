ALL_TILES = [
    "bf4210be91ea2013ff13961a885cc9b16cb631a5b54cc89276010d1e4adf74e2 * auckland/.point-cloud-dataset.v1/tile/08/auckland_1_3",
    "ec80af6cae31be5318f9380cd953b25469bd8ecda25086deca2b831bbb89168a * auckland/.point-cloud-dataset.v1/tile/36/auckland_3_0",
    "467dbced134249ad341e762737ca42e731f92dadd2d290cf093b68c788aa0067 * auckland/.point-cloud-dataset.v1/tile/51/auckland_2_1",
    "c7874972e856eaff4d28fa851b9abc72be9056caa41187211de0258b5ac30f28 * auckland/.point-cloud-dataset.v1/tile/56/auckland_2_0",
    "a968f575322d6de93ebc10f972a4b20a36f918f4f8f76891da4d67232f3976e4 * auckland/.point-cloud-dataset.v1/tile/6e/auckland_2_2",
    "3ba3a4bd4629af7c934c61fa132021bf2b3bdd1a52d981315ce5ecb09d71e10a * auckland/.point-cloud-dataset.v1/tile/72/auckland_1_2",
    "a1862450841dede2759af665825403e458dfa551c095d9a65ea6e6765aeae0f7 * auckland/.point-cloud-dataset.v1/tile/8b/auckland_0_0",
    "817b6ddadd95166012143df55fa73dd6c5a8b42b603c33d1b6c38f187261096e * auckland/.point-cloud-dataset.v1/tile/91/auckland_3_1",
    "11ba773069c7e935735f7076b2fa44334d0bb41c4742d8cd8111f575359a773c * auckland/.point-cloud-dataset.v1/tile/92/auckland_0_3",
    "64895828ea03ce9cafaef4f387338aab8d498c8eccaef1503b8b3bd97e57c5a3 * auckland/.point-cloud-dataset.v1/tile/96/auckland_3_3",
    "d380a98414ab209f36c7fba4734b02f67de519756e341837217716c5b4768339 * auckland/.point-cloud-dataset.v1/tile/9b/auckland_3_2",
    "9c49d1b59f33fa3f46ca6caf8cfc26e13e7e951758b41d811a9b734918ad1711 * auckland/.point-cloud-dataset.v1/tile/9c/auckland_0_1",
    "7d160940ad3087f610ccf6d41f5b7a49a4425bae61bf0ca59e3693910b5b11d4 * auckland/.point-cloud-dataset.v1/tile/c5/auckland_2_3",
    "23c4bb0642bf467bb35ece586f5460f7f4d32288832796458bcbe1a928b32fb4 * auckland/.point-cloud-dataset.v1/tile/c7/auckland_0_2",
    "add2d011a19b39c0c8d70ed2313ad4955b1e0faf9a24394ab1a103930580a267 * auckland/.point-cloud-dataset.v1/tile/d5/auckland_1_1",
    "7041a3ee11a33d750289d44ef4096fd7efcc195958d52f56ab363415f9363e61 * auckland/.point-cloud-dataset.v1/tile/e8/auckland_1_0",
]


def test_ls_files(cli_runner, data_archive):
    with data_archive("point-cloud/auckland.tgz") as repo_path:
        # Add an extra commit at HEAD with no tiles added.
        r = cli_runner.invoke(["commit-files", "foo=bar", "-m", "Extra commit"])
        assert r.exit_code == 0, r.stderr

        r = cli_runner.invoke(["lfs+", "ls-files", "--all"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ALL_TILES

        # Current branch has all tiles
        r = cli_runner.invoke(["lfs+", "ls-files"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ALL_TILES

        # Current commit has no tiles.
        r = cli_runner.invoke(["lfs+", "ls-files", "HEAD"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []

        # Prev commit has all tiles.
        r = cli_runner.invoke(["lfs+", "ls-files", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == ALL_TILES

        # No tiles committed between this commit and previous.
        r = cli_runner.invoke(["lfs+", "ls-files", "HEAD", "HEAD^"])
        assert r.exit_code == 0, r.stderr
        assert r.stdout.splitlines() == []
