ALL_TILES = [
    "32b5fe23040b236dfe469456dd8f7ebbb4dcb3326305ba3e183714a32e4dd1ac * auckland/.point-cloud-dataset.v1/tile/08/auckland_1_3",
    "c9de49a81e30153254fc65c8eb291545cbb30b520aff7d4ec0cff0fab086c60b * auckland/.point-cloud-dataset.v1/tile/36/auckland_3_0",
    "842b5d3fac074264a128559c0fc0ff462c4d15349fa1e88ebfa8a866df024394 * auckland/.point-cloud-dataset.v1/tile/51/auckland_2_1",
    "cfa530937fdbfde520ad2f2c56e64062b19063abee659d1ec8c02544b28b1b88 * auckland/.point-cloud-dataset.v1/tile/56/auckland_2_0",
    "583789bcea43177dbba446574f00f817b2f89782fcf71709d911b2ad10872d0e * auckland/.point-cloud-dataset.v1/tile/6e/auckland_2_2",
    "41e855de94194ea12da99ed84612050eccd9f79523b275337012e43e69e425e9 * auckland/.point-cloud-dataset.v1/tile/72/auckland_1_2",
    "adbc1dc7fc99c88fcb627b9c40cdb56c211b791fe9cf83fe066b1a9932c12569 * auckland/.point-cloud-dataset.v1/tile/8b/auckland_0_0",
    "0a696f35ab1404bbe9663e52774aaa800b0cf308ad2e5e5a9735d1c8e8b0a8c4 * auckland/.point-cloud-dataset.v1/tile/91/auckland_3_1",
    "4269cf4db9798d077786bb2f842aa28608fd3a52dd7cdaa0fa66bc1cb47cc483 * auckland/.point-cloud-dataset.v1/tile/92/auckland_0_3",
    "0fd4dc03d2e9963658cf70e9d52fa1eaa7292da71d89d0188cfa88d5afb75ab6 * auckland/.point-cloud-dataset.v1/tile/96/auckland_3_3",
    "27411724d0de7c09913eb3d22b4f1d352afb8fa5b786403f59474e47c7492d9d * auckland/.point-cloud-dataset.v1/tile/9b/auckland_3_2",
    "f28e69d8a1c9ce6b6494cd17645c0316af9c28641ccd9058f32dc1f60da50a13 * auckland/.point-cloud-dataset.v1/tile/9c/auckland_0_1",
    "4a406d29abc0b57325449d9ebe69400441742ac3d8116133f25a1d160c2a2cc7 * auckland/.point-cloud-dataset.v1/tile/c5/auckland_2_3",
    "ab12b4d27ce40f976af3584f44ab04bdeba0de32304ed7f6baf7ed264dba6ca0 * auckland/.point-cloud-dataset.v1/tile/c7/auckland_0_2",
    "8624133a3fa257e528fe1e0a01e1f2a7fa9f453cbe4fe283a31eabaf77c68794 * auckland/.point-cloud-dataset.v1/tile/d5/auckland_1_1",
    "644fa013aca1e97827be4e8cc36a5a6f347cc7fba3764c560386ed59a1b571e9 * auckland/.point-cloud-dataset.v1/tile/e8/auckland_1_0",
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
