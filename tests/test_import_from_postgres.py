import os


def test_import_from_postgres_db_without_postgis_extension(
    no_postgis_db, cli_runner, tmp_path, chdir
):
    """Test importing a non-spatial table from a Postgres database which doesn't have a PostGIS extension"""
    with no_postgis_db.connect() as conn:
        test_table = "test_table"
        create_sample_table(conn, test_table)

    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    # empty dir
    r = cli_runner.invoke(["init", str(repo_path)])

    assert r.exit_code == 0, r
    assert (repo_path / ".kart" / "HEAD").exists()

    postgres_url = os.environ["KART_POSTGRES_URL"]

    # Import the test_table from the PostgreSQL container:
    with chdir(repo_path):
        r = cli_runner.invoke(["import", f"{postgres_url}", f"{test_table}"])

        assert r.exit_code == 0, r.stderr


def create_sample_table(conn, test_table):
    """Create a sample table in the database and add a few rows to it"""
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {test_table} (
            id int PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            age INTEGER
        )
    """
    )
    conn.execute(
        f"""
        INSERT INTO {test_table} (id, name, age)
        VALUES
            (1, 'Alice', 25),
            (2, 'Bob', 30),
            (3, 'Charlie', 35)
        ON CONFLICT (id) DO NOTHING
    """
    )
