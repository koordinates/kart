import pytest
from pathlib import Path
from http.server import HTTPServer, SimpleHTTPRequestHandler
from threading import Thread
import json

from kart.exceptions import ImportSourceError
from kart.repo import KartRepo
from kart.tabular.esri_rest_import_source import (
    ESRIJSONImportSource,
    ESRIRestServerSource,
    ESRIRestImportSource,
)


class TestESRIJSONImportSourceURLAdaptation:
    """Tests for ESRIJSONImportSource.adapt_source_for_ogr() URL handling."""

    @pytest.mark.parametrize(
        "source,expected_adapted_url",
        [
            (
                "https://example.com/arcgis/rest/services/MyService/MapServer/0",
                "https://example.com/arcgis/rest/services/MyService/MapServer/0/query",
            ),
            (
                "esri:https://example.com/arcgis/rest/services/MyService/MapServer/0/",
                "https://example.com/arcgis/rest/services/MyService/MapServer/0/query",
            ),
            (
                "https://example.com/arcgis/rest/services/MyService/FEATURESERVER/0",
                "https://example.com/arcgis/rest/services/MyService/FEATURESERVER/0/query",
            ),
            (
                "esri:https://example.com/arcgis/rest/services/MyService/MapServer/0/query",
                "https://example.com/arcgis/rest/services/MyService/MapServer/0/query",
            ),
            (
                "https://example.com/arcgis/rest/services/MyService/MapServer/5",
                "https://example.com/arcgis/rest/services/MyService/MapServer/5/query",
            ),
            (
                "esri:https://example.com/arcgis/rest/services/MyService/MapServer/0",
                "https://example.com/arcgis/rest/services/MyService/MapServer/0/query",
            ),
            (
                "https://gis.example.com/server/rest/services/Folder1/Folder2/MyService/MapServer/12",
                "https://gis.example.com/server/rest/services/Folder1/Folder2/MyService/MapServer/12/query",
            ),
            (
                "esri:https://example.com:6443/arcgis/rest/services/MyService/MapServer/0",
                "https://example.com:6443/arcgis/rest/services/MyService/MapServer/0/query",
            ),
        ],
    )
    def test_mapserver_url_basic(self, source, expected_adapted_url):
        """Test rest URL adaptation."""
        adapted_url, drivers = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert drivers == ["ESRIJSON"]
        assert expected_adapted_url in adapted_url
        assert "where=1%3D1" in adapted_url  # where=1=1
        assert "f=json" in adapted_url
        assert "outFields=%2A" in adapted_url  # outFields=*
        assert "returnGeometry=true" in adapted_url


class TestESRIJSONImportSourceQueryParameters:
    """Tests for query parameter handling."""

    def test_default_parameters_added(self):
        """Test that default parameters are added when not present."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "where=1%3D1" in adapted_url
        assert "f=json" in adapted_url
        assert "outFields=%2A" in adapted_url
        assert "returnGeometry=true" in adapted_url

    def test_existing_where_preserved(self):
        """Test that existing where parameter is preserved."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0?where=STATE='CA'"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert (
            "where=STATE%3D%27CA%27" in adapted_url or "where=STATE='CA'" in adapted_url
        )
        # Should not have the default where=1=1
        assert adapted_url.count("where=") == 1

    def test_existing_format_overridden(self):
        """Test that existing f parameter is overridden."""
        source = (
            "https://example.com/arcgis/rest/services/MyService/MapServer/0?f=geojson"
        )
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "f=json" in adapted_url
        assert adapted_url.count("f=") == 1

    def test_existing_outfields_preserved(self):
        """Test that existing outFields parameter is preserved."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0?outFields=NAME,POPULATION"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "outFields=NAME" in adapted_url
        assert "POPULATION" in adapted_url

    def test_existing_returngeometry_preserved(self):
        """Test that existing returnGeometry parameter is preserved."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0?returnGeometry=false"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "returnGeometry=false" in adapted_url

    def test_case_insensitive_parameter_check(self):
        """Test that parameter checking is case-insensitive."""
        # Using uppercase parameter names
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0?WHERE=1=1&F=geojson&OUTFIELDS=*&RETURNGEOMETRY=true"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        # Should preserve the existing parameters and not add duplicates
        # The exact casing may vary but should not have duplicates
        param_count = adapted_url.lower().count("where=")
        assert param_count == 1

    def test_mixed_existing_and_default_parameters(self):
        """Test mixing existing and default parameters."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0?where=ID>100"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        # Should have the user's where clause
        assert "where=" in adapted_url
        # Should add missing defaults
        assert "f=json" in adapted_url
        assert "outFields=%2A" in adapted_url or "outFields=*" in adapted_url
        assert "returnGeometry=true" in adapted_url

    def test_additional_parameters_preserved(self):
        """Test that additional user parameters are preserved."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0?resultRecordCount=1000&orderByFields=NAME"
        adapted_url, _ = ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "resultRecordCount=1000" in adapted_url
        assert "orderByFields=NAME" in adapted_url
        # Should still add defaults
        assert "where=1%3D1" in adapted_url


class TestESRIJSONImportSourceErrors:
    """Tests for error handling."""

    def test_missing_layer_id(self):
        """Test error when layer ID is missing."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer"
        with pytest.raises(ImportSourceError) as exc_info:
            ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "Invalid ESRI Rest service URL" in str(exc_info.value)
        assert "MapServer/LAYER_ID" in str(exc_info.value)

    def test_missing_service_type(self):
        """Test error when neither MapServer nor FeatureServer is present."""
        source = "https://example.com/arcgis/rest/services/MyService/0"
        with pytest.raises(ImportSourceError) as exc_info:
            ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "Invalid ESRI Rest service URL" in str(exc_info.value)

    def test_invalid_path_suffix(self):
        """Test error when URL has invalid path suffix."""
        source = "https://example.com/arcgis/rest/services/MyService/MapServer/0/invalidendpoint"
        with pytest.raises(ImportSourceError) as exc_info:
            ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "Invalid ESRI Rest service URL" in str(exc_info.value)

    def test_non_numeric_layer_id(self):
        """Test error when layer ID is not numeric."""
        source = (
            "https://example.com/arcgis/rest/services/MyService/MapServer/notanumber"
        )
        with pytest.raises(ImportSourceError) as exc_info:
            ESRIJSONImportSource.adapt_source_for_ogr(source)

        assert "Invalid ESRI Rest service URL" in str(exc_info.value)


@pytest.fixture
def mock_esri_server(tmp_path):
    """
    Fixture that creates a simple HTTP server serving ESRI JSON test data.
    Returns the server URL.
    """
    # Create a directory to serve from
    serve_dir = tmp_path / "esri_data"
    serve_dir.mkdir()

    # Copy test data to serve directory
    test_data_path = Path(__file__).parent / "data" / "test-point.esrijson"
    if test_data_path.exists():
        # Create the service structure
        service_path = (
            serve_dir
            / "arcgis"
            / "rest"
            / "services"
            / "TestService"
            / "MapServer"
            / "0"
        )
        service_path.mkdir(parents=True)

        # Copy the test data as the query endpoint
        import shutil

        query_file = service_path / "query"
        shutil.copy(test_data_path, query_file)

        # Create a simple HTTP server
        class Handler(SimpleHTTPRequestHandler):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=str(serve_dir), **kwargs)

            def log_message(self, format, *args):
                pass  # Suppress logging

        server = HTTPServer(("127.0.0.1", 0), Handler)
        port = server.server_address[1]
        url = f"http://127.0.0.1:{port}"

        # Start server in background thread
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()

        yield url

        # Cleanup
        server.shutdown()
    else:
        # If test data doesn't exist, skip the fixture
        pytest.skip("Test data file not found")


class TestESRIJSONImportSourceIntegration:
    """Integration tests that import actual ESRI JSON data."""

    @pytest.mark.slow
    def test_import_from_mock_esri_server(self, mock_esri_server, tmp_path, cli_runner):
        """Test importing from a mocked ESRI REST service."""
        # Initialize a new repo
        repo_path = tmp_path / "repo"
        r = cli_runner.invoke(["init", str(repo_path)])
        assert r.exit_code == 0, r.stderr

        # Import from the mock ESRI server
        esri_url = (
            f"esri:{mock_esri_server}/arcgis/rest/services/TestService/MapServer/0"
        )
        r = cli_runner.invoke(
            ["-C", str(repo_path), "import", esri_url, "--dataset", "test_points"]
        )
        assert r.exit_code == 0, r.stderr

        # Verify the import
        repo = KartRepo(repo_path)
        dataset = repo.datasets()["test_points"]

        # Verify expected fields from the test-point.esrijson
        field_names = {col["name"] for col in dataset.schema}
        assert "FEATURE_TYPE" in field_names
        assert "ID" in field_names
        assert "SOURCE" in field_names

        # Verify feature count
        feature_count = sum(1 for _ in dataset.features())
        assert feature_count == 1

        # Verify the feature data
        feature = next(dataset.features())
        assert feature["FEATURE_TYPE"] == "Test Feature"
        assert feature["ID"] == "Test ID"


class TestESRIRestServerSource:
    """Tests for ESRIRestServerSource layer discovery."""

    def test_server_url_validation(self):
        """Test that ESRIRestServerSource validates server URLs correctly."""
        # Valid server URLs
        source = ESRIRestServerSource.open(
            "esri:https://example.com/arcgis/rest/services/MyService/MapServer"
        )
        assert (
            source.base_url
            == "https://example.com/arcgis/rest/services/MyService/MapServer"
        )

        source = ESRIRestServerSource.open(
            "https://example.com/arcgis/rest/services/MyService/FeatureServer"
        )
        assert (
            source.base_url
            == "https://example.com/arcgis/rest/services/MyService/FeatureServer"
        )

    def test_server_url_with_layer_id_rejected(self):
        """Test that URLs with layer IDs are rejected (should use ESRIJSONImportSource)."""
        with pytest.raises(ImportSourceError) as exc_info:
            ESRIRestServerSource.open(
                "https://example.com/arcgis/rest/services/MyService/MapServer/0"
            )
        assert "ESRIJSONImportSource" in str(exc_info.value)

    def test_invalid_server_url(self):
        """Test that invalid server URLs are rejected."""
        with pytest.raises(ImportSourceError):
            ESRIRestServerSource.open("https://example.com/not-a-service")

    def test_get_tables_with_mock_service(self):
        """Test layer discovery from a mocked ESRI Rest service."""
        # Create a mock service metadata JSON
        service_info = {
            "currentVersion": 10.91,
            "serviceDescription": "Test Service",
            "layers": [
                {"id": 0, "name": "Layer0", "type": "Feature Layer"},
                {"id": 1, "name": "Layer1", "type": "Feature Layer"},
                {"id": 2, "name": "Layer2", "type": "Feature Layer"},
            ],
            "tables": [
                {"id": 3, "name": "Table3", "type": "Table"},
            ],
        }

        import unittest.mock as mock

        with mock.patch("kart.tabular.esri_rest_import_source.urlopen") as mock_urlopen:
            # Mock the response
            mock_response = mock.MagicMock()
            mock_response.read.return_value = json.dumps(service_info).encode("utf-8")
            mock_response.__enter__.return_value = mock_response
            mock_response.__exit__.return_value = False
            mock_urlopen.return_value = mock_response

            # Create the source
            source = ESRIRestServerSource.open(
                "https://example.com/arcgis/rest/services/TestService/MapServer"
            )

            # Get tables - this should return the layer metadata
            tables = source.get_tables()

            # Verify the layers were discovered correctly
            assert len(tables) == 4
            assert "Layer0" in tables
            assert "Layer1" in tables
            assert "Layer2" in tables
            assert "Table3" in tables

            # Verify layer structure
            assert tables["Layer0"]["id"] == 0
            assert tables["Layer0"]["name"] == "Layer0"
            assert tables["Layer0"]["type"] == "layer"

            assert tables["Table3"]["id"] == 3
            assert tables["Table3"]["name"] == "Table3"
            assert tables["Table3"]["type"] == "table"

    def test_default_dest_path(self):
        """Test default destination path generation."""
        source = ESRIRestServerSource(
            "https://example.com/arcgis/rest/services/MyService/MapServer"
        )
        assert source.default_dest_path() == "MyService"

        source = ESRIRestServerSource(
            "https://example.com/arcgis/rest/services/MyService/FeatureServer",
            table="Layer0",
        )
        assert source.default_dest_path() == "MyService/Layer0"

    def test_clone_for_table(self):
        """Test clone_for_table method with invalid layer name."""
        import unittest.mock as mock

        # Create a mock service metadata
        service_info = {
            "layers": [
                {"id": 0, "name": "Layer0", "type": "Feature Layer"},
                {"id": 1, "name": "Layer1", "type": "Feature Layer"},
            ],
        }

        with mock.patch("kart.tabular.esri_rest_import_source.urlopen") as mock_urlopen:
            # Mock the response
            mock_response = mock.MagicMock()
            mock_response.read.return_value = json.dumps(service_info).encode("utf-8")
            mock_response.__enter__.return_value = mock_response
            mock_response.__exit__.return_value = False
            mock_urlopen.return_value = mock_response

            source = ESRIRestServerSource.open(
                "https://example.com/arcgis/rest/services/MyService/MapServer"
            )

            # Test cloning for an invalid table - this should fail before trying to open OGR
            with pytest.raises(ImportSourceError) as exc_info:
                source.clone_for_table("NonExistentLayer")
            assert "Layer 'NonExistentLayer' not found" in str(exc_info.value)
            assert "Layer0, Layer1" in str(exc_info.value)


class TestESRIRestImportSourceRouter:
    """Tests for ESRIRestImportSource routing logic."""

    def test_routes_to_server_source(self):
        """Test that server URLs route to ESRIRestServerSource."""
        source = ESRIRestImportSource.open(
            "esri:https://example.com/arcgis/rest/services/MyService/MapServer"
        )
        assert isinstance(source, ESRIRestServerSource)

        source = ESRIRestImportSource.open(
            "https://example.com/arcgis/rest/services/MyService/FeatureServer"
        )
        assert isinstance(source, ESRIRestServerSource)

    def test_routes_to_layer_source(self):
        """Test that layer URLs route to ESRIJSONImportSource."""
        # Note: This will fail at OGR open time, but we can verify the routing
        with pytest.raises(ImportSourceError) as exc_info:
            source = ESRIRestImportSource.open(
                "esri:https://example.com/arcgis/rest/services/MyService/MapServer/0"
            )
        # Implied here: the layer URL was routed to ESRIJSONImportSource, which then failed to open it (since it's not a real service),
        # and the error message should include the adapted URL:
        assert (
            "https://example.com/arcgis/rest/services/MyService/MapServer/0/query?f=json&where=1%3D1&outFields=%2A&returnGeometry=true"
            in str(exc_info.value)
        )

    def test_invalid_url_raises_error(self):
        """Test that invalid URLs raise an error."""
        with pytest.raises(ImportSourceError) as exc_info:
            ESRIRestImportSource.open("https://example.com/not-a-service")
        assert "Invalid ESRI Rest service URL" in str(exc_info.value)
