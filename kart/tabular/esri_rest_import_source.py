"""
Import source for ESRI Rest services (MapServer/FeatureServer).

ESRI Rest services can be imported using the "ESRIJSON" OGR driver which does the
heavy lifting of fetching data from the service and converting it to a format we can
import. This module provides ESRIRestImportSource which routes to either of the following
depending on supplied source:

ESRIRestServerSource:
    Discovers and lists all available layers from a MapServer/FeatureServer endpoint.
    Use this when you want to import multiple layers from a service or need to see
    what layers are available.

    Example:
        # List all layers in a service
        source = ESRIRestServerSource.open("https://example.com/.../MapServer")
        tables = source.get_tables()

        # Import all layers from a service
        kart import "esri:https://example.com/.../MapServer"

ESRIJSONImportSource:
    Imports a specific layer from a MapServer/FeatureServer by layer ID.
    Automatically constructs proper query URLs with necessary parameters.

    Example:
        # Import a specific layer
        kart import "esri:https://example.com/.../MapServer/0" --dataset my_layer

"""

import functools
import json
import re
import sys
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse
from urllib.request import urlopen

from .import_source import TableImportSource
from .ogr_import_source import OgrTableImportSource
from kart.exceptions import ImportSourceError


class ESRIRestImportSource(TableImportSource):
    """
    Router class for ESRI Rest service imports.

    This class examines the URL and routes to either:
    - ESRIRestServerSource: for server-level URLs (without layer ID)
    - ESRIJSONImportSource: for layer-specific URLs (with layer ID)
    """

    @classmethod
    def open(cls, source, table=None):
        """
        Route to the appropriate ESRI import source based on URL structure.

        Server URLs (no layer ID) -> ESRIRestServerSource
        Layer URLs (with layer ID) -> ESRIJSONImportSource
        """
        spec = str(source).lstrip("esri:")
        parsed = urlparse(spec)

        # Check if this is a server-level URL (MapServer/FeatureServer without layer ID)
        # or a layer-specific URL (with layer ID)
        if re.search(r"/(MapServer|FeatureServer)/\d+", parsed.path, re.I):
            # Layer-specific URL - use ESRIJSONImportSource
            return ESRIJSONImportSource.open(source, table=table)
        elif re.search(r"/(MapServer|FeatureServer)/?$", parsed.path, re.I):
            # Server-level URL - use ESRIRestServerSource
            return ESRIRestServerSource.open(source, table=table)
        else:
            raise ImportSourceError(
                "Invalid ESRI Rest service URL. Expected format:\n"
                "  Server: esri:https://HOST/PATH/MapServer\n"
                "  Layer:  esri:https://HOST/PATH/MapServer/LAYER_ID"
            )


class ESRIRestServerSource(TableImportSource):
    """
    Import source for discovering and importing all layers from an ESRI Rest service.

    This class handles MapServer/FeatureServer endpoints and discovers available layers,
    then delegates the actual import of each layer to ESRIJSONImportSource.
    """

    @classmethod
    def open(cls, source, table=None):
        """Open an ESRI Rest service and optionally select a specific layer."""
        spec = str(source).lstrip("esri:")
        parsed = urlparse(spec)

        # Validate the URL points to a MapServer or FeatureServer
        if service_match := re.search(
            r"(?P<base>.*/(MapServer|FeatureServer))(?P<remainder>/.*)?$",
            parsed.path,
            re.I,
        ):
            base_path = service_match.group("base")
            remainder = service_match.group("remainder")

            # If it already has a layer ID, this should be handled by ESRIJSONImportSource
            if remainder and re.match(r"^/\d+(/.*)?$", remainder):
                raise ImportSourceError(
                    "Use ESRIJSONImportSource for specific layer imports. "
                    "ESRIRestServerSource is for server-level discovery."
                )
        else:
            raise ImportSourceError(
                "Invalid ESRI Rest service URL. Expected format: "
                "esri:https://HOST/PATH/MapServer or esri:https://HOST/PATH/FeatureServer"
            )

        # Build the base service URL (without layer ID)
        base_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                base_path,
                parsed.params,
                "",
                parsed.fragment,
            )
        )

        return cls(base_url, table=table)

    def __init__(self, base_url, table=None):
        """
        Initialize the ESRIRestServerSource.

        Args:
            base_url: Base URL of the MapServer/FeatureServer (without layer ID)
            table: Optional specific layer name/ID to import
        """
        self.base_url = base_url
        self.table = table

    @functools.lru_cache(maxsize=1)
    def _get_layer_metadata(self):
        """
        Fetch and cache layer metadata from the ESRI Rest service.

        Returns:
            dict: Service metadata from the ?f=json endpoint
        """
        metadata_url = f"{self.base_url}?f=json"

        try:
            with urlopen(metadata_url) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as e:
            raise ImportSourceError(
                f"Failed to fetch service metadata from {metadata_url}: {e}"
            )

    @functools.lru_cache(maxsize=1)
    def get_tables(self):
        """
        Discover available layers from the ESRI Rest service.

        Returns:
            dict: Dictionary mapping layer names to layer info dicts
        """
        service_info = self._get_layer_metadata()

        # Extract layers from the service info
        layers = {}
        for layer_info in service_info.get("layers", []):
            layer_id = layer_info.get("id")
            layer_name = layer_info.get("name")

            if layer_id is not None and layer_name:
                layers[layer_name] = {
                    "id": layer_id,
                    "name": layer_name,
                    "type": "layer",
                }

        # Also check for 'tables' in FeatureServer responses
        for table_info in service_info.get("tables", []):
            table_id = table_info.get("id")
            table_name = table_info.get("name")

            if table_id is not None and table_name:
                layers[table_name] = {
                    "id": table_id,
                    "name": table_name,
                    "type": "table",
                }

        if not layers:
            raise ImportSourceError(
                f"No layers found in ESRI Rest service: {self.base_url}"
            )

        # If a specific table was requested, filter to just that one
        if self.table is not None:
            if self.table in layers:
                return {self.table: layers[self.table]}
            else:
                # Try to match by ID as well
                try:
                    layer_id = int(self.table)
                    # Create a pseudo-entry for this layer ID
                    return {
                        self.table: {
                            "id": layer_id,
                            "name": self.table,
                            "type": "layer",
                        }
                    }
                except ValueError:
                    raise ImportSourceError(
                        f"Layer '{self.table}' not found in service. "
                        f"Available layers: {', '.join(layers.keys())}"
                    )

        return layers

    def print_table_list(self, *, do_json=False):
        """Print a list of available layers in the ESRI Rest service."""
        from kart.output_util import dump_json_output

        tables = self.get_tables()

        if do_json:
            dump_json_output({"kart.tables/v1": list(tables.keys())}, sys.stdout)
        else:
            for name in tables:
                print(name)

    def default_dest_path(self):
        """Default destination path based on the service name."""
        # Extract service name from URL
        match = re.search(r"/([^/]+)/(MapServer|FeatureServer)", self.base_url, re.I)
        if match:
            service_name = match.group(1)
            if self.table:
                return self._normalise_dataset_path(f"{service_name}/{self.table}")
            return self._normalise_dataset_path(service_name)
        return self._normalise_dataset_path("esri_service")

    def clone_for_table(
        self, table, *, dest_path=None, primary_key=None, meta_overrides=None
    ):
        """
        Create an import source for a specific table/layer.

        Returns an ESRIJSONImportSource configured for the specified layer.
        """
        tables = self.get_tables()

        if table not in tables:
            raise ImportSourceError(
                f"Layer '{table}' not found in service. "
                f"Available layers: {', '.join(tables.keys())}"
            )

        # Get the layer metadata
        layer_info = tables[table]
        layer_id = layer_info["id"]
        layer_name = layer_info["name"]

        # Construct the URL for this layer
        layer_url = f"{self.base_url}/{layer_id}"

        # Create and open an ESRIJSONImportSource for this layer
        layer_source = ESRIJSONImportSource.open(layer_url, table=layer_name)

        # If dest_path or other overrides are provided, clone with those settings
        if dest_path or primary_key or meta_overrides:
            if hasattr(layer_source, "clone_for_table"):
                return layer_source.clone_for_table(
                    layer_source.table,
                    dest_path=dest_path,
                    primary_key=primary_key,
                    meta_overrides=meta_overrides or {},
                )
            else:
                # If it doesn't have clone_for_table, just set dest_path
                if dest_path:
                    layer_source.dest_path = dest_path
                return layer_source

        return layer_source

    def __str__(self):
        if self.table:
            return f"ESRIRestServerSource({self.base_url}, table={self.table})"
        return f"ESRIRestServerSource({self.base_url})"

    def aggregate_import_source_desc(self, import_sources):
        """Return a description of all layers being imported from this service."""
        if len(import_sources) == 1:
            source = next(iter(import_sources))
            return source.import_source_desc()

        desc = f"Import {len(import_sources)} layers from {self.base_url}:"
        for source in import_sources:
            desc += f"\n * {source.dest_path}/"
        return desc


class ESRIJSONImportSource(OgrTableImportSource):
    """
    Import source for ESRI Rest services (MapServer/FeatureServer).

    Handles:
    - Automatic construction of query URLs with necessary parameters
    - Layer-specific imports from MapServer/FeatureServer endpoints
    """

    def __init__(
        self,
        ogr_ds,
        table=None,
        *,
        source,
        ogr_source,
        dest_path=None,
        primary_key=None,
        meta_overrides=None,
    ):
        # For ESRI JSON sources, the datasource typically has only one layer
        # If no table is specified or the specified table doesn't exist,
        # use the first (and usually only) layer in the datasource
        if table is None or ogr_ds.GetLayerByName(table) is None:
            if ogr_ds.GetLayerCount() > 0:
                actual_layer = ogr_ds.GetLayerByIndex(0)
                actual_table_name = actual_layer.GetName()
                # If a table name was requested but doesn't match, still use it for dest_path
                # but use the actual layer name for accessing the data
                if table and table != actual_table_name:
                    # Store the requested name for dest_path purposes
                    if not dest_path:
                        dest_path = table
                    table = actual_table_name
                elif not table:
                    table = actual_table_name
            elif table is None:
                raise ImportSourceError(
                    f"No layers found in ESRI JSON datasource: {source}"
                )

        super().__init__(
            ogr_ds,
            table,
            source=source,
            ogr_source=ogr_source,
            dest_path=dest_path,
            primary_key=primary_key,
            meta_overrides=meta_overrides,
        )

    @classmethod
    def adapt_source_for_ogr(cls, source):
        source = str(source).lstrip("esri:")

        parsed = urlparse(source)

        # Validate the URL and ensure the /query endpoint for the service is used:
        if layer_match := re.search(
            r"(?P<base>.*/(MapServer|FeatureServer)/)(?P<layer_id>\d+)(?P<remainder>/[^?]*)?$",
            parsed.path,
            re.I,
        ):
            # Raise errors on unexpected path suffixes
            if layer_match.group("remainder") not in [None, "/", "/query"]:
                raise ImportSourceError(
                    "Invalid ESRI Rest service URL. Expected format: "
                    "esri:https://HOST/PATH/MapServer/LAYER_ID or esri:https://HOST/PATH/FeatureServer/LAYER_ID"
                )
            adapted_path = (
                f"{layer_match.group('base')}{layer_match.group('layer_id')}/query"
            )
        else:
            raise ImportSourceError(
                "Invalid ESRI Rest service URL. Expected format: "
                "esri:https://HOST/PATH/MapServer/LAYER_ID or esri:https://HOST/PATH/FeatureServer/LAYER_ID"
            )

        parsed = urlparse(source)
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        # always override format:
        query_params["f"] = ["json"]

        # Create a lowercase key lookup for case-insensitive checking
        query_params_lower = {k.lower(): k for k in query_params.keys()}

        # Add default parameters if not already present (case-insensitive)
        if "where" not in query_params_lower:
            query_params["where"] = ["1=1"]
        if "outfields" not in query_params_lower:
            query_params["outFields"] = ["*"]
        if "returngeometry" not in query_params_lower:
            query_params["returnGeometry"] = ["true"]

        # Rebuild query string
        new_query = urlencode(query_params, doseq=True)
        adapted_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                adapted_path,
                parsed.params,
                new_query,
                parsed.fragment,
            )
        )
        return adapted_url, ["ESRIJSON"]
