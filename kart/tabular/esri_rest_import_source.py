"""
Import sources for Esri ArcGIS REST services (MapServer/FeatureServer).

GDAL's ESRIJSON driver fetches the data; these classes build the URLs it needs.
TableImportSource.open() routes here for "esri:" specs, and for http(s) URLs whose path
ends in /MapServer or /FeatureServer[/LAYER_ID] (see kart.import_sources.from_spec).
User-facing documentation is in docs/pages/command_reference.rst.

ESRIRestImportSource
    Picks one of the classes below, depending on whether the URL has a layer ID.

ESRIRestServerSource (.../MapServer)
    Lists the service's layers and tables from ".../MapServer?f=json".
    clone_for_table() returns an ESRIJSONImportSource for the chosen layer.

ESRIJSONImportSource (.../MapServer/LAYER_ID)
    Reads one layer. Rewrites the URL to ".../LAYER_ID/query", and adds where /
    outFields / returnGeometry if the user hasn't set them. The OGR layer is always
    called "ESRIJSON", so the Kart table name comes from the layer metadata
    (".../LAYER_ID?f=json") instead, falling back to "ESRIJSON" if that can't be read.
"""

import functools
import json
import re
import sys
from urllib.parse import parse_qs, parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import urlopen

from .import_source import TableImportSource
from .ogr_import_source import OgrTableImportSource
from kart.exceptions import ImportSourceError
from kart.sqlalchemy import strip_username_and_password

# Query params that are shown in commit messages and progress output - they say which subset
# of the layer was imported. All other params (eg token) may be credentials, so they are left out.
DISPLAYED_QUERY_PARAMS = ("where", "outFields", "returnGeometry")


def display_url(url, *, service_only=False):
    """
    Returns the given Esri URL in a form safe to print or put in a commit message:
    no "esri:" prefix, no username / password, no "/query" suffix, and only the
    DISPLAYED_QUERY_PARAMS. If service_only is set, the layer ID and query are removed too.
    """
    url = strip_username_and_password(str(url).removeprefix("esri:"))
    parsed = urlparse(url)
    path = re.sub(r"/query/?$", "", parsed.path, flags=re.I)
    if service_only:
        path = re.sub(r"/\d+/?$", "", path)
        query = ""
    else:
        displayed = {p.lower(): p for p in DISPLAYED_QUERY_PARAMS}
        query = urlencode(
            [
                (displayed[k.lower()], v)
                for k, v in parse_qsl(parsed.query, keep_blank_values=True)
                if k.lower() in displayed
            ]
        )
    return urlunparse(parsed._replace(path=path, query=query, fragment=""))


def _json_format_query_params(query):
    """
    Parses the given URL query string, and forces the response format to JSON.
    ArcGIS parameter names are case-insensitive, so any existing "f" / "F" params are removed.
    """
    query_params = {}
    for k, v in parse_qs(query, keep_blank_values=True).items():
        if k.lower() == "f":
            # Replaced in place, so the param order is otherwise unchanged.
            query_params["f"] = ["json"]
        else:
            query_params[k] = v
    query_params["f"] = ["json"]
    return query_params


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
        spec = str(source).removeprefix("esri:")
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
        spec = str(source).removeprefix("esri:")
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

        # Build the base service URL (without layer ID or query)
        base_url = urlunparse(
            (parsed.scheme, parsed.netloc, base_path, parsed.params, "", "")
        )

        return cls(base_url, table=table, query=parsed.query)

    def __init__(self, base_url, table=None, query=""):
        """
        Initialize the ESRIRestServerSource.

        Args:
            base_url: Base URL of the MapServer/FeatureServer (without layer ID or query)
            table: Optional specific layer name/ID to import
            query: Query string from the user's URL (eg token=...) - sent with every request.
        """
        self.base_url = base_url
        self.table = table
        self.query = query

    def _url_with_query(self, path_suffix="", query=None):
        """Returns base_url + path_suffix, with the given query (default: the user's query)."""
        parsed = urlparse(self.base_url)
        return urlunparse(
            parsed._replace(
                path=parsed.path + path_suffix,
                query=self.query if query is None else query,
            )
        )

    @functools.lru_cache(maxsize=1)
    def _get_layer_metadata(self):
        """
        Fetch and cache layer metadata from the ESRI Rest service.

        Returns:
            dict: Service metadata from the ?f=json endpoint
        """
        metadata_url = self._url_with_query(
            query=urlencode(_json_format_query_params(self.query), doseq=True)
        )

        try:
            with urlopen(metadata_url) as response:
                return json.loads(response.read().decode("utf-8"))
        except Exception as e:
            raise ImportSourceError(
                f"Failed to fetch service metadata from {display_url(metadata_url, service_only=True)}: {e}"
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

        layer_info = tables[table]
        layer_url = self._url_with_query(f"/{layer_info['id']}")
        layer_source = ESRIJSONImportSource.open(layer_url, table=layer_info["name"])
        return layer_source.clone_for_table(
            layer_source.table,
            dest_path=dest_path,
            primary_key=primary_key,
            meta_overrides=meta_overrides or {},
        )

    def __str__(self):
        url = display_url(self.base_url)
        if self.table:
            return f"ESRIRestServerSource({url}, table={self.table})"
        return f"ESRIRestServerSource({url})"

    def aggregate_import_source_desc(self, import_sources):
        """Return a description of all layers being imported from this service."""
        if len(import_sources) == 1:
            source = next(iter(import_sources))
            return source.import_source_desc()

        desc = f"Import {len(import_sources)} layers from {display_url(self.base_url)}:"
        for source in import_sources:
            desc += f"\n * {source.dest_path}/"
        return desc


class ESRIJSONImportSource(OgrTableImportSource):
    """
    Import source for ESRI Rest services (MapServer/FeatureServer).

    Handles:
    - Automatic construction of query URLs with necessary parameters
    - Layer-specific imports from MapServer/FeatureServer endpoints

    The OGR datasource always has exactly one layer, named after the driver ("ESRIJSON").
    Kart-facing, that layer is named after the Esri layer's name from the layer metadata
    endpoint (eg ".../MapServer/0?f=json"), which gives a sensible default dataset path.
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
        if ogr_ds.GetLayerCount() != 1:
            raise ImportSourceError(
                f"Expected exactly one layer in ESRI JSON datasource: {source}"
            )
        if table is None:
            table = self.fetch_esri_layer_name(ogr_ds, ogr_source)

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
    def layer_metadata_url(cls, ogr_source):
        """
        Given the adapted ".../MapServer/N/query?..." URL, returns ".../MapServer/N?f=json".
        Other query params (eg token) are kept, since some services need them for metadata too.
        """
        parsed = urlparse(str(ogr_source))
        query_params = _json_format_query_params(parsed.query)
        return urlunparse(
            parsed._replace(
                path=re.sub(r"/query$", "", parsed.path),
                query=urlencode(query_params, doseq=True),
            )
        )

    @classmethod
    def fetch_esri_layer_name(cls, ogr_ds, ogr_source):
        """
        Returns the layer's name according to the Esri layer metadata.
        Falls back to the OGR layer name ("ESRIJSON") if the metadata isn't available.
        """
        try:
            with urlopen(cls.layer_metadata_url(ogr_source)) as response:
                name = json.loads(response.read().decode("utf-8")).get("name")
        except Exception:
            name = None
        if isinstance(name, str) and name.strip("/"):
            return name
        return ogr_ds.GetLayerByIndex(0).GetName()

    @property
    def source_name(self):
        # The full URL (not just the last path part), minus anything that might be a credential.
        return display_url(self.source)

    def __str__(self):
        return f"{self.source_name}:{self.table}"

    def aggregate_import_source_desc(self, import_sources):
        if len(import_sources) == 1:
            return next(iter(import_sources)).import_source_desc()
        # Several layers from one service - name the service, not this layer.
        desc = f"Import {len(import_sources)} datasets from {display_url(self.source, service_only=True)}:"
        for source in import_sources:
            if source.dest_path == source.table:
                desc += f"\n * {source.table}/"
            else:
                desc += f"\n * {source.dest_path} (from {source.table})"
        return desc

    def get_tables(self):
        # The single OGR layer, keyed by the Esri layer name.
        return {self.table: self.ds.GetLayerByIndex(0)}

    @property
    @functools.lru_cache(maxsize=1)
    def ogrlayer(self):
        return self.ds.GetLayerByIndex(0)

    @classmethod
    def adapt_source_for_ogr(cls, source):
        source = str(source).removeprefix("esri:")

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

        # always override format:
        query_params = _json_format_query_params(parsed.query)

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
