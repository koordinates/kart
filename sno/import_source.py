import functools

from .meta_items import META_ITEM_NAMES
from .schema import Schema


class ImportSource:
    """
    A dataset-like interface that can be imported as a dataset.
    A read-only interface.
    """

    @property
    def dest_path(self):
        """
        The destination path where this dataset should be imported.
        Defaults to self.path if nothing else is set, which works for dataset-like objects that already have a path.
        """
        if hasattr(self, "_dest_path"):
            return self._dest_path
        elif hasattr(self, "path"):
            return self.path
        else:
            raise ValueError(f"No dest_path is set for {self}")

    @dest_path.setter
    def dest_path(self, dest_path):
        self._dest_path = dest_path

    def get_meta_item(self, name):
        """Find or generate a V2 meta item."""
        # If self.get_gpkg_meta_item works already, this method can be implemented as follows:
        # >>> return gpkg_adapter.generate_v2_meta_item(self, name)
        raise NotImplementedError()

    def get_gpkg_meta_item(self, name):
        """Find or generate a gpkg / V1 meta item."""
        # If self.get_meta_item works already, this method can be implemented as follows:
        # >>> return gpkg_adapter.generate_gpkg_meta_item(self, name)

        raise NotImplementedError()

    def iter_meta_items(self):
        """Iterates over all the meta items that need to be imported."""
        for name in META_ITEM_NAMES:
            meta_item = self.get_meta_item(name)
            if meta_item is not None:
                yield name, meta_item

        for identifier, definition in self.iter_crs_definitions():
            yield f"crs/{identifier}.wkt", definition

    def iter_crs_definitions(self):
        """
        Yields a (identifier, definition) tuple for every CRS definition.
        The identifier should be a string that uniquely identifies the CRS eg "EPSG:4326"
        The definition should be a string containing a WKT definition eg 'GEOGCS["WGS 84"...'
        """
        raise NotImplementedError()

    def get_crs_definition(self, identifier=None):
        """
        Returns the CRS definition with the given identifer,
        or the only CRS definition if no identifer is supplied.
        """
        # Subclasses may overrdie this to make it more efficient.
        all_crs_definitions = dict(self.iter_crs_definitions())
        if identifier is not None:
            return all_crs_definitions[identifier]
        num_defs = len(all_crs_definitions)
        if num_defs == 1:
            return next(iter(all_crs_definitions.values()))
        raise ValueError(
            f"get_crs_definition() only works when there is exactly 1 CRS definition, but there is {num_defs}"
        )

    @property
    @functools.lru_cache(maxsize=1)
    def schema(self):
        """Convenience method for loading the schema.json into a Schema object"""
        return Schema.from_column_dicts(self.get_meta_item("schema.json"))

    def features(self):
        """
        Yields a dict for every feature. Dicts contain key-value pairs for each feature property,
        and geometries use sno.geometry.Geometry objects, as in the following example:
        {
            "fid": 123,
            "geom": Geometry(b"..."),
            "name": "..."
            "last-modified": "..."
        }
        """
        raise NotImplementedError()

    @property
    def feature_count(self):
        """Returns the number of features in self.features"""
        # Subclasses should generally override this to make it more efficient:
        count = 0
        for f in self.features():
            count += 1
        return count

    def __enter__(self):
        """Some import sources have resources that need to be opened and closed."""
        pass

    def __exit__(self, *args):
        """Some import sources have resources that need to be opened and closed."""
        pass

    def __str__(self):
        return f"{self.__class__.__name__}"

    def import_source_desc(self):
        """Return a description of this ImportSource."""
        # Subclasses should override if str() does not return the right information.
        return str(self)

    def aggregate_import_source_desc(self, import_sources):
        """
        Return a description of this collection of import_sources. For example:

        Import 3 datasets from example.gpkg:
        first_table
        second_dataset (from second_table)
        third_table
        """
        # Subclasses should override this if a more useful aggregate description can be generated.
        return "\n".join(s.import_source_desc() for s in import_sources)
