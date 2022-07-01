import click

from kart.exceptions import NO_TABLE, NotFound
from kart import list_of_conflicts
from kart.schema import Schema
from kart.output_util import InputMode, get_input_mode


class TableImportSource:
    """
    A dataset-like interface that can be imported as a dataset.
    A read-only interface.
    """

    UNNECESSARY_PREFIXES = ("OGR:", "GPKG:", "PG:")

    @classmethod
    def _remove_unnecessary_prefix(cls, spec):
        spec_upper = spec.upper()
        for p in cls.UNNECESSARY_PREFIXES:
            if spec_upper.startswith(p):
                return spec[len(p) :]

        return spec

    @classmethod
    def open(cls, full_spec, table=None):
        from kart.sqlalchemy import DbType

        spec = cls._remove_unnecessary_prefix(str(full_spec))

        db_type = DbType.from_spec(spec)
        if db_type is not None:
            from .sqlalchemy_import_source import SqlAlchemyTableImportSource

            return SqlAlchemyTableImportSource.open(spec, table=table)
        else:
            from .ogr_import_source import OgrTableImportSource

            return OgrTableImportSource.open(full_spec, table=table)

    @classmethod
    def check_valid(cls, import_sources, param_hint=None):
        """Given an iterable of TableImportSources, checks that all are fully specified and none of their dest_paths collide."""
        dest_paths = {}
        for s1 in import_sources:
            s1.check_fully_specified()
            dest_path = s1.dest_path
            if dest_path not in dest_paths:
                dest_paths[dest_path] = s1
            else:
                s2 = dest_paths[dest_path]
                raise click.BadParameter(
                    f"Can't import both {s1} and {s2} as {dest_path}",
                    param_hint=param_hint,
                )
        list_of_conflicts.check_sources_are_importable(import_sources)

    def check_fully_specified(self):
        """
        Some TableImportSources can be constructed only partially specified, but they will not work as an import source
        until they are fully specified. This checks that self is fully specified and raises an error if it is not.
        """
        pass

    @property
    def dest_path(self):
        """
        The destination path where this dataset should be imported.
        TableImportSource.dest_path can be set, otherwise defaults to TableImportSource.default_dest_path()
        """
        if hasattr(self, "_dest_path"):
            return self._dest_path
        return self.default_dest_path()

    @classmethod
    def _normalise_dataset_path(cls, path):
        # we treat back-slash and forward-slash as equivalent at import time.
        # (but we only ever import forward-slashes)
        return path.strip("/").replace("\\", "/")

    @dest_path.setter
    def dest_path(self, dest_path):
        self._dest_path = self._normalise_dataset_path(dest_path)

    def default_dest_path(self):
        """
        The default destination path where this dataset should be imported.
        This should be generated based on the source path / source table name of the TableImportSource.
        """
        raise NotImplementedError()

    def get_meta_item(self, name, missing_ok=True):
        """
        Find or generate a V3 meta item. A missing meta item is treated the same as it being None,
        but a client can set missing_ok=False to raise an error if it is missing.
        """
        result = self.meta_items().get(name)
        if result is None and not missing_ok:
            raise KeyError(f"No meta item found with name {name}")
        return result

    def meta_items(self):
        """
        Returns a dict of all the meta items that need to be imported. See TableV3.META_ITEMS.
        Meta items from this list can be ommitted if there is no data (eg, no title or description exists).
        Meta items not on this list can also be included, they will be stored verbatim in the resulting dataset.
        All CRS definitions from self.crs_definitions() should also be included with keys crs/{identifier}.wkt
        """
        raise NotImplementedError()

    def attachment_items(self):
        """
        Returns a dict of all the attachment items that need to be imported.
        These are files that will be imported verbatim to dest_path, but not hidden inside the dataset.
        This could be a license or a readme.
        """
        return {}

    def crs_definitions(self):
        """
        Returns an {identifier: definition} dict containing every CRS definition.
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
        if not self.has_geometry:
            return None
        crs_definitions_dict = self.crs_definitions()
        if identifier is not None:
            if identifier.startswith("crs/") and identifier.endswith(".wkt"):
                identifier = identifier[4:-4]
            return crs_definitions_dict[identifier]

        num_defs = len(crs_definitions_dict)
        if num_defs == 1:
            return next(iter(crs_definitions_dict.values()))
        raise ValueError(
            f"get_crs_definition() only works when there is exactly 1 CRS definition, but there is {num_defs}"
        )

    @property
    def schema(self):
        """
        The TableImportSource implementation must return the schema as a meta-item called "schema.json", so this accessor
        simply delegates to that. Calling self.align_schema_to_existing_schema(...) should modify the schema returned by
        self.meta_items()
        """
        return Schema.from_column_dicts(self.get_meta_item("schema.json"))

    def align_schema_to_existing_schema(self, existing_schema):
        """
        Aligning the schema with an existing schema means that the pre-existing colunms will keep the same ID
        that they had last time. Failing to align the schema would mean that some features would be re-encoded
        even if they hadn't actually changed. This should update the schema.json meta-item of this import source.
        """
        raise NotImplementedError()

    @property
    def has_geometry(self):
        return self.schema.has_geometry

    def features(self):
        """
        Yields a dict for every feature. Dicts contain key-value pairs for each feature property,
        and geometries use kart.geometry.Geometry objects, as in the following example::

        {
            "fid": 123,
            "geom": Geometry(b"..."),
            "name": "..."
            "last-modified": "..."
        }

        Each dict is guaranteed to iterate in the same order as the columns are ordered in the schema,
        so that zip(schema.columns, feature.values()) matches each field with its column.
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
        """Return a description of this TableImportSource."""
        # Subclasses should override if str() does not return the right information.
        return (
            f"Importing {self.feature_count} features from {self} to {self.dest_path}/"
        )

    def aggregate_import_source_desc(self, import_sources):
        """
        Return a description of this collection of import_sources (which should contain self).
        For example:

        Import 3 datasets from example.gpkg:
        first_table
        second_dataset (from second_table)
        third_table
        """
        # Subclasses should override this if a more useful aggregate description can be generated.
        return "\n".join(s.import_source_desc() for s in import_sources)

    def prompt_for_table(self, prompt):
        table_list = list(self.get_tables().keys())

        if not table_list:
            raise NotFound(f"No tables found in {self}", exit_code=NO_TABLE)

        if len(table_list) == 1:
            return table_list[0]
        else:
            self.print_table_list()
            if get_input_mode() == InputMode.NO_INPUT:
                raise NotFound("No table specified", exit_code=NO_TABLE)
            t_choices = click.Choice(choices=table_list)
            t_default = table_list[0] if len(table_list) == 1 else None
            return click.prompt(
                f"\n{prompt}",
                type=t_choices,
                show_choices=False,
                default=t_default,
            )
