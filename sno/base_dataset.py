import functools
import logging

from .import_source import ImportSource

# So tests can patch this out. it's hard to mock memoryviews...
_blob_to_memoryview = memoryview


class BaseDataset(ImportSource):
    def __init__(self, tree, path):
        if self.__class__ is BaseDataset:
            raise TypeError("Cannot construct a BaseDataset - use a subclass")

        self.tree = tree
        self.path = path.strip("/")
        self.table_name = self.path.replace("/", "__")
        self.L = logging.getLogger(self.__class__.__qualname__)

    def default_dest_path(self):
        # ImportSource method - by default, a dataset should import with the same path it already has.
        return self.path

    @property
    @functools.lru_cache(maxsize=1)
    def meta_tree(self):
        return self.tree / self.META_PATH

    @property
    @functools.lru_cache(maxsize=1)
    def feature_tree(self):
        return self.tree / self.FEATURE_PATH

    def get_data_at(self, rel_path, as_memoryview=False, missing_ok=False):
        """
        Return the data at the given relative path from within this dataset.

        Data is usually returned as a bytestring.
        If as_memoryview=True is given, data is returned as a memoryview instead
        (this avoids a copy, so can make loops more efficient for many rows)

        If missing_ok is true, we return None instead of raising a KeyError for
        missing data.
        """
        leaf = None
        try:
            leaf = self.tree / str(rel_path)
        except KeyError:
            pass

        if leaf is not None and leaf.type_str == 'blob':
            if as_memoryview:
                try:
                    return _blob_to_memoryview(leaf)
                except TypeError:
                    pass
            else:
                try:
                    return leaf.data
                except AttributeError:
                    pass

        # If we got here, that means leaf wasn't a blob, or one of the above
        # exceptions happened...
        if missing_ok:
            return None
        else:
            raise KeyError(f"No data found at rel-path {rel_path}, type={type(leaf)}")
