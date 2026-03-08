"""
Batch metadata collection for working copy operations.
Collects metadata for multiple tables in single database queries to reduce round-trip overhead.
"""

from itertools import groupby

from kart.diff_structs import DeltaDiff, WORKING_COPY_EDIT


class WorkingCopyMetadataCollector:
    """
    Collects metadata for multiple tables in batch queries.
    This significantly reduces database round-trips when working with many datasets.
    """

    def __init__(self, working_copy):
        """
        Initialize the collector with a working copy instance.

        Args:
            working_copy: TabularWorkingCopy instance
        """
        self.working_copy = working_copy
        self.adapter = working_copy.adapter
        self._metadata_cache = {}

    def collect_batch(self, datasets):
        """
        Collect metadata for multiple datasets in batch queries.

        Args:
            datasets: List of dataset objects to collect metadata for

        Returns:
            Dict mapping dataset.path -> metadata dict
        """
        if not datasets:
            return {}

        # Extract table names from datasets
        table_names = [ds.table_name for ds in datasets]

        with self.working_copy.session() as sess:
            # Batch query all metadata
            batch_meta = self.adapter.all_v2_meta_items_batch(
                sess,
                self.working_copy.db_schema,
                table_names,
                id_salt=self.working_copy.get_tree_id(),
            )

        # Cache and return results mapped by dataset path
        result = {}
        for ds in datasets:
            if ds.table_name in batch_meta:
                meta = batch_meta[ds.table_name]
                self._metadata_cache[ds.path] = meta
                result[ds.path] = meta

        return result

    def get_cached_metadata(self, dataset):
        """
        Get cached metadata for a single dataset.

        Args:
            dataset: Dataset object

        Returns:
            Cached metadata dict, or None if not cached
        """
        return self._metadata_cache.get(dataset.path)

    def diff_dataset_to_working_copy_meta(self, dataset):
        """
        Get metadata diff for a dataset, using cached data if available.

        This is a drop-in replacement for the working_copy method that uses
        cached metadata when available.

        Args:
            dataset: Dataset object

        Returns:
            DeltaDiff of metadata changes
        """
        # Try to use cached metadata first
        wc_meta_items = self.get_cached_metadata(dataset)

        if wc_meta_items is None:
            # Fallback to regular single-table query
            wc_meta_items = self.working_copy.meta_items(dataset.table_name)

        # Get dataset metadata
        ds_meta_items = self.adapter.remove_empty_values(dataset.meta_items())

        # Remove hidden diffs (same as original implementation)
        self.working_copy._remove_hidden_meta_diffs(
            dataset, ds_meta_items, wc_meta_items
        )

        return DeltaDiff.diff_dicts(
            ds_meta_items, wc_meta_items, delta_flags=WORKING_COPY_EDIT
        )
