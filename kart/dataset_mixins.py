from kart.diff_structs import DatasetDiff, DeltaDiff
from kart.key_filters import DatasetKeyFilter, MetaKeyFilter


class DatasetDiffMixin:
    """Adds diffing of meta-items to a dataset, by delegating to dataset.meta_items()"""

    def diff(self, other, ds_filter=DatasetKeyFilter.MATCH_ALL, reverse=False):
        """
        Generates a Diff from self -> other.
        If reverse is true, generates a diff from other -> self.
        """
        ds_diff = DatasetDiff()
        meta_filter = ds_filter.get("meta", ds_filter.child_type())
        ds_diff["meta"] = self.diff_meta(other, meta_filter, reverse=reverse)
        return ds_diff

    def diff_meta(self, other, meta_filter=MetaKeyFilter.MATCH_ALL, reverse=False):
        """
        Returns a diff from self -> other, but only for meta items.
        If reverse is true, generates a diff from other -> self.
        """
        if reverse:
            old, new = other, self
        else:
            old, new = self, other

        meta_old = (
            {k: v for k, v in old.meta_items().items() if k in meta_filter}
            if old
            else {}
        )
        meta_new = (
            {k: v for k, v in new.meta_items().items() if k in meta_filter}
            if new
            else {}
        )
        return DeltaDiff.diff_dicts(meta_old, meta_new)
