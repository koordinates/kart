from kart.base_dataset import BaseDataset


class UnsupportedDataset(BaseDataset):
    """A dataset that Kart recognises as a dataset, but doesn't support beyond that."""

    # TODO - add more functionality so that unsupported datasets can show up in a limited way in diffs etc.
