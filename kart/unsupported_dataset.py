class UnsupportedDataset:
    """A dataset that Kart recognises as a dataset, but doesn't support beyond that."""

    def __init__(self, tree, path, repo, dirname=None):
        super().__init__(self, tree, path, repo, dirname=dirname)

    # TODO - add more functionality so that unsupported datasets can show up in a limited way in diffs etc.
