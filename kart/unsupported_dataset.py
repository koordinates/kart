class UnsupportedDataset:
    """A dataset that Kart recognises as a dataset, but doesn't support beyond that."""

    def __init__(self, tree, path, dirname=None, repo=None):
        self.tree = tree
        self.path = path
        if tree is not None and dirname is not None:
            self.inner_tree = tree / dirname
        else:
            self.inner_tree = None

        self.repo = repo

    # TODO - add more functionality so that unsupported datasets can show up in a limited way in diffs etc.
