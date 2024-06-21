from collections.abc import Iterable

from kart.tile import ALL_TILE_DATASET_TYPES


class UrlRedirector:
    """
    Loads a set of redirect rules that apply to linked-datasets from a given commit.

    Suppose, for example, a user migrates all their data from one S3 region to another, for whatever reason.
    And suppose the bucket in the new region has a new name, since bucket names are globally unique.
    (It may be possible to migrate the bucket name, but for the purpose of this example, the new bucket has a new name).
    That will break a linked-dataset where the URLs embedded in each tile point to the original bucket.

    The workaround: each linked-dataset has a meta-item called "linked-storage.json", which may contain a mapping
    called "urlRedirects". If these redirect rules are updated appropriately, then URLs that point to the old bucket
    will be treated as if they point to the new bucket, without needing to update the URL in every single tile
    individually and retroactively.

    Here is an example urlRedirects mapping that contains 3 rules:
    {
        "s3://old/and/broken/": "s3://new/and/shiny/",
        "s3://old/path/to/tile.laz": "s3://new/path/to/same/tile.laz",
        "s3://old/", "s3://new/"
    }

    This would be applied to an URL as follows - each rule is attempted in turn.
    If a rule applies, the url is updated, and subsequent rules are attempted against the updated url.
    Eventually the url - which may have been updated by zero, one, or many rules - is returned.

    - The first rule ends with a '/' so it does prefix matching:
      If the url starts with "s3://old/and/broken/", this prefix will be replaced with "s3://new/and/shiny/"
    - The second rule does not end with a '/' so it does exact matching:
      If the url is now exactly "s3://old/path/to/tile.laz", it will be set to" s3://new/path/to/same/tile.laz"
    - The third rule ends with a '/' so it does prefix matching:
      If the url now starts with "s3://old/", this prefix will be replaced with "s3://new/"

    Currently url redirect rules are only loaded from the HEAD commit - this is subject to change.
    """

    def __init__(self, repo, commit=None):
        # TODO - improve redirect-commit finding logic - probably do some of the following:
        # - find the tip of the default branch
        # - find the local tip of the branch that the remote HEAD was pointing to when we last fetched
        # - find a branch specified somehow in the config as the url-redirect branch

        self.commit = commit if commit is not None else repo.head_commit

        self.dataset_to_redirects = {}

        if not self.commit:
            return

        for dataset in repo.datasets(
            self.commit, filter_dataset_type=ALL_TILE_DATASET_TYPES
        ):
            linked_storage = dataset.get_meta_item("linked-storage.json")
            redirects = linked_storage.get("urlRedirects") if linked_storage else None
            if redirects:
                self.dataset_to_redirects[dataset.path] = redirects

    def apply_redirect(self, url, dataset):
        # It could be the case that a single LFS object is in more than one dataset.
        # In this case, we just try to find any set of redirect rules that applies to the object.
        if isinstance(dataset, Iterable) and not isinstance(dataset, str):
            for d in dataset:
                result = self.apply_redirect(url, d)
                if result != url:
                    return result
            return url

        if not isinstance(dataset, str):
            dataset = dataset.path
        redirects = self.dataset_to_redirects.get(dataset)
        if not redirects:
            return url

        for from_, to_ in redirects.items():
            if from_.endswith("/"):
                if url.startswith(from_):
                    url = to_ + url[len(from_) :]
            else:
                if url == from_:
                    url = to_

        return url
