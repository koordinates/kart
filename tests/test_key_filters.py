from kart.key_filters import RepoKeyFilter


def attachment_filter(*user_patterns):
    return RepoKeyFilter.build_from_user_patterns(user_patterns).as_attachment_filter()


def test_attachment_filter_match_all():
    # No patterns means match everything.
    f = attachment_filter()
    assert f.match_all
    assert "anything/at/all.txt" in f


def test_attachment_filter_whole_dataset_key_matches_file_and_folder():
    f = attachment_filter("aaa")
    # The file named exactly "aaa", and anything inside the folder "aaa":
    assert "aaa" in f
    assert "aaa/bbb/ccc" in f
    # But not a prefix that doesn't align on a path boundary:
    assert "a" not in f
    assert "aaabbb" not in f
    # And not an unrelated path:
    assert "bbb" not in f


def test_attachment_filter_folder_path_key():
    f = attachment_filter("aaa/bbb")
    assert "aaa/bbb" in f
    assert "aaa/bbb/ccc" in f
    assert "aaa" not in f
    assert "aaa/bbbbbb" not in f


def test_attachment_filter_filename_with_extension():
    f = attachment_filter("README.md")
    assert "README.md" in f
    assert "README" not in f
    assert "docs/README.md" not in f


def test_attachment_filter_disregards_subset_keys():
    # A filter that only matches a subset of a dataset (a feature/meta) doesn't name any attachment.
    f = attachment_filter("polygons:feature:123")
    assert not f
    assert "polygons" not in f
    assert "polygons/notes.txt" not in f


def test_attachment_filter_mixes_whole_and_subset_keys():
    f = attachment_filter("aaa", "bbb:feature:1")
    assert "aaa/x" in f
    # bbb is only matched as a subset, so its files are not attachments we match:
    assert "bbb" not in f
    assert "bbb/x" not in f


def test_attachment_filter_glob_matches_like_dataset_path():
    # A glob matches a file/folder path the same way it matches a dataset path.
    f = attachment_filter("data*")
    assert "database" in f  # file matched directly
    assert "data123/file.txt" in f  # folder matched, file inside it
    assert "db" not in f


def test_attachment_filter_glob_with_slash():
    f = attachment_filter("aaa/*")
    assert "aaa/bbb/ccc" in f
    # The folder "aaa" itself isn't matched by "aaa/*" (same as for dataset paths):
    assert "aaa" not in f
