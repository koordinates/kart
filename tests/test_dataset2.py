import os

from sno.dataset2 import Dataset2, Legend


class DictTree:
    """
    A fake directory tree structure.
    Contains a dict of all file data, {path: data}, and the current path.
    Supports only two operators:
    self / "some/path" returns a fake subdirectory / descendant file.
    self.data returns data of the current file, if there is a file at the current path.
    More complex directory navigation is not supported.
    """

    def __init__(self, all_data, cur_path=""):
        self.all_data = all_data
        self.cur_path = cur_path

        if all_data.get(cur_path) is not None:
            self.data = all_data[cur_path]

    def __truediv__(self, path):
        return DictTree(self.all_data, os.path.join(self.cur_path, path))


def test_legend_roundtrip():
    orig = Legend(["a", "b", "c"], ["d", "e", "f"])

    roundtripped = Legend.loads(orig.dumps())

    assert roundtripped is not orig
    assert roundtripped == orig

    path, data = Dataset2.encode_legend(orig)
    tree = DictTree({path: data})

    dataset2 = Dataset2(tree)
    roundtripped = dataset2.get_legend(orig.hexhash())

    assert roundtripped is not orig
    assert roundtripped == orig


def test_raw_dict_to_value_tuples():
    legend = Legend(["a", "b", "c"], ["d", "e", "f"])
    raw_feature_dict = {
        "e": "eggs",
        "a": 123,
        "f": None,
        "d": 5.0,
        "c": [0, 0],
        "b": True,
    }
    pk_values, non_pk_values = legend.raw_dict_to_value_tuples(raw_feature_dict)
    assert pk_values == (123, True, [0, 0])
    assert non_pk_values == (5.0, "eggs", None)
    roundtripped = legend.value_tuples_to_raw_dict(pk_values, non_pk_values)
    assert roundtripped is not raw_feature_dict
    assert roundtripped == raw_feature_dict


def test_feature_roundtrip():
    legend = Legend(["a", "b", "c"], ["d", "e", "f"])
    legend_path, legend_data = Dataset2.encode_legend(legend)

    raw_feature_dict = {
        "e": "eggs",
        "a": 123,
        "f": None,
        "d": 5.0,
        "c": [0, 0],
        "b": True,
    }
    feature_path, feature_data = Dataset2.encode_raw_feature_dict(
        raw_feature_dict, legend
    )
    tree = DictTree({legend_path: legend_data, feature_path: feature_data})

    dataset2 = Dataset2(tree)
    roundtripped = dataset2.read_raw_feature_dict(feature_path)
    assert roundtripped is not raw_feature_dict
    assert roundtripped == raw_feature_dict
