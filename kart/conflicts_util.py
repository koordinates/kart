import click

# Stand in for a conflict if the conflict is going to be summarised anyway -
# this helps code re-use between summary and full-diff output modes.
_CONFLICT_PLACEHOLDER = object()


def set_value_at_dict_path(root_dict, path, value):
    """
    Ensures the given path exists as a nested dict structure in root dict,
    and then places the given value there. For example:
    >>> d = {"x": 1}
    >>> add_value_at_dict_path(d, ("a", "b", "c"), 100)
    >>> d
    {"a": {"b": {"c": 100}}, "x": 1}
    """
    cur_dict = root_dict
    for c in path[:-1]:
        cur_dict.setdefault(c, {})
        cur_dict = cur_dict[c]

    leaf = path[-1]
    cur_dict[leaf] = value


def summarise_conflicts(cur_dict, summarise):
    """
    Recursively traverses the tree of categorised conflicts,
    looking for a dict where the values are placeholders.
    For example:
    {
        K1: _CONFLICT_PLACEHOLDER,
        K2: _CONFLICT_PLACEHOLDER,
    }
    When found, it will be replaced with one of the following,
    depending on the summarise-level specified:
    summarise=1: [K1, K2]
    summarise=2: 2 (the size of the dict)
    """
    first_value = next(iter(cur_dict.values())) if cur_dict else None
    if first_value == _CONFLICT_PLACEHOLDER:
        if summarise == 1:
            return sorted(cur_dict.keys(), key=_path_sort_key)
        elif summarise >= 2:
            return len(cur_dict)

    for k, v in cur_dict.items():
        cur_dict[k] = summarise_conflicts(v, summarise)
    return cur_dict


def _path_sort_key(path):
    """Sort conflicts in a sensible way."""
    if isinstance(path, str) and ":" in path:
        return tuple(_path_part_sort_key(p) for p in path.split(":"))
    else:
        return _path_part_sort_key(path)


def _path_part_sort_key(path_part):
    # Treat stringified numbers as numbers
    if isinstance(path_part, str) and path_part.isdigit():
        path_part = int(path_part)

    # Put meta before features:
    if path_part == "meta":
        return "A", path_part
    elif path_part == "feature":
        return "B", path_part

    # Put complicated conflicts last:
    if isinstance(path_part, str) and "," in path_part:
        return "Z", path_part

    if isinstance(path_part, int):
        return "N", "", path_part
    else:
        return "N", path_part


def conflicts_json_as_text(value: str | int | dict | list, path="", level=0) -> str:
    """Converts the JSON output of list_conflicts to a string.

    The conflicts themselves should already be in the appropriate format -
    this function deals with the hierarchy that contains them.
    """
    if isinstance(value, str):
        return f"{value}\n"
    elif isinstance(value, int):
        return f"{value} conflicts\n"
    elif isinstance(value, dict):
        separator = "\n" if level == 0 else ""
        return separator.join(
            item_to_text(k, v, path, level) for k, v in sorted(value.items())
        )
    elif isinstance(value, list):
        indent = "    " * level
        return "".join(f"{indent}{path}{item}\n" for item in value)
    else:
        raise ValueError(f"Unexpected value type: {type(value)}")


def item_to_text(key: str, value: dict, path: str, level: int) -> str:
    key_text = f"{path}{key}:"
    color = get_key_text_color(key_text)
    styled_key_text = click.style("    " * level + key_text, fg=color)
    value_text = conflicts_json_as_text(value, key_text, level + 1)

    if isinstance(value, int):
        return f"{styled_key_text} {value_text}"
    else:
        return f"{styled_key_text}\n{value_text}"


def get_key_text_color(key_text: str) -> str | None:
    """Takes a given path and outputs an appropriate style for it

    The format for the key_text is:
        - For meta items: <dataset>:meta:schema.json:ancestor/theirs/ours:
        - For features:   <dataset>:feature:id:ancestor/theirs/ours:
    """
    style = {
        ":ancestor:": "red",
        ":ours:": "green",
        ":theirs:": "cyan",
    }
    for key, color in style.items():
        if key_text.endswith(key):
            return color
    return None
