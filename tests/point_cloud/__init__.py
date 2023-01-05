import re


def assert_lines_almost_equal(a, b, places=10):
    """Asserts that two lists of strings strings are the same but allows for some floating point innacuracy."""
    a, b = fix_almost_equal_lines(a, b, places=places)
    assert a == b


def fix_almost_equal_lines(orig_a_lines, orig_b_lines, places=10):
    if (
        orig_a_lines is None
        or orig_b_lines is None
        or len(orig_a_lines) != len(orig_b_lines)
    ):
        return orig_a_lines, orig_b_lines

    a_lines = []
    b_lines = []
    for a_line, b_line in zip(orig_a_lines, orig_b_lines):
        a_line, b_line = fix_almost_equal_strings(a_line, b_line, places=places)
        a_lines.append(a_line)
        b_lines.append(b_line)
    return a_lines, b_lines


def fix_almost_equal_strings(orig_a_str, orig_b_str, places=10):
    if orig_a_str is None or orig_b_str is None or orig_a_str == orig_b_str:
        return orig_a_str, orig_b_str

    float_pattern = r"\d+\.\d+"
    a_floats = re.findall(float_pattern, orig_a_str)
    b_floats = re.findall(float_pattern, orig_b_str)

    if len(a_floats) != len(b_floats):
        return orig_a_str, orig_b_str

    shortened_pattern = r"\d+\.\d{1,%d}" % places
    common_floats = []
    for a_float, b_float in zip(a_floats, b_floats):
        if a_float == b_float:
            common_floats.append(a_float)
            continue
        # If the floats are the same for {places} decimal places, then truncate them both to that.
        a_shorter = re.search(shortened_pattern, a_float).group(0)
        b_shorter = re.search(shortened_pattern, b_float).group(0)
        if a_shorter == b_shorter:
            common_floats.append(a_shorter)
        else:
            break

    if len(common_floats) != len(a_floats):
        return orig_a_str, orig_b_str

    it = iter(common_floats)
    modifed_a = re.sub(float_pattern, lambda x: next(it), orig_a_str)
    it = iter(common_floats)
    modified_b = re.sub(float_pattern, lambda x: next(it), orig_b_str)
    return modifed_a, modified_b
