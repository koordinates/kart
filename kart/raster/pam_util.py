import re


def is_same_xml_ignoring_stats(lhs, rhs):
    """
    Returns True if two strings (left-hand-side, right-hand-side) of XML are the same,
    or if they are the same except that gdal has inserted statistics into one of them,
    in the form of a <Histograms> and/or <Metadata> block.
    """
    return (
        lhs == rhs
        or _lhs_is_rhs_minus_stats(lhs, rhs)
        or _lhs_is_rhs_minus_stats(rhs, lhs)
    )


ANY_TAG = r"(<[^<>]+>)"
HISTOGRAMS_PATTERN = re.compile(
    rf"{ANY_TAG}\s*<Histograms>(.*)</Histograms>\s*{ANY_TAG}", re.DOTALL
)
# TODO - maybe check for non-statistics metadata.
METADATA_PATTERN = re.compile(
    rf"{ANY_TAG}\s*<Metadata>(.*)</Metadata>\s*{ANY_TAG}", re.DOTALL
)


def _lhs_is_rhs_minus_stats(lhs, rhs):
    # Regex is sufficient for doing what we need to do here, which saves us parsing the XML.
    for rhs_pattern in (HISTOGRAMS_PATTERN, METADATA_PATTERN):
        rhs_match = re.search(rhs_pattern, rhs)
        if not rhs_match:
            continue
        pre_tag = rhs_match.group(1)
        post_tag = rhs_match.group(3)
        # Look for the equivalent place in the LHS - a place preceded by pre_tag and succeeded
        # by post_tag, but on the LHS it should be empty (contain only whitespace).
        lhs_pattern = rf"{re.escape(pre_tag)}\s*{re.escape(post_tag)}"
        lhs_match = re.search(lhs_pattern, lhs)
        if not lhs_match:
            # Can't find the equivalent place in the LHS, so, these files don't match.
            return False

        # We'll keep the tags outside this stats block intact (even though this code might work even if we didn't).
        replace_with = f"{pre_tag}{post_tag}"
        lhs_span = lhs_match.span()
        lhs = f"{lhs[:lhs_span[0]]}{replace_with}{lhs[lhs_span[1]:]}"
        rhs_span = rhs_match.span()
        rhs = f"{rhs[:rhs_span[0]]}{replace_with}{rhs[rhs_span[1]:]}"

        if lhs == rhs:
            return True

    return False
