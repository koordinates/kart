from xml.dom import minidom
from xml.dom.minidom import Node


def is_same_xml_ignoring_stats(lhs, rhs):
    """
    Returns True if two files (left-hand-side, right-hand-side) containing XML are the same,
    or if they are the same except that gdal has inserted statistics into one of them,
    in the form of <Histograms> and/or <Metadata> blocks.
    Either file can be None meaning that the file does not exist.

    The files, when not None, are read using minidom.parse, so can be string paths or file-like objects.
    """
    if lhs == rhs:
        return True
    elif lhs is None or rhs is None:
        return _is_only_stats(lhs or rhs)
    else:
        return _is_same_xml_ignoring_stats(lhs, rhs)


STATISTICS_ELEMENTS = ["Histograms", "Metadata"]
STATISTICS_CONTAINING_ELEMENTS = ["PAMDataset", "PAMRasterBand"]


def _is_only_stats(xml_file):
    try:
        with minidom.parse(xml_file) as parsed:
            return _is_node_only_stats(parsed.firstChild)
    except Exception:
        # Don't suppress diffs if we weren't able to parse them.
        return False


def _is_node_only_stats(xml_node):
    if xml_node.nodeType == Node.TEXT_NODE:
        return xml_node.wholeText.isspace()
    elif xml_node.nodeType == Node.ELEMENT_NODE:
        if xml_node.tagName in STATISTICS_ELEMENTS:
            return True
        elif xml_node.tagName in STATISTICS_CONTAINING_ELEMENTS:
            return all(_is_node_only_stats(child) for child in xml_node.childNodes)
        else:
            return False
    else:
        return False


def _is_same_xml_ignoring_stats(lhs, rhs):
    try:
        with minidom.parse(lhs) as lhs_parsed, minidom.parse(rhs) as rhs_parsed:
            return _is_same_element(
                lhs_parsed.firstChild, rhs_parsed.firstChild, filter_out_stats=True
            )
    except Exception:
        # Don't suppress diffs if we weren't able to parse them.
        return False


def _is_same_element(lhs_element, rhs_element, filter_out_stats=True):
    assert lhs_element.nodeType == Node.ELEMENT_NODE
    assert rhs_element.nodeType == Node.ELEMENT_NODE

    if lhs_element.tagName != rhs_element.tagName:
        return False

    if bool(lhs_element.attributes) != bool(rhs_element.attributes):
        return False

    if bool(lhs_element.attributes):
        if lhs_element.attributes.items() != lhs_element.attributes.items():
            return False

    lhs_children = lhs_element.childNodes
    rhs_children = rhs_element.childNodes
    if filter_out_stats:
        lhs_children = _filter_out_stats(lhs_children)
        rhs_children = _filter_out_stats(rhs_children)

    for lhs_child, rhs_child in zip(lhs_children, rhs_children):
        if lhs_child is None or rhs_child is None and lhs_child != rhs_child:
            return False

        if lhs_child.nodeType != rhs_child.nodeType:
            return False

        if lhs_child.nodeType == Node.TEXT_NODE:
            if lhs_child.wholeText != rhs_child.wholeText:
                return False
        elif lhs_child.nodeType == Node.ELEMENT_NODE:
            if lhs_child.tagName != rhs_child.tagName:
                return False
            if not _is_same_element(
                lhs_child,
                rhs_child,
                filter_out_stats=filter_out_stats
                and lhs_child.tagName in STATISTICS_CONTAINING_ELEMENTS,
            ):
                return False

    return True


def _filter_out_stats(node_iter):
    for node in node_iter:
        if node.nodeType == Node.TEXT_NODE:
            if not node.wholeText.isspace():
                yield node
        elif node.nodeType == Node.ELEMENT_NODE:
            if node.tagName not in STATISTICS_ELEMENTS:
                yield node
        else:
            yield node
