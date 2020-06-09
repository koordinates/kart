from osgeo import ogr


def adapt_value_noop(value):
    return value


def adapt_ogr_datetime(value):
    if value is None:
        return value
    # OGR uses this strange format: '2012/07/09 09:01:52+00'
    # We convert back to a normal ISO8601 format.
    return value.replace('/', '-').replace(' ', 'T').replace('+00', 'Z')


def get_type_value_adapter(ogr_type):
    """
    Returns a function which will convert values of the given OGR type
    into a more-sensible value.

    For most types this is a noop.
    """
    if ogr_type == ogr.OFTDateTime:
        return adapt_ogr_datetime
    return adapt_value_noop
