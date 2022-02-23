from kart.tabular.schema import ALL_DATA_TYPES

_ALL_SUBTYPES = {
    "integer": (8, 16, 32, 64),
    "float": (32, 64),
    "timestamp": ("UTC", None),
}


def compute_approximated_types(v2_to_sql, sql_to_v2):
    result = {}
    for orig_type in ALL_DATA_TYPES:
        subtypes = _ALL_SUBTYPES.get(orig_type)

        if subtypes:
            # Check roundtripping on all subtypes of this type.

            for orig_subtype in subtypes:
                sql_type = v2_to_sql[orig_type][orig_subtype]
                roundtripped_tuple = sql_to_v2[sql_type]

                orig_tuple = (orig_type, orig_subtype)

                if roundtripped_tuple != orig_tuple:
                    result[orig_tuple] = roundtripped_tuple

        else:
            # Type has no subtype - just check roundtripping on the type itself.
            sql_type = v2_to_sql[orig_type]
            roundtripped_type = sql_to_v2[sql_type]

            if roundtripped_type != orig_type:
                result[orig_type] = roundtripped_type

    return result
