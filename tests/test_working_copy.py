from sno.schema import ALL_DATA_TYPES

_ALL_SIZE_VARIANTS = {
    "integer": (8, 16, 32, 64),
    "float": (32, 64),
}


def compute_approximated_types(v2_to_db, db_to_v2):
    result = {}
    for orig_type in ALL_DATA_TYPES:
        size_variants = _ALL_SIZE_VARIANTS.get(orig_type, (None,))
        for orig_size in size_variants:
            db_type = v2_to_db[orig_type]
            if orig_size is not None:
                db_type = db_type[orig_size]

            roundtripped_type = db_to_v2[db_type]
            roundtripped_size = None
            if isinstance(roundtripped_type, tuple):
                roundtripped_type, roundtripped_size = roundtripped_type

            if (orig_type, orig_size) == (roundtripped_type, roundtripped_size):
                continue

            if orig_size is not None:
                result[(orig_type, orig_size)] = (roundtripped_type, roundtripped_size)
            else:
                result[orig_type] = roundtripped_type

    return result
