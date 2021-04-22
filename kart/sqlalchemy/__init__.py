import sqlalchemy as sa


def text_with_inlined_params(text, params):
    """
    Uses sqlalchemy feature bindparam(literal_execute=True)
    to ensure that the params are inlined as literals during execution (ie "LIMIT 5"),
    and not left as placeholders (ie "LIMIT :limit", {"limit": 5}).
    This is required when the DBA doesn't support placeholders in a particular context.
    See https://docs.sqlalchemy.org/en/14/core/sqlelement.html?highlight=execute#sqlalchemy.sql.expression.bindparam.params.literal_execute

    Note: this sqlalchemy feature is new and still a bit clunky.
    Each param can only be inlined once - to inline the same value twice, it must have two different names.
    """
    return sa.text(text).bindparams(
        *[
            sa.bindparam(key, value, literal_execute=True)
            for key, value in params.items()
        ]
    )
