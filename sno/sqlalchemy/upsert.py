from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql import elements
from sqlalchemy.sql.dml import ValuesBase


def upsert(table):
    """
    Returns a SQL commmand to insert of replace into the given table.
    table - sqlalchemy table definition. At a minimum, column names and primary keys must be included.
    """
    return Upsert(table)


class Upsert(ValuesBase):
    """
    A compilable SQL command to insert or replace into the given table.
    Performs an INSERT unless the inserted primary key(s) collide with existing primary keys,
    in which case, performs an UPDATE.
    """

    def __init__(self, table):
        ValuesBase.__init__(self, table, None, None)
        self._returning = None
        self._inline = None

    @property
    def columns(self):
        return self.table.columns

    @property
    def pk_columns(self):
        return [c for c in self.table.columns if c.primary_key]

    @property
    def non_pk_columns(self):
        return [c for c in self.table.columns if not c.primary_key]

    def values(self, compiler):
        return [self._create_bind_param(compiler, c) for c in self.table.columns]

    def _create_bind_param(self, compiler, col, process=True):
        bindparam = elements.BindParameter(col.key, type_=col.type, required=True)
        bindparam._is_crud = True
        bindparam = bindparam._compiler_dispatch(compiler)
        return bindparam


@compiles(Upsert, "sqlite")
def compile_upsert_sqlite(upsert_stmt, compiler, **kwargs):
    # See https://sqlite.org/lang_insert.html
    insert_stmt = upsert_stmt.table.insert().prefix_with("OR REPLACE")
    return compiler.process(insert_stmt)


@compiles(Upsert, "postgresql")
def compile_upsert_postgresql(upsert_stmt, compiler, **kwargs):
    # See https://docs.sqlalchemy.org/en/14/dialects/postgresql.html#insert-on-conflict-upsert
    insert_stmt = postgresql_insert(upsert_stmt.table)
    pk_col_names = [c.name for c in upsert_stmt.pk_columns]
    update_dict = {
        c.name: c for c in insert_stmt.excluded if c.name not in pk_col_names
    }
    insert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=pk_col_names, set_=update_dict
    )
    return compiler.process(insert_stmt)


@compiles(Upsert, "mssql")
def compile_upsert_mssql(upsert_stmt, compiler, **kwargs):
    # See https://docs.microsoft.com/sql/t-sql/statements/merge-transact-sql
    preparer = compiler.preparer

    def list_cols(col_names, prefix=""):
        return ", ".join([prefix + c for c in col_names])

    values = ", ".join(upsert_stmt.values(compiler))

    table = preparer.format_table(upsert_stmt.table)
    all_columns = [preparer.quote(c.name) for c in upsert_stmt.columns]
    pk_columns = [preparer.quote(c.name) for c in upsert_stmt.pk_columns]
    non_pk_columns = [preparer.quote(c.name) for c in upsert_stmt.non_pk_columns]

    result = f"MERGE {table} TARGET"
    result += f" USING (VALUES ({values})) AS SOURCE ({list_cols(all_columns)})"

    result += " ON "
    result += " AND ".join([f"SOURCE.{c} = TARGET.{c}" for c in pk_columns])

    result += " WHEN MATCHED THEN UPDATE SET "
    result += ", ".join([f"{c} = SOURCE.{c}" for c in non_pk_columns])

    result += " WHEN NOT MATCHED THEN INSERT "
    result += (
        f"({list_cols(all_columns)}) VALUES ({list_cols(all_columns, 'SOURCE.')});"
    )

    return result
