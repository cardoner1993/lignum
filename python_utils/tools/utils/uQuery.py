from pypika import Field, Query, Table
from sqlalchemy import NVARCHAR, FLOAT, INTEGER, REAL, DATETIME, DATE

"""
    Query Tools
"""

# Todo: In testing process
TYPES_SQL_KEY_DICT = {'character': NVARCHAR(50), 'time': NVARCHAR(8), 'date': DATE(),
                      'float': FLOAT(), 'month': NVARCHAR(7), 'year': NVARCHAR(4), 'datetime': DATETIME(),
                      'int': INTEGER(), 'real': REAL(), 'week': NVARCHAR(8), 'quarter': NVARCHAR(7)}

_DEFAULT_INDEX = ['local', 'exe_date']

_DEFAULT_SCHEMA = 'proves'

_OPERATORS = {
    'IN': Field.isin,
    'IS': Field.eq,
    '=': Field.eq,
    '>': Field.gt,
    '>=': Field.gte,
    '<': Field.lt,
    '<=': Field.lte
}


def get_table(table, schema=_DEFAULT_SCHEMA):
    if isinstance(table, Table):
        return table
    else:
        return Table(table, schema=schema)


def select(table, columns='*', where=None, order=None, schema=_DEFAULT_SCHEMA):
    table = get_table(table, schema=schema)
    query = Query.from_(table).select(*columns)
    if where:
        query = query.where(where)
    if order:
        query = query.orderby(*order)
    return query


def create_index(table, fields, schema=_DEFAULT_SCHEMA):
    table = get_table(table, schema=schema)
    fields = ', '.join(fields)
    return "CREATE INDEX [%s] ON %s (%s)" % (fields, table.get_sql(), fields)


def create_unique_index(table, fields, schema=_DEFAULT_SCHEMA):
    table = get_table(table, schema=schema)
    fields = ', '.join(fields)
    return "CREATE UNIQUE INDEX [%s] ON %s (%s)" % (fields, table.get_sql(), fields)
