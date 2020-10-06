import pandas as pd
from sqlalchemy.sql.elements import and_
import logging
import sqlalchemy
from sqlalchemy_views import CreateView, DropView

from lib.python.tools.utils.uYaml import load_config


def select_alchemy(table, columns=None, where=None, order=  None):
    query = sqlalchemy.select(columns) if columns is not None else table.select()
    if where is not None:
        query = query.where(where)
    if order is not None:
        query = query.order_by(order)
    return query


class SqlAlchConnection(object):
    def __init__(self, engine, host, port, database, user, password):
        '''
        By default the connection is made to the sql_server client
        :param engine: str. Available mysql and sql_server
        :param host: str Host IP
        '''
        self.engine = engine
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = database
        self._engine = self.sql_server_engine() if self.engine == 'sql_server' else self.mysql_engine()
        self.metadata = sqlalchemy.MetaData(self._engine)

    @property
    def sql_engine(self):
        return self._engine

    def sql_server_engine(self):
        self._engine = sqlalchemy.create_engine('mssql+pymssql://{}:{}@{}:{}/{}'
                                                .format(self.user, self.password, self.host, self.port,
                                                        self.db))
        return self._engine

    def mysql_engine(self):
        self._engine = sqlalchemy.create_engine('mysql+pymysql://{}:{}@{}:{}/{}'
                                                .format(self.user, self.password, self.host, self.port,
                                                        self.db))
        return self._engine

    def reflect(self, schema):
        self.metadata.reflect(schema=schema)

    def drop_view(self, view_name):
        drop_view = DropView(view_name, if_exists=True)
        self._engine.execute(drop_view)

    def create_view(self, definition, view_name, schema, replace=False):
        self.reflect(schema=schema)
        view_name = sqlalchemy.Table(view_name, self.metadata, schema=schema)
        if replace:
            self.drop_view(view_name)
        create_view = CreateView(view_name, definition, or_replace=False)
        self._engine.execute(create_view)

    def define_and_create_table(self, tbl_name, schema, columns_names, columns_types, primary_key_flags,
                                nullable_flags):

        table = sqlalchemy.Table(tbl_name, self.metadata,
                                 *(sqlalchemy.Column(column_name, column_type,
                                                     primary_key=primary_key_flag,
                                                     nullable=nullable_flag)
                                   for column_name,
                                       column_type,
                                       primary_key_flag,
                                       nullable_flag in zip(columns_names,
                                                            columns_types,
                                                            primary_key_flags,
                                                            nullable_flags)),
                                 schema=schema)

        table.create()

    def define_index(self, tbl_name, schema, index, name, unique=False):
        table = sqlalchemy.Table(tbl_name, self.metadata, schema=schema, autoload=True)
        index_field = sqlalchemy.Index(name, *[table.c[item] for item in index], unique=unique)
        index_field.create()

    def read_table(self, table, schema):
        try:
            with self._engine.connect() as conn:
                table = pd.read_sql_table(table, con=conn, schema=schema)
                return table
        except Exception as e:
            logging.error("Something happen when reading table. Reason %s" % str(e))

    def read_query(self, query):
        '''
        :param query: str. Query to be performed in sql format
        :return: Dataframe. Table with the resultant query
        '''
        try:
            with self._engine.connect() as conn:
                table = pd.read_sql_query(query, con=conn)
                return table
        except Exception as e:
            logging.error("Something happen when reading table. Reason %s" % str(e))

    def define_and_read_query(self, from_query_dict):
        table = from_query_dict.get('table')
        columns = from_query_dict.get('columns', None)
        where = from_query_dict.get('where', None)
        order = from_query_dict.get('order', None)
        distinct = from_query_dict.get('distinct', False)

        query = select_alchemy(table, columns=columns, where=where, order=order)
        if distinct:
            query = query.distinct()
        retDF = self.read_query(query=query)
        return retDF

    @staticmethod
    def define_query(from_query_dict):
        table = from_query_dict.get('table')
        columns = from_query_dict.get('columns', None)
        where = from_query_dict.get('where', None)
        order = from_query_dict.get('order', None)
        distinct = from_query_dict.get('distinct', False)

        query = select_alchemy(table, columns=columns, where=where, order=order)
        if distinct:
            query = query.distinct()

        return query

    def write_table(self, df, table, schema, sql_types_dict=None, action='append'):
        '''
        :param df:
        :param table: str. Table Name.
        :param sql_types_dict: Deprecated. dict of types for the table to be created.
        :param action: str. What to do with the table if exists. Options accepted 'append' & 'replace' & 'fail'.
        :return:
        '''
        logging.info("Inserting table %s" % str(table))
        try:
            with self._engine.connect() as conn:
                df.to_sql(table, if_exists=action, con=conn, schema=schema, index=False, dtype=sql_types_dict,
                          chunksize=1000)
        except Exception as e:
            logging.error("Something happen when reading table. Reason %s" % str(e))

    def upsert_results_where(self, df, table, schema, where, action):
        assert where is not None, "Error Condition where is necessary. Exiting code"
        deletion_where = table.delete().where(where)
        self._engine.execute(deletion_where)
        self.write_table(df, table.description, schema, action=action)


if __name__ == '__main__':
    table_name = 'hourly_values'
    schema = 'proves'
    local = 'GrillosaBCN01'
    date = '2019-01-29'
    columns = ['local', 'exe_date']
    sql_read = SqlAlchConnection(**load_config(env='dev')['sqlserver_read'])
    table = sqlalchemy.Table(table_name, sql_read.metadata, schema=schema, autoload=True)
    where = and_(table.c.local == local, table.c.exe_date.in_([date]))
    # where = and_(table.c.local == local, table.c.exe_date.__lt__([date]))
    queryDict = {'table': table, 'where': where}
    df = sql_read.define_and_read_query(queryDict)
    sql_read.upsert_results_where(df, table, schema, where, action='append')