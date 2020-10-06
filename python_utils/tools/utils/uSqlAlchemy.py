import logging
import sqlalchemy
from sqlalchemy.sql.elements import and_
from datetime import datetime

from lib.python.tools.utils.uQuery import TYPES_SQL_KEY_DICT
from lib.python.tools.utils.uYaml import load_config


def check_table_exist_or_create_and_upsert(data_to_insert, sql_reader, tblName, schema_to_insert_on, path, config_path):
    parameters_dict = load_config(env='tables', path_to_file=path, yaml_fie=config_path)[tblName]
    data_to_insert = add_insertion_time_column(data_to_insert)
    data_to_insert = rename_columns(data_to_insert, parameters_dict['columns'])
    unique = parameters_dict.get('unique', None)
    index = parameters_dict.get('index', None)
    if sql_reader.sql_engine.dialect.has_table(sql_reader.sql_engine, tblName, schema=schema_to_insert_on):
        logging.info("Upserting data into table %s" % tblName)
        table = sqlalchemy.Table(tblName, sql_reader.metadata, schema=schema_to_insert_on, autoload=True)
        where = and_(
            table.c[item].in_(data_to_insert[item].unique().tolist()) for item in parameters_dict['index'])
        sql_reader.upsert_results_where(data_to_insert, table, schema_to_insert_on, where, action='append')
    else:
        logging.info("Generating table %s and inserting data into it" % tblName)
        table_types = [TYPES_SQL_KEY_DICT[k] for k in parameters_dict['types']]
        sql_reader.define_and_create_table(tblName, schema_to_insert_on, parameters_dict['columns'],
                                           table_types, parameters_dict['primary_key_flag'],
                                           parameters_dict['nullable_flag'])
        if unique and index:
            sql_reader.define_index(tblName, schema_to_insert_on, index, 'index', unique=unique)
        else:
            logging.info(f"None index or unique clause for the index column")

        sql_reader.write_table(data_to_insert, tblName, schema_to_insert_on, sql_types_dict=None, action='append')


def rename_columns(data, columns_names):
    columns = dict(zip(data.columns, columns_names))
    data.rename(columns=columns, inplace=True)
    return data


def add_insertion_time_column(data):
    df = data.copy()
    time = datetime.utcnow()
    df["insertion_time"] = time
    return df