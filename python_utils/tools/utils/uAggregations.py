# Utils to get time variable as index to then generate useful variables.
from datetime import datetime
import pandas as pd
from math import ceil
import logging
import sqlalchemy
from sqlalchemy.sql.elements import and_

from python.tools.utils.uDate import get_yearstr, get_monthstr, sanitize_to_datestr, get_quarterstr, get_weekstr
from python.tools.utils.uQuery import _DEFAULT_SCHEMA, TYPES_SQL_KEY_DICT


def generate_tables(dataset, sql_reader, parameters_dict, client_elastic=None, action='append'):
    '''
    Generate new tables from old ones using config parameter to read the groupers, aggregators and variables
    Args:
        dataset: Table to apply transformations on it.
        sql_reader: Class to comunicate with the SQL database.
        parameters_dict: dict with the steps to apply in the data.
        client_elastic: instance of class ElasticPython.
        action: str action to apply on the write table statement. Options are fail, append or replace.
    '''
    for tblName in parameters_dict:
        assert all(item in parameters_dict[tblName] for item in ['groupers', 'aggregators', 'variables']), \
            'Check the configReader dict some of groupers, aggregators, variables parameters are missing'
        groupers = parameters_dict[tblName]['groupers']
        aggregators = parameters_dict[tblName]['aggregators']
        variables = parameters_dict[tblName]['variables']
        schema_to_insert_on = parameters_dict[tblName].get('schema', _DEFAULT_SCHEMA)
        unique_index = parameters_dict[tblName].get('unique_index', None)
        index = parameters_dict[tblName].get('index', None)
        table_types = [TYPES_SQL_KEY_DICT[k] for k in parameters_dict[tblName]['types']]

        groupedDF = apply_group_data(dataset, groupers, aggregators, variables)

        if sql_reader.sql_engine.dialect.has_table(sql_reader.sql_engine, tblName, schema=schema_to_insert_on):
            logging.info("Upserting data into table %s" % tblName)
            table = sqlalchemy.Table(tblName, sql_reader.metadata, schema=schema_to_insert_on, autoload=True)
            where = and_(table.c[item].in_(groupedDF[item].unique().tolist()) for item in parameters_dict[tblName]['unique_index'])
            sql_reader.upsert_results_where(groupedDF, table, schema_to_insert_on, where, action=action)
        else:
            logging.info("Generating table %s and inserting data into it" % tblName)
            sql_reader.define_and_create_table(tblName, schema_to_insert_on, parameters_dict[tblName]['columns'],
                    table_types, parameters_dict[tblName]['primary_key_flag'], parameters_dict[tblName]['nullable_flag'])
            if unique_index:
                sql_reader.define_index(tblName, schema_to_insert_on, unique_index, 'unique_index', unique=True)
            if index:
                sql_reader.define_index(tblName, schema_to_insert_on, index, 'index', unique=False)

            sql_reader.write_table(groupedDF, tblName, schema_to_insert_on, action='append')

        if client_elastic:
            message = client_elastic.prepare_data('Aggregation process', 'INFO', info_resp='Inserted table %s' % str(tblName))
            client_elastic.load_json_data(doc_type="logs", message=message)


def index_to_timestamp(df, time_variable, extra_time_variable=None):
    '''
    Takes dataframe and transforms its index to timestamp index. to do that it recives the time column variable.
    If time column has more than one value (ex date and time) then it combines to make the index as timestamp.
    :param df: Dataframe. Object from where generate index timestamp
    :param time_variable: str column name variable with timestamp format or date format
    :param extra_time_variable: str optional. Variable with time format (hh:mm:ss)
    :return: pd Dataframe.
    '''
    data = df.copy()
    if extra_time_variable is not None:
        if not isinstance(data[time_variable][0], str):
            try:
                data[time_variable] = list(data[time_variable].apply(lambda x: str(x)))
            except Exception as e:
                print("Something happen when trying to transform date to str. Reason %s" % str(e))

        data[time_variable] = data[time_variable] + data[extra_time_variable]
        data.index = pd.to_datetime(data[time_variable], format='%Y-%m-%d%H:%M:%S')
    else:
        if not any(isinstance(data[time_variable][0], item) for item in [datetime, pd.Timestamp]):
            data.index = pd.to_datetime(data[time_variable], format='%Y-%m-%d%H:%M:%S')

        else:
            data.set_index(time_variable, drop=True, inplace=True)

    return data


def week_of_month(datetime):
    """ Returns the week of the month for the specified date.
    """
    dom = datetime.day
    return int(ceil(dom/7.0))


def generate_time_variables(df):
    '''
    Generates time variables from the timestamp index variable
    :param df: Dataframe object
    :return: Dataframe with new generated variables.
    '''
    data = df.copy()
    data['month'] = list(map(get_monthstr, df.index))
    data['year'] = list(map(get_yearstr, df.index))
    data['day'] = list(map(sanitize_to_datestr, df.index))
    data['quarter'] = list(map(get_quarterstr, df.index))
    data['weekday'] = data.index.weekday.astype(str)
    data['week_year'] = list(map(get_weekstr, df.index))
    data['week_month'] = list(map(week_of_month, data.index))

    return data


def apply_group_data(df, groupers, aggregations, variables):
    '''
    Generate groupby applied over variables with aggregations
    :param df: Dataframe. Table from which generate the group by
    :param groupers: list. Set the grouper variables
    :param aggregations: list. Set of aggregations for variables
    :param variables: list. Variables to apply the aggregation on it
    :return: Dataframe. Grouped df with the aggreagtions applied on it
    '''
    data = df.copy()
    aggregat = {var: [value for value in aggregations] for var in variables}
    result = data.groupby(groupers).agg(aggregat)
    result.columns = [variables[i] + '_' + aggregations[j] for i in range(len(variables)) for j in range(len(aggregations))]

    return result.reset_index()
