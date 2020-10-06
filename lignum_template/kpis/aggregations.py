import logging
import sys

from lib.python.tools.utils.uAggregations import index_to_timestamp, generate_time_variables, generate_tables
from lib.python.services.sqlServer import SqlAlchConnection
from lib.python.services.elastic import ElasticPython
from lib.python.tools.utils.uQuery import _DEFAULT_SCHEMA
from kpis.params.configReaderParams import configReader
from multiprocessing import Pool, cpu_count
from lib.python.tools.utils.uYaml import load_config


def multiprocess_work(function, args):
    '''
    :param function: str name of the function to run in parallel.
    :param args: list of tuples containing items to process
    :return:
    '''
    processes = min(cpu_count(), 4)
    with Pool(processes=processes) as pool:
        pool.starmap(function, args)


def prepare_inputs(dict_items, *args):
    '''
    Prepare inputs to run the main function run_aggregations
    :param dict_items: dict. Group of tables to generate
    :param args: necessary args
    :return:
    '''
    list_of_tuples_args = list()
    for key, value in dict_items.items():
        list_of_tuples_args.append((*args, {key: value}))
    return list_of_tuples_args


def run_aggregations(sqlcon, table, schema, elastic, config_reader):
    elastic_client = elastic(**load_config(env='dev')['elastic'])
    reader = sqlcon(**load_config(env='dev')['sqlserver_read'])
    tableDF = reader.read_table(table=table, schema=schema)
    tableDF = index_to_timestamp(tableDF, 'exe_date', 'time')
    tableDF = generate_time_variables(tableDF)
    # Generate new SQL table
    writer = sqlcon(**load_config(env='dev')['sqlserver_write'])
    generate_tables(tableDF, writer, config_reader, client_elastic=elastic_client)


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - '
                                                            '%(funcName)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    table = 'hourly_values'
    schema = _DEFAULT_SCHEMA
    if len(configReader.keys()) > 1:
        list_inputs = prepare_inputs(configReader, SqlAlchConnection, table, schema, ElasticPython)
        multiprocess_work(run_aggregations, list_inputs)
    else:
        run_aggregations(SqlAlchConnection, table, schema, ElasticPython, configReader)