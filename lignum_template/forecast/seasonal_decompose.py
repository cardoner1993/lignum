# StatsModels interpolation based on LOESS
from statsmodels.tsa.seasonal import STL
import pandas as pd
import os
import sys

from lib.python.tools.utils.uSqlAlchemy import check_table_exist_or_create_and_upsert
from lib.python.services.sqlServer import SqlAlchConnection


def seasonal_decomposition_loess(data, freq='D', visualize=False):
    df = data.copy()
    data_series = pd.Series(df['y'].values, index=pd.to_datetime(df['ds'], format='%Y-%m-%d %H:%M:%S'),
                            name='serie').asfreq(freq)
    data_series.interpolate(method='time', inplace=True)  # Interpolate if has missing data
    print(data_series.describe())

    stl = STL(data_series, seasonal=13)
    res = stl.fit()
    if visualize:
        fig = res.plot()

    return res


def combine_decomposed_stl_object(data):
    '''
    Transform the decomposeresult into a pandas DataFrame.
    :param data: Decomposeresult object
    :return:
    '''
    result_df = pd.concat([data.observed, data.seasonal, data.trend, data.resid], axis=1)
    return result_df


def main_decomposition(data, freq):
    decompose_data = seasonal_decomposition_loess(data, freq=freq, visualize=False)
    result_decompose_df = combine_decomposed_stl_object(decompose_data)
    result_decompose_df = result_decompose_df.reset_index().rename(columns={result_decompose_df.index.name: 'time'})
    result_decompose_df['time'] = list(map(lambda x: x.date(), result_decompose_df['time']))
    return result_decompose_df


if __name__ == '__main__':
    # Root path
    root_path = os.path.dirname(os.path.abspath(__file__))

    engine = os.environ["ENGINE"]
    host = os.environ["HOST"]
    port = os.environ["PORT"]
    user = os.environ["USER"]
    password = os.environ["PASSWORD"]
    database = os.environ["DB"]

    schema = 'nasa'
    table_name = 'mars_weather_deduplicated'

    conn = SqlAlchConnection(engine=engine,
                             host=host,
                             port=port,
                             user=user,
                             password=password,
                             database=database)

    tableDF = conn.read_table(table_name, schema)

    # Reading First_UTC hws_av from table_name mars_weather_deduplicated
    ds = sys.argv[1]
    y = sys.argv[2]

    # Config to read from
    config_file = 'config_tables_sql.yml'

    # Prova descomposició
    inputDF = tableDF.loc[:, [ds, y]]
    inputDF.rename(columns={ds: "ds", y: "y"}, inplace=True)
    table_name = 'seas_decomposition_mars_weather_deduplicated'
    seasonal_decomposed_df = main_decomposition(inputDF, freq='D')
    check_table_exist_or_create_and_upsert(seasonal_decomposed_df, conn, table_name, schema, root_path, config_file)
    # conn.write_table(seasonal_decomposed_df, table_name, schema, sql_types_dict=None, action='replace')
