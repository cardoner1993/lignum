import os
import sys
from fbprophet import Prophet

# from forecast.utils_prophet import main_fprophet
from lib.python.services.sqlServer import SqlAlchConnection
from lib.python.tools.utils.uSqlAlchemy import check_table_exist_or_create_and_upsert

# parser options
# parser = argparse.ArgumentParser()
# parser = optparse.OptionParser()
# parser.add_option('--window', help='number of forecast window', type=int, default=10)
# optionDict, remainder = parser.parse_args()

# Root path
root_path = os.path.dirname(os.path.abspath(__file__))

# Read from SQL
engine = os.environ["ENGINE"]
host = os.environ["HOST"]
port = os.environ["PORT"]
user = os.environ["USER"]
password = os.environ["PASSWORD"]
database = os.environ["DB"]

schema = "nasa"
table_name = "mars_weather_deduplicated"

conn = SqlAlchConnection(engine=engine,
                         host=host,
                         port=port,
                         user=user,
                         password=password,
                         database=database)

tableDF = conn.read_table(table_name, schema)

ds = sys.argv[1]
y = sys.argv[2]

# schema = "nasa"
# table_name = "mars_weather_deduplicated"

inputDF = tableDF.loc[:, [ds, y]]
inputDF.rename(columns={ds: "ds", y: "y"}, inplace=True)

# Config to read
config_file = 'config_tables_sql.yml'

# model = main_fprophet()
model = Prophet()
model.fit(inputDF, iter=3000)

futureDF = model.make_future_dataframe(periods=12, include_history=False)
forecastDF = model.predict(futureDF)
forecastDF['ds'] = list(map(lambda x: x.date(), forecastDF['ds']))

# updateDB
table_name = 'forecast_mars_weather_deduplicated_additive'
ADDITIVE_COLS = ['additive_terms', 'additive_terms_lower', 'additive_terms_upper']
MULTIPLICATIVE_COLS = ['multiplicative_terms', 'multiplicative_terms_lower', 'multiplicative_terms_upper']
STL_COLS = ['ds', 'trend_lower',  'trend', 'trend_upper']
FORECAST_COLS = ['ds', 'yhat_lower', 'yhat', 'yhat_upper']
WRITE_COLS = FORECAST_COLS + ADDITIVE_COLS  # Todo Pensar aixo
check_table_exist_or_create_and_upsert(forecastDF[WRITE_COLS], conn, table_name, schema, root_path, config_file)
# conn.write_table(forecastDF.loc[:, FORECAST_COLS], table_name, schema, sql_types_dict=None, action='replace')

