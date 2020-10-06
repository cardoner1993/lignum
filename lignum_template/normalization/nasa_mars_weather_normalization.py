import json
import sys
import os
import pandas as pd
import logging

from lib.python.services import sqlServer
from lib.python.tools.utils.uDict import get_child

HOST_KEY = os.environ["HOST"]
PORT_KEY = os.environ["PORT"]
DB_KEY = os.environ["DB"]
USER_KEY = os.environ["USER"]
PASSWORD_KEY = os.environ["PASSWORD"]
ENGINE_KEY = os.environ["ENGINE"]
DIRECTORY_KEY = os.environ["LANDING_ZONE"]

table_name = 'mars_weather'
db_schema = 'nasa'

day_from = int(sys.argv[1])
day_to = int(sys.argv[2])

directory = f"{DIRECTORY_KEY}/api.nasa.gov/insight_weather/"
files = os.listdir(directory)

conn = sqlServer.SqlAlchConnection(host=HOST_KEY,
                                   port=PORT_KEY,
                                   database=DB_KEY,
                                   user=USER_KEY,
                                   password=PASSWORD_KEY,
                                   engine=ENGINE_KEY)

for file in files:
    file_day = file.split("-")[0]

    if day_to >= int(file_day) >= day_from:
        with open(f"{directory}{file}") as f:
            data_loaded = json.load(f)

            column_names = [
                "sol_key",
                "First_UTC",
                "Last_UTC",
                "AT_av",
                "AT_ct",
                "AT_mn",
                "AT_mx",
                "HWS_av",
                "HWS_ct",
                "HWS_mn",
                "HWS_mx",
                "PRE_av",
                "PRE_ct",
                "PRE_mn",
                "PRE_mx",
                "WD_compass_degrees",
                "WD_compass_point",
                "WD_compass_right",
                "WD_compass_up",
                "WD_ct"
            ]

            df = pd.DataFrame(columns=column_names)

            sol_keys = data_loaded['sol_keys']

            for sk in sol_keys:

                sol_key = sk
                First_UTC = get_child(['First_UTC'], data_loaded[sk]) #TODO: convert to DATE
                Last_UTC = get_child(['Last_UTC'], data_loaded[sk]) #TODO: convert to DATE
                AT_av = get_child(['AT', 'av'], data_loaded[sk])
                AT_ct = get_child(['AT', 'ct'], data_loaded[sk])
                AT_mn = get_child(['AT', 'mn'], data_loaded[sk])
                AT_mx = get_child(['AT', 'mx'], data_loaded[sk])
                HWS_av = get_child(['HWS', 'av'], data_loaded[sk])
                HWS_ct = get_child(['HWS', 'ct'], data_loaded[sk])
                HWS_mn = get_child(['HWS', 'mn'], data_loaded[sk])
                HWS_mx = get_child(['HWS', 'mx'], data_loaded[sk])
                PRE_av = get_child(['PRE', 'av'], data_loaded[sk])
                PRE_ct = get_child(['PRE', 'ct'], data_loaded[sk])
                PRE_mn = get_child(['PRE', 'mn'], data_loaded[sk])
                PRE_mx = get_child(['PRE', 'mx'], data_loaded[sk])
                WD_compass_degrees = get_child(['WD', 'most_common', 'compass_degrees'], data_loaded[sk])
                WD_compass_point = get_child(['WD', 'most_common', 'compass_point'], data_loaded[sk])
                WD_compass_right = get_child(['WD', 'most_common', 'compass_right'], data_loaded[sk])
                WD_compass_up = get_child(['WD', 'most_common', 'compass_up'], data_loaded[sk])
                WD_ct = get_child(['WD', 'most_common', 'ct'], data_loaded[sk])

                new_row = [
                            sol_key,
                            First_UTC,
                            Last_UTC,
                            AT_av,
                            AT_ct,
                            AT_mn,
                            AT_mx,
                            HWS_av,
                            HWS_ct,
                            HWS_mn,
                            HWS_mx,
                            PRE_av,
                            PRE_ct,
                            PRE_mn,
                            PRE_mx,
                            WD_compass_degrees,
                            WD_compass_point,
                            WD_compass_right,
                            WD_compass_up,
                            WD_ct
                ]

                df = df.append(pd.DataFrame([new_row], columns=column_names))

            try:
                conn.write_table(df,
                                 table_name,
                                 db_schema)
            except:
                logging.error(f"Error writing table {table_name}")
                pass


