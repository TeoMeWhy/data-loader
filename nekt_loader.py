# %%
import os
import dotenv
import sqlalchemy
import pandas as pd

dotenv.load_dotenv()

import nekt

nekt.data_access_token = os.getenv("NEKT_DATA_ACCESS_TOKEN")
MYSQL_ENGINE = sqlalchemy.create_engine(f'mysql+pymysql://{os.getenv("MYSQL_URI")}')

# %%

def nekt_to_mysql(layer_name, t_nekt, t_fs, schema, renames):
    fs_table = nekt.load_table(layer_name=layer_name, table_name=t_nekt)
    df= fs_table.toPandas().rename(columns=renames)
    df.to_sql(t_fs, schema=schema, con=MYSQL_ENGINE, if_exists="replace", index=False)