from types import SimpleNamespace

import polars as pl
from connectors.sql import SQLConnector

sql = SQLConnector('base', 'db_CentralStore')
df = pl.read_csv('uszips.csv', schema_overrides={'zip': pl.Utf8})
df = df.rename({col: col.replace('_', ' ').title().replace(' ', '_') for col in df.columns})
df = df.drop(['Zcta', 'Parent_Zcta'])
sql.insert_df(df, '_.ZipCodes')
bp = 'here'

