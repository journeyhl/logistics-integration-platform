from config.settings import DATABASES, TABLES
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from pathlib import Path
import polars as pl
import pandas as pd
import logging
import pyodbc
from dataclasses import dataclass
from typing import Generic, TypeVar


@dataclass(frozen=True)
class Query:
    name: str
    query: str


class Queries:
    def __init__(self, database_name: str):
        queries_dir = Path(__file__).resolve().parent.parent / 'sql' / 'queries' / database_name
        for sql_file in queries_dir.glob('*.sql'):
            setattr(self, sql_file.stem, Query(name=sql_file.stem, query=sql_file.read_text()))

    def __getattr__(self, name: str) -> Query:
        raise AttributeError(f"No query named '{name}'")


class CentralStoreQueries(Queries):
    ReturnsPendingReciept: Query
    StatusCheckRMI: Query


class AcumaticaDbQueries(Queries):
    SendReturns: Query
    SendShipments: Query
    OpenRCsNoReceipt: Query


_QUERY_CLASSES: dict[str, type[Queries]] = {
    'db_CentralStore': CentralStoreQueries,
    'AcumaticaDb': AcumaticaDbQueries,
}

QT = TypeVar('QT', bound=Queries)


class SQLConnector(Generic[QT]):
    queries: QT


    def __init__(self, pipeline, database_name: str):
        self.pipeline = pipeline
        # self.logger = logging.getLogger(f'{pipeline.pipeline_name}.{database_name}')
        self.logger = logging.getLogger(f'{database_name}')
        if database_name not in DATABASES:
            raise ValueError(f'Unknown db!')

        self.database_name = database_name
        self.config = DATABASES[database_name]
        self.engine = self._create_engine()
        self.pyodbc_connection = pyodbc.connect(self._get_pyodbc_connection())
        self.queries = _QUERY_CLASSES.get(database_name, Queries)(database_name)  # type: ignore[assignment]
        pass

        
    def _create_engine(self):
        password = quote_plus(str(self.config['password']))
        connection_string = (
            f"mssql+pyodbc://{self.config['username']}:{password}"
            f"@{self.config['server']}/{self.config['database']}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
        )
        fast_executemany=True   # switches pyodbc from row-by-row to ODBC batch
        # mode — critical for large inserts (2M+ rows)
        return create_engine(connection_string, fast_executemany=True)
    
    def _get_pyodbc_connection(self) -> str:
        return (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={self.config['server']};"
            f"DATABASE={self.config['database']};"
            f"UID={self.config['username']};"
            f"PWD={self.config['password']}"
        )
    

    def query_db(self, query: str):        
        data_extract = pl.read_database(query, self.engine)
        self.logger.info(f'Extracted {data_extract.height} rows')
        return data_extract
    

    def insert_df(self, df_data_loaded: pl.DataFrame, table_name: str):
        '''_summary_

        _extended_summary_

        :param df_data_loaded: _description_
        :type df_data_loaded: pl.DataFrame
        :param table_name: _description_
        :type table_name: str
        '''
        df_data_loaded.write_database(table_name=table_name, 
                                      connection=self.engine,
                                      if_table_exists='append',                                      
                                      )
        bp = 'here'


    
    def checked_upsert(self, table_name: str, data: list):        
        self.tables = TABLES
        sql_table = self.tables[table_name]
        upsert_string = f'''
if not exists(
select 1 
from {table_name}
where {' = ? and '.join(sql_table['keys'])} = ?
)
begin
insert into {table_name}({', '.join(sql_table['columns'])})
values({', '.join(['?'] * len(sql_table['columns']))})
end
else
begin
update {table_name} set {' = ?, '.join(col for col in sql_table['update_columns'])} = ?
where {' = ? and '.join(sql_table['keys'])} = ?
end
'''
        try:
            params = [self._dict_to_params(data_dict, sql_table['keys'] + sql_table['columns'] + sql_table['update_columns'] + sql_table['keys']) for data_dict in data]
            cursor = self.pyodbc_connection.cursor()
            cursor.fast_executemany = True
            cursor.executemany(upsert_string, params)
            self.logger.info(f'{table_name} ╍ Upserted {len(data)} rows')
            cursor.commit()
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })
            bp = 'here'



    
    def _dict_to_params(self, d: dict, keys: list) -> tuple:
        return tuple(d[k.replace('[', '').replace(']', '')] for k in keys)
    

