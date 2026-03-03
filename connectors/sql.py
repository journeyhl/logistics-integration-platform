from config.settings import DATABASES
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
import polars as pl
import pandas as pd

class SQLConnector:

    def __init__(self, pipeline, database_name: str):
        self.pipeline = pipeline
        if database_name not in DATABASES:
            raise ValueError(f'Unknown db!')
        
        self.database_name = database_name
        self.config = DATABASES[database_name]
        self.engine = self._create_engine()
        self._pyodbc_connection = None
        pass

        
    def _create_engine(self):
        password = quote_plus(self.config['password'])
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
        return data_extract