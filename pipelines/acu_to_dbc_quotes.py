
from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.audit_fulfillment import Transform
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class AcuToDbcQuotes(Pipeline):
    def __init__(self):
        super().__init__('acu-to-dbc-quotes')


    def extract(self) -> pl.DataFrame:
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.AcuToDbc_Quotes)
        return data_extract

    def transform(self, data_extract: pl.DataFrame):
        data_transformed = data_extract.with_columns(
            pl.col('LineNbr').fill_null(99).alias('LineNbr'),
            pl.lit(datetime.now(ZoneInfo('America/New_York'))).alias('LastChecked')
        ).to_dicts()
        return data_transformed
    
    def load(self, data_transformed):
        if len(data_transformed) >= 500:
            start = 0
            end = 500
            while end < len(data_transformed):
                self.centralstore.checked_upsert('acu.Quotes', data_transformed[start:end])
                start += 500
                end += 500
                bp = 'here'
            if len(data_transformed) - start <= 500:
                self.centralstore.checked_upsert('acu.Quotes', data_transformed[start:])
                bp = 'here'
            bp = 'here'
            


        data_loaded = self.centralstore.checked_upsert('acu.Quotes', data_transformed)
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
