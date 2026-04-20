from pipelines import Pipeline
from connectors import AcumaticaAPI, Kustomer
from transform.kustomer import Transform
from load.kustomer import Load
import polars as pl
from typing import Literal
import json

class SendOrderDetailsToKustomer(Pipeline):
    def __init__(self):
        super().__init__('kustomer-orders')
        self.transformer = Transform(self)
        self.api = Kustomer(self)
        self.loader = Load(self)


    def extract(self) -> pl.DataFrame:
        data_extract = self.acudb.query_to_dataframe(self.order_query)

        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = self.loader.landing(data_transformed)
        
        return data_loaded
    
    def log_results(self, data_loaded):
        self.logger.info(f'Logging Kustomer api interactions...')
        for entry in data_loaded:
            entry['jsonData'] = json.dumps(entry['jsonData'])
        self.centralstore.checked_upsert('json.K_OrderIngest', data_loaded)
        pass


    def _re_init(self, type: Literal['ingest', 'backfill'] = 'ingest'):
        if type == 'ingest':
            self.order_query = self.acudb.queries.Kustomer_OrderIngest
        elif type == 'backfill':
            self.order_query = self.acudb.queries.Kustomer_OrderIngestBackfill