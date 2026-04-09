from pipelines import Pipeline
from connectors import CriteoAPI
import polars as pl
import json
import time
from datetime import datetime, timedelta
from transform.criteo import Transform
from typing import Literal

class Criteo(Pipeline):
    def __init__(self):
        super().__init__('criteo')
        self.criteoapi = CriteoAPI(self)
        self.transformer = Transform(self)
        self.lookback_days = 30
        self.api_max_days = 90
        self.incremental_end = datetime.now().date()
        self.backfill_end = datetime.now().date() - timedelta(days=1)

    def extract(self) -> dict[str, pl.DataFrame]:
        db_extract = self.centralstore.query_db('select * from criteo.campaign_performance_daily')
        criteo_extract = self.criteoapi.fetch_campaign_data()
        data_extract = {
            'db_extract': db_extract,
            'criteo_extract': criteo_extract
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        data_transformed = self.transformer.landing(data_extract)

        return data_transformed
    
    def load(self, data_transformed: dict[str, list]):        
        self.centralstore.checked_upsert('criteo.diff_log', data_transformed['diff_log'])
        self.centralstore.checked_upsert('criteo.campaign_performance_daily', data_transformed['criteo_transformed'])
        pass
    
    def log_results(self, data_loaded):

        # self.logger.info(f'Logging acu_api interactions...')
        # for entry in data_loaded:
        #     entry['Payload'] = json.dumps(entry['Payload'])
        # self.centralstore.checked_upsert('_util.acu_api_log', data_loaded)
        pass


    def _re_init(self, start_date: str, end_date: str, mode: Literal['incremental', 'backfill'] = 'incremental'):
        self.mode = mode
        self.start_date = start_date
        self.end_date = end_date