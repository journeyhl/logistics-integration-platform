from pipelines import Pipeline
from connectors import CriteoAPI
import polars as pl
import json
import time
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from transform.criteo import Transform
from typing import Literal

class Criteo(Pipeline):
    '''`Criteo`(Pipeline)
    ---
    <hr>

    

    # Extraction
     - 

    # Transformation
     - 

    # Load
     - 

    # Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
    '''
    def __init__(self):
        '''`init`()
        ---
        <hr>
        
        Initializes Criteo Pipeline
        
        <hr>
        
        Sets
        ---
        >>> self.criteoapi = CriteoAPI(self)
        >>> self.transformer = Transform(self)
        >>> self.lookback_days = 30
        >>> self.api_max_days = 90
        >>> self.incremental_end = datetime.now(ZoneInfo('America/New_York')).date()
        >>> self.backfill_end = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(days=1)        
        '''
        super().__init__('criteo')
        self.criteoapi = CriteoAPI(self)
        self.transformer = Transform(self)
        self.lookback_days = 30
        self.api_max_days = 90
        self.incremental_end = datetime.now(ZoneInfo('America/New_York')).date()
        self.backfill_end = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(days=1)

    def extract(self) -> dict[str, pl.DataFrame]:
        '''`extract`(self)
        ---
        <hr>
        
        Extracts data from **criteo.campaign_performance_daily** via :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.query_db`

        Extracts data from Criteo's API with parameters set in :meth:`~_re_init` via :class:`~connectors.criteo_api.CriteoAPI`.:meth:`~connectors.criteo_api.CriteoAPI.fetch_campaign_data`
        
        ### Downstream Calls 
         #### :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.query_db`
            - Extracts data from **criteo.campaign_performance_daily**
         #### :class:`~connectors.criteo_api.CriteoAPI`.:meth:`~connectors.criteo_api.CriteoAPI.fetch_campaign_data`
            - Extracts data from **CriteoAPI**
            
        <hr>
        
        Returns
        ---
        :return `data_extract` (dict[str, pl.DataFrame]): dict of extracted data, containing **`db_extract`** and **`criteo_extract`**
        '''
        db_extract = self.centralstore.query_db('select * from criteo.campaign_performance_daily')
        criteo_extract = self.criteoapi.fetch_campaign_data()
        data_extract = {
            'db_extract': db_extract,
            'criteo_extract': criteo_extract
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        '''`transform`(data_extract: *dict[str, pl.DataFrame]*, )
        ---
        <hr>
        
        Transforms data_extract with **`self.transformer`**, :class:`~transform.criteo.Transform`
        
        ### Downstream Calls 
         #### :meth:`~transform.criteo.Transform.landing`
            - Landing function that orchestrations transformation of Criteo data
            
        <hr>
        
        Parameters
        ---
        :param (*dict[str, pl.DataFrame]*) `data_extract`: _description_
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (_type_): dict of transformed data, containing **`diff_log`** and **`criteo_transformed`**
        '''
        data_transformed = self.transformer.landing(data_extract)

        return data_transformed
    
    def load(self, data_transformed: dict[str, list]): 
        '''`load`(self, data_transformed: *dict[str, list]*)
        ---
        <hr>
        
        Upserts data to **criteo.campaign_performance_daily** and **criteo.diff_log** via :meth:`~connectors.sql.SQLConnector.checked_upsert`
        
        ### Downstream Calls 
         #### :meth:`~connectors.sql.SQLConnector.checked_upsert`
            - Given a table name and data formatted for its structure, perform upsert
            
        <hr>
        
        Parameters
        ---
        :param (*dict[str, list]*) `data_transformed`: _description_
        
        <hr>
        '''       
        self.centralstore.checked_upsert(table_name='criteo.diff_log', data=data_transformed['diff_log'])
        self.centralstore.checked_upsert(table_name='criteo.campaign_performance_daily', data=data_transformed['criteo_transformed'])
        pass
    
    def log_results(self, data_loaded):
        pass


    def _re_init(self, start_date: date, end_date: date, mode: Literal['incremental', 'backfill'] = 'incremental'):
        '''`_re_init`(self, start_date: *date*, end_date: *date*)
        ---
        <hr>
        
        Resets parameters to send to :class:`~connectors.criteo_api.CriteoAPI`.:meth:`~connectors.criteo_api.CriteoAPI.fetch_campaign_data`
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*date*) `start_date`: Low end of date window used to filter API query
        :param (*date*) `end_date`:  Low end of date window used to filter API query        
        
        <hr>
        
        Sets
        ---
        >>> self.mode = mode
        >>> self.start_date = start_date
        >>> self.end_date = end_date
        '''
        self.mode = mode
        self.start_date = start_date
        self.end_date = end_date