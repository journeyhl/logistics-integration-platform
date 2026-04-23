
from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.audit_fulfillment import Transform
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class AcuToDbcSalesOrders(Pipeline):
    '''`AcuToDbcSalesOrders`(Pipeline)
    ---
    <hr>

    Gets all Sales Orders from AcumaticaDb that were modified within the last few hours and loads them to **acu.SalesOrders**

    # Extraction
     - Gets all Sales Orders from AcumaticaDb that were modified within the last few hours
        - OrderType not in('QT', 'RA', 'RC', 'RR', 'RM')

    # Transformation
     - Transforms extracted data into a format needed for acu.SalesOrders

    # Load
     - Upsert to **acu.SalesOrders** via :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.checked_upsert_paginated`

    # Results Logging
     - None needed
    '''
    def __init__(self):
        super().__init__('acu-to-dbc-sales-orders')


    def extract(self) -> dict[str, pl.DataFrame]:
        acu_extract = self.acudb.query_to_dataframe(self.acudb.queries.AcuToDbc_SalesOrders)
        data_extract = {
            'acu_extract': acu_extract,
            'dbc_extract': '' #dbc_extract 
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        dbc_extract = data_extract['dbc_extract']
        acu_extract = data_extract['acu_extract']
        acu_extract = acu_extract.with_columns(
            pl.col('LineNbr').fill_null(99).alias('LineNbr')
        ).to_dicts()


        data_transformed = acu_extract
        return data_transformed
    
    def load(self, data_transformed):
        total = len(data_transformed)
        for item in data_transformed:
            item['LastChecked'] = datetime.now(ZoneInfo('America/New_York'))
        self.logger.info(f'{total} rows to upsert')
        self.centralstore.checked_upsert_paginated('acu.SalesOrders', data_transformed, page_size= 100)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass
