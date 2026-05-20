
from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.audit_fulfillment import Transform
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class AcuToDbcPhoneRevenue(Pipeline):
    '''`AcuToDbcPhoneRevenue`(Pipeline)
    ---
    <hr>

    Gets all Orders having an *OrderType* of ***PH*** or ***BF*** and a non null phone number and loads to acu.PhoneRevByMonth in db_CentralStore

    # Extraction
     - Gets all ***PH*** or ***BF*** type Sales Orders from AcumaticaDb that were created or modified within the last day
        - The phone number must not be null
        - Rows with warranty items are excluded

    # Transformation
     - Converts pl.DataFrame to a list of dicts

    # Load
     - Upsert to **acu.PhoneRevByMonth** via :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.checked_upsert_paginated`

    # Results Logging
     - None needed
    '''
    def __init__(self, function: str):
        super().__init__('acu-to-dbc-phone-revenue', function)


    def extract(self) -> pl.DataFrame:
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.AcuToDbc_PhoneRevByMonth)
        return data_extract

    def transform(self, data_extract: pl.DataFrame):

        data_transformed = data_extract.to_dicts()
        return data_transformed
    
    def load(self, data_transformed):
        total = len(data_transformed)
        self.logger.info(f'{total} rows to upsert')
        self.centralstore.checked_upsert_paginated('acu.PhoneRevByMonth', data_transformed, page_size= 100)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass
