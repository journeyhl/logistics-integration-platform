
from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.audit_fulfillment import Transform
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class AcuToDbcSalesOrders(Pipeline):
    def __init__(self):
        super().__init__('acu-to-dbc-sales-orders')


    def extract(self) -> dict[str, pl.DataFrame]:
        acu_extract = self.acudb.query_to_dataframe(self.acudb.queries.AcuToDbc_SalesOrders)
        dbc_extract = self.centralstore.query_db('select distinct OrderNumber from acu.SalesOrders where LastChecked is not null')
        data_extract = {
            'acu_extract': acu_extract,
            'dbc_extract': dbc_extract
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        dbc_extract = data_extract['dbc_extract']
        acu_extract = data_extract['acu_extract']
        # acu_extract = data_extract['acu_extract'].join(
        #     dbc_extract, on='OrderNumber', how='anti'
        # )
        acu_extract = acu_extract.with_columns(
            pl.col('LineNbr').fill_null(99).alias('LineNbr')
        ).to_dicts()


        data_transformed = acu_extract
        return data_transformed
    
    def load(self, data_transformed):
        rows_remaining = len(data_transformed)
        upserts = 0
        self.logger.info(f'{rows_remaining} rows to upsert')
        if len(data_transformed) >= 100:
            start = 0
            end = 100
            while end < len(data_transformed):
                for item in data_transformed[start:end]:
                    item['LastChecked'] = datetime.now(ZoneInfo('America/New_York'))
                self.centralstore.checked_upsert('acu.SalesOrders', data_transformed[start:end])
                start += 100
                end += 100
                rows_remaining -= 100
                upserts += 1
                self.logger.info(f'{len(data_transformed) - rows_remaining} rows complete, {rows_remaining} remain. {upserts} Upserts complete, {int(rows_remaining/100)} to go')
            if len(data_transformed) - start <= 100:
                for item in data_transformed[start:]:
                    item['LastChecked'] = datetime.now(ZoneInfo('America/New_York'))
                self.centralstore.checked_upsert('acu.SalesOrders', data_transformed[start:])
                bp = 'here'
            bp = 'here'
        else:
            for item in data_transformed:
                item['LastChecked'] = datetime.now(ZoneInfo('America/New_York'))
            self.centralstore.checked_upsert('acu.SalesOrders', data_transformed)
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
