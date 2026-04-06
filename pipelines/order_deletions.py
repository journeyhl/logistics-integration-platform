from . import Pipeline
from transform.notify_fulfillment_ops import Transform
import polars as pl

class SOOrderDeletions(Pipeline):
    def __init__(self):
        super().__init__('order-deletions')
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.SOOrderDeletions)
        return data_extract

    def transform(self, data_extract: pl.DataFrame):
        data_transformed = data_extract.to_dicts()
        return data_transformed
    
    def load(self, data_transformed: list):
        data_loaded = data_transformed
        if len(data_transformed) > 0:
            self.centralstore.checked_upsert('_util.SOOrderDeletions', data_loaded)
        else:
            self.logger.info(f'No rows to load to CentralStore')
        return data_loaded
    
    def log_results(self, data_loaded):
        pass