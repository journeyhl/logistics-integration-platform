import logging
import polars as pl
class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass
    
    def transform(self, data_extract: pl.DataFrame):
        bp = 'here'
        for row in data_extract.iter_rows(named=True):
            return_nbr = row['KeyValue']
            bp = 'here'