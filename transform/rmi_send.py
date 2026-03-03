import polars as pl
import logging


class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract: pl.DataFrame):
        data_formatted = {}
        for row in data_extract.iter_rows(named=True):
            if data_formatted.get(row['RMANumber']) == None: 
                data_formatted[row['RMANumber']] = [row]   
                bp = 'here'
            else:
                data_formatted[row['RMANumber']].append(row)
                bp = 'here'
            bp = 'here'
        self.logger.info(f'Transformed {data_extract.height} rows to {len(data_formatted)} shipments')
        return data_formatted