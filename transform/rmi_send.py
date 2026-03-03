import polars as pl
import logging


class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
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
        return data_formatted