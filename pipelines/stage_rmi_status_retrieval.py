from pipelines import Pipeline
import logging
import polars as pl


class StageRMIStatusRetrieval(Pipeline):
    def __init__(self):
        super().__init__('rmi_status_staging')

    
    def extract(self):
        self.logger.info(f'Running StatusCheckRMI.sql in CentralStore')
        data_extract = self.centralstore.query_db(self.centralstore.queries.StatusCheckRMI.query)
        return data_extract
    
    def transform(self, data_extract):
        self.logger.info('Transforming to list')
        data_transformed = data_extract.to_series().to_list()
        return data_transformed
    
    def load(self, data_transformed):
        self.logger.info('Skipping load, not neccesary here')
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        self.logger.info('Skipping logging, not neccesary here')
        pass
    