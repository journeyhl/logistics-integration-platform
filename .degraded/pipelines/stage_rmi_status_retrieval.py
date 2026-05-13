from pipelines import Pipeline
import logging
import polars as pl


class StageRMIStatusRetrieval(Pipeline):
    '''`StageRMIStatusRetrieval`(Pipeline)
    ---
    <hr>

    Gets all recently sent Shipments & Returns and recently retrieved ClosedShipments and Receipts

    # Extraction
     - Gets all recently sent Shipments & Returns and recently retrieved ClosedShipments and Receipts from db_CentralStore

    # Transformation
     - Transforms extracted data into a list of distinct RMANumbers

    # Load
     - Skipped

    # Results Logging
     - None needed
    '''

    def __init__(self):
        super().__init__('rmi-status-staging')

    
    def extract(self):
        self.logger.info(f'Running StatusCheckRMI.sql in CentralStore')
        data_extract = self.centralstore.query_to_dataframe(self.centralstore.queries.StatusCheckRMI)
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
    