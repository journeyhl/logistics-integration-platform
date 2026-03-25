from pipelines import Pipeline
import polars as pl
from connectors import AcumaticaAPI
import json
import time
class ShipmentsReadyToConfirm(Pipeline):
    def __init__(self):
        super().__init__('shipment_confirmations')
        self.acu_api = AcumaticaAPI(self)


    def extract(self):
        data_extract = self.acudb.query_db(self.acudb.queries.ShipmentsReadyToConfirm.query)
        return data_extract
    
    def transform(self, data_extract: pl.DataFrame):
        data_transformed = data_extract.to_series().to_list()
        data_transformed = [{'ShipmentNbr': s} for s in data_transformed]
        return data_transformed
    
    def load(self, data_transformed):
        for shipment in data_transformed:
            self.logger.info(f'Sleeping 5 seconds')
            time.sleep(5)
            self.acu_api.confirm_shipment(shipment)
        return self.acu_api.data_log
    
    def log_results(self, data_loaded):
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in data_loaded:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', data_loaded)
        pass