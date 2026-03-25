
from typing import Any

from pipelines import Pipeline


class ShipmentsReadyToConfirm(Pipeline):
    def __init__(self):
        super().__init__('shipment_confirmations')


    def extract(self):
        data_extract = self.acudb.query_db(self.acudb.queries.ShipmentsReadyToConfirm.query)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = data_extract
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = []
        for shipment in data_transformed:
            data_loaded.append(self.acu_api.confirm_shipment(shipment))
        return data_loaded
    
    def log_results(self) -> Any:
        pass