from pipelines import Pipeline
import polars as pl

class ShipmentsReadyToConfirm(Pipeline):
    def __init__(self):
        super().__init__('shipment_confirmations')


    def extract(self):
        data_extract = self.acudb.query_db(self.acudb.queries.ShipmentsReadyToConfirm.query)
        return data_extract
    
    def transform(self, data_extract: pl.DataFrame):
        data_transformed = data_extract.to_series().to_list()
        data_transformed = [{'ShipmentNbr': s} for s in data_transformed]
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = []
        for shipment in data_transformed:
            data_loaded.append(self.acu_api.confirm_shipment(shipment))
        return data_loaded
    
    def log_results(self):
        pass