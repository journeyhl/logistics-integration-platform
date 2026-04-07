from pipelines import Pipeline
from connectors import AddressVerificationSystem, AcumaticaAPI
import polars as pl
from transform.address_validator import Transform
import json
import time
class AddressValidator(Pipeline):
    def __init__(self):
        super().__init__('address-validator')
        self.avs = AddressVerificationSystem(self)
        self.acu_api = AcumaticaAPI(self)
        self.transformer = Transform(self)

    def extract(self):
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.ValidateAddresses)
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass