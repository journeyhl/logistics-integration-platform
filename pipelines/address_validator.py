from pipelines import Pipeline
from connectors import AddressVerificationSystem, AcumaticaAPI
import polars as pl
from transform.address_validator import Transform
from load.address_validator import Load
import json
import time
class AddressValidator(Pipeline):
    def __init__(self):
        super().__init__('address-validator')
        self.avs = AddressVerificationSystem(self)
        self.acu_api = AcumaticaAPI(self)
        self.transformer = Transform(self)
        self.loader = Load(self)

    def extract(self) -> pl.DataFrame:
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.ValidateAddresses)
        return data_extract

    def transform(self, data_extract: pl.DataFrame) -> list:
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed: list):
        data_loaded = self.loader.landing(data_transformed)
        return self.acu_api.data_log
    
    def log_results(self, data_loaded):
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in data_loaded:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', data_loaded)
        pass