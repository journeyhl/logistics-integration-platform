from pipelines import Pipeline
from connectors import RedStagAPI, AcumaticaAPI
from transform.redstag_send import Transform
from load.load_redstag_send import Load
import json

class SendRedStagShipments(Pipeline):
    def __init__(self):
        super().__init__('redstag-send-shipments')
        self.transformer = Transform(self)
        self.redstag = RedStagAPI(self)
        self.acu_api = AcumaticaAPI(self)
        self.loader = Load(self)

    def extract(self):
        data_extract = self.acudb.query_db(self.acudb.queries.SendRedStagShipments.query)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    

    def load(self, data_transformed):
        data_loaded = {}
        if data_transformed:
            data_loaded['data_loaded'] = self.loader.send_shipments(data_transformed)
        else:
            self.logger.info(f'No Shipments to load')
            data_loaded['data_loaded'] = []
            
        data_loaded['api_data_log'] = self.acu_api.data_log
        return data_loaded
    
    def log_results(self, data_loaded):
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in data_loaded['api_data_log']:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', data_loaded['api_data_log'])
        pass



