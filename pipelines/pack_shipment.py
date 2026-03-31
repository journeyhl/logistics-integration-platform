from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.pack_shipment import Transform
from load.shipment_api import Load
import json

class PackShipment(Pipeline):
    def __init__(self):
        super().__init__('pack_shipment')
        self.acu_api = AcumaticaAPI(self)
        self.transformer = Transform(self)
        self.loader = Load(self)
        


    def extract(self):
        central_extract = self.centralstore.query_db(self.centralstore.queries.PackShipment.query)
        redstag_event_extract = self.centralstore.query_db(self.centralstore.queries.RedStagEvents.query)
        acu_extract = self.acudb.query_db(self.acudb.queries.PackShipment.query)
        data_extract = {
            'central_extract': central_extract,
            'redstag_event_extract': redstag_event_extract,
            'acu_extract': acu_extract
        }
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        self.loader.load_shipments(data_transformed)
        return self.acu_api.data_log
    
    def log_results(self, data_loaded):
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in data_loaded:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', data_loaded)
        pass
