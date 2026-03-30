
from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.audit_fulfillment import Transform

class AuditFulfillment(Pipeline):
    def __init__(self):
        super().__init__('audit_fulfillment')
        self.acu_api = AcumaticaAPI(self)
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.centralstore.query_db(self.centralstore.queries.AuditFulfillment.query)
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer
        self.acu_api.shipment_details
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
