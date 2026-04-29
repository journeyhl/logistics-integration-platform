
from pipelines.base import Pipeline
from connectors import AfterShip, AcumaticaAPI
from transform.aftership_send import Transform

class SendToAfterShip(Pipeline):
    def __init__(self):
        super().__init__('aftership-send')
        self.aftership = AfterShip(self)
        self.acuapi = AcumaticaAPI
        self.transformer = Transform(self)
        pass



    def extract(self):
        data_extract = {
            'slugs_extract': self.centralstore.query_db('select * from SlugsAfterShip'),
            'shipment_extract': self.acudb.query_to_dataframe(self.acudb.queries.Aftership_Shipments)
        }
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
