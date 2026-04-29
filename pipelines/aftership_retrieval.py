
from pipelines.base import Pipeline
from connectors import AfterShip, AcumaticaAPI
from transform.aftership_retrieval import Transform

class AfterShipRetrieval(Pipeline):
    def __init__(self):
        super().__init__('aftership-retrieval')
        self.aftership = AfterShip(self)
        self.acuapi = AcumaticaAPI
        self.transformer = Transform(self)
        pass



    def extract(self):
        data_extract = self.aftership.retrieve_trackings()
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        for i, row in enumerate(data_transformed):
            prefix = f'{i+1}/{len(data_transformed)}, {len(data_transformed)} to go: '
            self.logger.info(f'{prefix}Posting data to aftership for {row['OrderNbr']}-{row['ShipmentNbr']}')
            self.aftership.post_data(self.aftership.tracking_endpoint, row['formatted'])
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
