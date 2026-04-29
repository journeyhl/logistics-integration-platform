
from pipelines.base import Pipeline
from connectors import AfterShip, AcumaticaAPI
from transform.aftership import Transform

class UpdateAfterShip(Pipeline):
    def __init__(self):
        super().__init__('aftership-update')
        self.aftership = AfterShip(self)
        self.acuapi = AcumaticaAPI
        self.transformer = Transform(self)
        pass



    def extract(self):
        data_extract = {
            'slugs_extract': self.centralstore.query_db('select * from SlugsAfterShip'),
            'shipment_extract': self.acudb.query_to_dataframe(self.acudb.queries.Aftership_Shipments),
            'aftership_extract': self.aftership.retrieve_trackings()
        }
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform_update(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        for i, row in enumerate(data_transformed):
            prefix = f'{i+1}/{len(data_transformed)}, {len(data_transformed)} to go: '
            self.logger.info(f'{prefix}Posting data to aftership for {row['OrderNbr']}-{row['ShipmentNbr']}-{row['Tracking']}')
            self.aftership.post_data(self.aftership.tracking_endpoint, row['formatted'])
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
