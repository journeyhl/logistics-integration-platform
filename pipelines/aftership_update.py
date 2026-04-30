
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
    #870900964167
    def load(self, data_transformed):
        for i, (id, values) in enumerate(data_transformed.items()):
            if i < 664:
                continue
            prefix = f'{i+1}/{len(data_transformed)}, {len(data_transformed)} to go: '
            self.logger.info(f'{prefix}Putting data to aftership for {values['tracking']}')
            self.aftership.put_data(
                endpoint=f'{self.aftership.tracking_endpoint}/{id}', 
                params={'id':id}, 
                payload= values['payload'], 
                log_prefix=f'{values['order']}-{values['shipment']}-{values['tracking']}',
                extra={'ShipmentNbr': values['shipment'], 'OrderNbr': values['order'], 'Tracking': values['tracking']}
            )
            bp = 'here'
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
