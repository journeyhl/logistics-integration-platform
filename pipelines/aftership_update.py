
from pipelines.base import Pipeline
from connectors import AfterShip, AcumaticaAPI
from transform.aftership import Transform

class UpdateAfterShip(Pipeline):
    '''`UpdateAfterShip`(Pipeline)
    ---
    <hr>

    Pipeline to update Aftership data if found to be outdated compared to what is extracted from Acumatica

    # Extraction
     - Three data sources are used in extraction:
        - **slugs_extract**: Pulls Slugs from CentralStore, not being used as of 5/6/26
            - Query: *select * from SlugsAfterShip*
        - **shipment_extract**: Extract Shipments that have tracking data from AcumaticaDb
            - Query: **Aftership_Shipments**
        - **aftership_extract**: Retrieves tracking data from AfterShip via :class:`~connectors.aftership.AfterShip`.:meth:`~connectors.aftership.AfterShip.retrieve_trackings`       

    # Transformation
     - For each row in the response back from Aftership, determine if the **shipment_tags** or **customer** information differs from that of our **shipment_extract** query
     - If a difference is found, the updated data is to be sent back to Aftership
     
    # Load
     - Send any rows having a discrepancy between Acu and Aftership to Aftership for update via :class:`~connectors.aftership.AfterShip`.:meth:`~connectors.aftership.AfterShip.put_data`

    # Results Logging
     - None needed
    '''
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
            'aftership_extract': self.aftership.retrieve_trackings(pipeline_name= self.pipeline_name)
        }
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform_update(data_extract)
        return data_transformed
    

    def load(self, data_transformed):
        for i, (id, values) in enumerate(data_transformed.items()):
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
