from pipelines import Pipeline
import polars as pl
from connectors import AcumaticaAPI
import json
import time
class ShipmentsReadyToConfirm(Pipeline):
    '''`ShipmentsReadyToConfirm`(Pipeline:)
    ---
    <hr>

    Pipeline to confirm any **Open** Shipments that are **fully packed**    

    # Extraction
     - Extracts data using :attr:`~connectors.sql.AcumaticaDbQueries.ShipmentsReadyToConfirm` query

    # Transformation
     - Transforms :attr:`~connectors.sql.AcumaticaDbQueries.ShipmentsReadyToConfirm` result to a list of dictionaries, each containing ShipmentNbr


    # Load
     - Confirms Shipments coming from :meth:`~transform` via :meth:`~connectors.acu_api.AcumaticaAPI.confirm_shipment`

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
    '''
    def __init__(self):
        super().__init__('shipment-confirmations')
        self.acu_api = AcumaticaAPI(self)


    def extract(self):
        '''
        `extract`()
        ---
        <hr>
        
        Extracts data using :attr:`~connectors.sql.AcumaticaDbQueries.ShipmentsReadyToConfirm` query
        
        ### Downstream Function Calls 
         #### :meth:`~connectors.sql.SQLConnector.query_to_dataframe`
            - Pass query that returns all Open Shipments that have a Tracking Number and are ready to be confirmed, returns polars DataFrame
        
        <hr>
        
        Returns
        ---
        :return `data_extract` (pl.DataFrame): Shipments that can be confirmed in Acumatica
        '''
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.ShipmentsReadyToConfirm)
        return data_extract
    
    def transform(self, data_extract: pl.DataFrame):
        '''
        `transform`(self, data_extract: *pl.DataFrame*)
        ---
        <hr>
    
        Transforms :attr:`~connectors.sql.AcumaticaDbQueries.ShipmentsReadyToConfirm` result to a list of dictionaries, each containing ShipmentNbr
        
        <hr>
        
        Parameters
        ---
        :param (*pl.DataFrame*) `data_extract`: result of the :attr:`~connectors.sql.AcumaticaDbQueries.ShipmentsReadyToConfirm` query
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (list): list of dictionaries, each containing a single kvp pair {'ShipmentNbr': '123456'}

         - 'ShipmentNbr': *each Shipment number in data_extract*
        '''
        data_transformed = data_extract.to_series().to_list()
        data_transformed = [{'ShipmentNbr': s} for s in data_transformed]
        return data_transformed
    
    def load(self, data_transformed: list):
        '''
        `load`(self, data_transformed: *list*)
        ---
        <hr>
        
        Confirms Shipments coming from :meth:`~transform`
        
        ### Downstream Function Calls 
         #### :meth:`~connectors.acu_api.AcumaticaAPI.confirm_shipment`
            - Given a dictionary containing ShipmentNbr, confirms Shipment in Acumatica
        
        <hr>
        
        Parameters
        ---
        :param (*list*) `data_transformed`: list of dictionaries, each containing a single kvp pair {'ShipmentNbr': '123456'}
        
        <hr>
        
        Returns
        ---
        :return `self.acu_api.data_log` (list): Data to send to :meth:`~connectors.sql.SQLConnector.checked_upsert`
        '''
        for shipment in data_transformed:
            self.logger.info(f'Sleeping 3 seconds')
            time.sleep(3)
            self.acu_api.confirm_shipment(shipment)
        return self.acu_api.data_log
    
    def log_results(self, data_loaded):
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in data_loaded:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', data_loaded)
        pass