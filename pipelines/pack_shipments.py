from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.pack_shipment import Transform
from load.shipment_api import Load
import json

class PackShipments(Pipeline):
    '''`PackShipments`(Pipeline:)
    ---
    <hr>

    Pipeline to pack any Shipments that have tracking data from 3PLs
    
    # Extraction
     - Four data sources are queried:
        - **central_extract**: Query originally drove the RedStag Confirmations celigo flow. Pulls shipments from CentralStore using the acu.rs_ tables to determine which should be shipped
            - Query: **PackShipmentRedStag**
        - **redstag_event_extract**: Uses the json.RedStagEvents table to get all rows where json_value(jsonData, '$.topic') = 'shipment:packed'
            - Query: **RedStagEvents**
        - **rmi_extract**: Pulls closed Shipments from rmi_ClosedShipments
            - Query: **PackShipmentRMI**
        - **acu_extract**: Query from adf that populates acu.rsFulfill. Pulls Open Shipments without Tracking data
            - Query: **PackShipment**

    # Transformation
     - For each shipment in **acu_extract**, determine if a match can be found in the other data sources. If a match is found, format the payloads we'll need to drop to Acumatica

    # Load
     - In :meth:`~load.shipment_api.Load.load_shipments`, for each shipment ***that was matched***, hit the :class:`~connectors.acu_api.AcumaticaAPI` to get the Shipment's details (:meth:`~connectors.acu_api.AcumaticaAPI.shipment_details`)
     - In we need to add a package, add it and get details, if not, just get details of package.
        - :meth:`~connectors.acu_api.AcumaticaAPI.add_package_v2` and :meth:`~connectors.acu_api.AcumaticaAPI.get_package_details`

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
    '''

    def __init__(self):
        super().__init__('pack-shipments')
        self.acu_api = AcumaticaAPI(self)
        self.transformer = Transform(self)
        self.loader = Load(self)
        


    def extract(self):
        central_extract = self.centralstore.query_to_dataframe(query=self.centralstore.queries.PackShipmentRedStag)
        redstag_event_extract = self.centralstore.query_to_dataframe(query=self.centralstore.queries.RedStagEvents)
        rmi_extract = self.centralstore.query_to_dataframe(query=self.centralstore.queries.PackShipmentRMI)
        acu_extract = self.acudb.query_to_dataframe(query=self.acudb.queries.PackShipment)
        data_extract = {
            'central_extract': central_extract,
            'redstag_event_extract': redstag_event_extract,
            'rmi_extract': rmi_extract,
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
