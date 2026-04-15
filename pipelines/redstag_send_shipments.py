from pipelines import Pipeline
from connectors import RedStagAPI, AcumaticaAPI
from transform.redstag_send import Transform
from load.load_redstag_send import Load
import json

class SendRedStagShipments(Pipeline):
    '''`SendRedStagShipments`(Pipeline:)
    ---
    <hr>

    Pipeline to send Open Shipments to RedStag via **SendRedStagShipments** query
    
    # Extraction
     - Extracts Shipments that are ready to be sent to RedStag
        - OrigOrderType != 'RC'
            - Not a Return order
        - Status not in('C', 'L', 'F', 'I')
            - Completed, Cancelled, Confirmed, Invoiced
        - AttributeSHP2WH = 0 
            - Not sent to Warehouse
        - **left(SiteCD, 7)** = *'RedStag'* (This covers REDSTAGSWT and REDSTAGSLC)
            - Warehouse is RedStag


    # Transformation
     - Transforms extracted data into different payloads that may be sent to RedStag via :class:`~connectors.redstag_api.RedStagAPI`, as well as the corresponding payloads to be sent to Acumatica via :class:`~connectors.acu_api.AcumaticaAPI`
     - For each row in data_extract:
        - Check to see if the current row's **ShipmentNbr** exists as a key in the dictionary we're using to hold each shipment's transformed data
        - If the ShipmentNbr doesnt exist, do a second check to verify we dont have a **rsOrderID** value on the row
        - If the rsOrderID doesn't exist, transform the payload needed to lookup an order at RedStag via their API in :meth:`~transform.redstag_send.Transform.transform_lookup_payload`
            - From :meth:`~transform.redstag_send.Transform.transform_lookup_payload`, send the formatted ***lookup*** payload to RedStag via :meth:`~connectors.redstag_api.RedStagAPI.target_api`
                - Parse the response in :meth:`~transform.redstag_send.Transform.transform_lookup_response` and return it
        - If the response from the lookup payload is an empty list, we'll send the Shipment. If not, it already exists and we'll stop here for that row.
        - The following is only if the order ***DOES NOT EXIST AT RedStag*** 
        - Next, create the payload needed to send the shipment data to RedStag via :meth:`~transform.redstag_send.Transform.transform_order_create_payload` (Acumatica Shipment = RedStag Order)
            - Determine if we need to do anything with the ShipVia value we send in :meth:`~transform.redstag_send.Transform._determine_shipvia`
        - Finally, add that row to our dictionary that holds each shipment's transformed data        
            >>> self.shipments_done[shipment_nbr] = {
                'ShipmentNbr': shipment_nbr,
                'CustomerID': customer_id,
                'lookup_payload': self.lookup_payload,
                'order_create_payload': self.order_create_payload,
                'execution_payload': self.lookup_payload if self.order_create_payload == None else self.order_create_payload,
                'execution_operation': f'{shipment_nbr}, order.' + f'{'search' if self.order_create_payload == None else 'create'}',
                'note': self.note
            }
        
    # Load
     - With the transformed shipment payloads, use :meth:`~connectors.redstag_api.RedStagAPI.target_api` to send the **execution_payload** and **execution_operation** values to determine our action
        - If the order_create_payload doesn't exist (aka the shipment was already at RedStag), we execute a lookup payload instead of a creation payload
    - If the `response['status']` value = ***'unable_to_process'** or **'new'***, we sent the data successfully and we'll try to mark that shipment as sent in Acumatica.
    - If our note values from data_transformed is 'Already at RedStag', we'll now try to mark that shipment as sent in Acumatica so it doesnt come through again.
    - Format the payload we'll need to send to Acumatica containing relevant attribute values in :class:`~transform.redstag_send.Transform`.:meth:`~transform.redstag_send.Transform.transform_acu_attribute_payload`
    - Drop the formatted payload to Acumatica via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.send_to_wh_v2`

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
    '''
    
    def __init__(self):
        super().__init__('redstag-send-shipments')
        self.transformer = Transform(self)
        self.redstag = RedStagAPI(self)
        self.acu_api = AcumaticaAPI(self)
        self.loader = Load(self)

    def extract(self):
        data_extract = self.acudb.query_to_dataframe(query=self.acudb.queries.SendRedStagShipments)
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



