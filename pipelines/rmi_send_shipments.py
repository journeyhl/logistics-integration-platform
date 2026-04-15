from pipelines import Pipeline
from connectors import AcuOData, SQLConnector, RMIXML, AcumaticaAPI
from transform.rmi_send import Transform
import polars as pl
import json
class SendRMIShipments(Pipeline):
    '''`SendRMIShipments`(Pipeline:)
    ---
    <hr>

    Pipeline to send Open RC Sales Orders that have a AttributeRCSHP2WH value that's null or not equal to 1
    
    ***Sent as Type W***

    # Extraction
     - Pulls Shipments that are ready to be sent to RMI as type Ws
        - OrigOrderType != 'RC'
            - Order associated with Shipment is NOT a Return
        - Status not in('C', 'L', 'F', 'I')
            - Completed, Cancelled, Confirmed, Invoiced
        - AttributeSHP2WH = 0
            - Not sent to Warehouse
        -SiteCD = 'RMI'
            - Warehouse is RMI

    # Transformation
     - Creates a dictionary with RMANumber as key, then holds a list containing a dict with data for each row (line) that shipment has
        
    # Load
     - Format the payload we'll send to RMI through :class:`~connectors.rmi_xml.RMIXML`.:meth:`~connectors.rmi_xml.RMIXML.post_w`
        - To get the data for each line, we'll do so in :class:`~connectors.rmi_xml.RMIXML`.:meth:`~connectors.rmi_xml.RMIXML._format_w_lines`
     - Once formatted, post to RMI via :class:`~connectors.rmi_xml.RMIXML`.:meth:`~connectors.rmi_xml.RMIXML.post_w`

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
     - Inserts RMI XML interactions to **_util.rmi_send_log**
    '''
    '''SendShipments
===

Queries *AcumaticaDb* for any **Open Shipments** for RMI that have **NOT** been sent to the warehouse

Sends Shipment payload to RMI and upserts *_util.rmi_send_log*'''
    def __init__(self):
        super().__init__('rmi-send-shipments')
        self.url = 'https://erp.journeyhl.com/ODATA/JHL/JHL RMI Shipment API'
        self.odata_source = AcuOData(self)
        self.transformer = Transform(self)
        self.rmi = RMIXML(self)
        self.acu_api = AcumaticaAPI(self)
        
        
    
    def extract(self):
        data_extract = self.acudb.query_to_dataframe(self.acudb.queries.SendRMIShipments)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    

    def load(self, data_transformed):
        data_loaded = self.rmi.initiate_send(data_transformed)
        return data_loaded
    
    def log_results(self, data_loaded: list):
        if len(data_loaded) > 0:
            data_loaded = [{k: json.dumps(v) if isinstance(v, dict) else v for k, v in row.items()} for row in data_loaded]
            df_loaded = pl.DataFrame(data_loaded)
            df_loaded = df_loaded.with_columns(pl.lit('Shipment').alias('Type'))
            df_loaded = df_loaded.rename({'key': 'KeyValue', 'lines': 'Lines', 'rmi_response': 'RMI_Response', 'rmi_payload': 'RMI_Payload', 'acu_response': 'ACU_Response', 'timestamp': 'Timestamp'})
            df_loaded = df_loaded.select(['Type', 'KeyValue', 'Lines', 'RMI_Response', 'RMI_Payload', 'ACU_Response', 'Timestamp'])
            self.centralstore.insert_df(df_loaded, '_util.rmi_send_log')
        self.acu_api._logout()
        
        self.logger.info(f'Logging acu_api interactions...')
        for entry in self.acu_api.data_log:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', self.acu_api.data_log)
        pass