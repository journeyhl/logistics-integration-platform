from pipelines import Pipeline
from connectors import SQLConnector, RMIXML, AcumaticaAPI
from transform.rmi_send import Transform
import polars as pl
import json
class SendRMIReturns(Pipeline):
    '''`SendRMIReturns`(Pipeline:)
    ---
    <hr>

    Pipeline to send Open RC Sales Orders that have a AttributeRCSHP2WH value that's null or not equal to 1

    ***Sent as Type 3***
    
    # Extraction
     - Pulls all RC Sales Orders that are in Open status and have a AttributeRCSHP2WH value that is null or not equal to 1
        - Query: SendRMIReturns
    # Transformation
     - Creates a dictionary with RMANumber as key, then holds a list containing a dict with data for each row (line) that Order has
        
    # Load
     - Format the payload we'll send to RMI through :class:`~connectors.rmi_xml.RMIXML`.:meth:`~connectors.rmi_xml.RMIXML.post_3`
        - To get the data for each line, we'll do so in :class:`~connectors.rmi_xml.RMIXML`.:meth:`~connectors.rmi_xml.RMIXML._format_3_lines`
     - Once formatted, post to RMI via :class:`~connectors.rmi_xml.RMIXML`.:meth:`~connectors.rmi_xml.RMIXML.post_3`

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
     - Inserts RMI XML interactions to **_util.rmi_send_log**
    '''
    def __init__(self):
        super().__init__('rmi-send-returns')
        self.transformer = Transform(self)
        self.rmi = RMIXML(self)
        self.acu_api = AcumaticaAPI(self)


    def extract(self):
        data_extract = self.acudb.query_to_dataframe(query=self.acudb.queries.SendRMIReturns)
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
            df_loaded = df_loaded.with_columns(pl.lit('Return').alias('Type'))
            df_loaded = df_loaded.rename({'key': 'KeyValue', 'lines': 'Lines', 'rmi_response': 'RMI_Response', 'rmi_payload': 'RMI_Payload', 'acu_response': 'ACU_Response', 'timestamp': 'Timestamp'})
            df_loaded = df_loaded.select(['Type', 'KeyValue', 'Lines', 'RMI_Response', 'RMI_Payload', 'ACU_Response', 'Timestamp'])
            self.centralstore.insert_df(df_loaded, '_util.rmi_send_log')
        else:
            self.logger.warning('Nothing was logged!')
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in self.acu_api.data_log:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', self.acu_api.data_log)
        
        pass