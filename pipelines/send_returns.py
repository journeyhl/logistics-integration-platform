from pipelines import Pipeline
from connectors import SQLConnector, RMIXMLConnector
from transform.rmi_send import Transform
import polars as pl
class SendReturns(Pipeline):
    def __init__(self):
        super().__init__('rmi-send-returns')
        self.transformer = Transform(self)
        self.rmi = RMIXMLConnector(self)


    def extract(self):
        with open('sql/SendReturns.sql', 'r') as f:
            query = f.read()
        data_extract = self.acudb.query_db(query)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = self.rmi.initiate_send(data_transformed)
        return data_loaded
    
    def log_results(self, data_loaded: list):
        if len(data_loaded) > 0:
            df_loaded = pl.DataFrame(data_loaded)
            df_loaded = df_loaded.with_columns(pl.lit('Return').alias('Type'))
            df_loaded = df_loaded.rename({'key': 'KeyValue', 'lines': 'Lines', 'rmi_response': 'RMI_Response', 'rmi_payload': 'RMI_Payload', 'acu_response': 'ACU_Response', 'timestamp': 'Timestamp'})
            df_loaded = df_loaded.select(['Type', 'KeyValue', 'Lines', 'RMI_Response', 'RMI_Payload', 'ACU_Response', 'Timestamp'])
            self.centralstore.insert_df(df_loaded, 'rmi_send_log')
        else:
            self.logger.warning('Nothing was logged!')
        pass