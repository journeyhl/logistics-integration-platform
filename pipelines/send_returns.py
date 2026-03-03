
from pipelines import Pipeline
from connectors import SQLConnector, RMIXMLConnector
from transform.rmi_send import Transform


class SendReturns(Pipeline):

    def __init__(self):
        super().__init__('rmi-send-returns')
        self.transformer = Transform(self)
        self.rmi = RMIXMLConnector(self)


    def extract(self):
        with open('RMI-Returns.sql', 'r') as f:
            query = f.read()
        data_extract = self.acudb.query_db(query)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        return super().load()
    
    def log_results(self):
        return super().log_results()