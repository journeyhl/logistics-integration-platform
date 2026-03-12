from pipelines import Pipeline
from connectors import SQLConnector
import json
from transform.rmi_send import Transform



class FormatData(Pipeline):
    def __init__(self, query_file: str):
        super().__init__('FormatData')
        self.query_file = query_file
        self.transformer = Transform(self)
        pass



    def extract(self):
        with open(self.query_file) as f:
            self.query = f.read()
        data_extract = self.acudb.query_db(self.query)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        return data_transformed