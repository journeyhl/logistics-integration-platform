from pipelines import Pipeline
from connectors import ODATAConnector, SQLConnector, RMIXMLConnector
from transform.rmi_send import Transform
class SendToRMI(Pipeline):
    def __init__(self):
        super().__init__('SendToRMI')
        self.url = 'https://erp.journeyhl.com/ODATA/JHL/JHL RMI Shipment API'
        self.odata_source = ODATAConnector(self)
        self.transformer = Transform(self)
        self.rmi = RMIXMLConnector(self)
        
        

    # def extract_odata(self):
    #     data_extract = self.odata_source.get_data(self.url)
    #     return data_extract
    
    def extract(self):
        with open('sql/SendToRMI.sql', 'r') as f:
            self.query = f.read()
        data_extract = self.acudb.query_db(self.query)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    

    def load(self, data_transformed):
        data_loaded = self.rmi.initiate_send(data_transformed)
        return data_loaded