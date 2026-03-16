from pipelines import Pipeline
from connectors import RMIAPIConnector, SQLConnector
from transform.rmi_receipt_pull import Transform

class GetReceiptsFromRMI(Pipeline):
    def __init__(self):
        super().__init__('rmi_receipts')
        self.rmi = RMIAPIConnector(self)
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.rmi.get_receipts()
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform_receipts(data_extract)
        return data_transformed
    

    def load(self, data_transformed):
        data_loaded = self.centralstore.checked_upsert('rmi_Receipts', data_transformed)
        return data_transformed
    
    def log_results(self):
        pass