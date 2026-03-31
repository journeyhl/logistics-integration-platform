

from pipelines import Pipeline
from connectors import RMIAPI, SQLConnector
from transform.rmi_receipt_pull import Transform


class GetClosedShipmentsFromRMI(Pipeline):
    '''
===

Hits RMI's *ClosedShipmentsV1* endpoint

Upserts to **rmi_ClosedShipments**
    '''
    def __init__(self):
        super().__init__('rmi-shipments')
        self.rmi = RMIAPI(self)
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.rmi.closed_shipments()
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform_closed_shipments(data_extract)
        return data_transformed


    def load(self, data_transformed):
        data_loaded = self.centralstore.checked_upsert('rmi_ClosedShipments', data_transformed)
        bp = 'here'
        return data_transformed

    def log_results(self, data_loaded):
        pass