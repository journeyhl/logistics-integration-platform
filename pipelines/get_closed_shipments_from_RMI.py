

from pipelines import Pipeline
from connectors import RMIAPI, SQLConnector
from transform.rmi_receipt_pull import Transform


class GetClosedShipmentsFromRMI(Pipeline):
    '''`GetClosedShipmentsFromRMI`(Pipeline)
    ---
    <hr>

    Hits RMI's *ClosedShipmentsV1* endpoint, retrieves all Closed Shipments and upsert to **rmi_ClosedShipments** in db_CentralStore

    # Extraction
     - Extract ClosedShipments data via :class:`~connectors.rmi_api.RMIAPI`.:meth:`~connectors.rmi_api.RMIAPI.target_api`

    # Transformation
     - Transforms extracted data into format needed for upsert to **rmi_ClosedShipments**

    # Load
     - Upserts data to **rmi_ClosedShipments** via :meth:`~connectors.sql.SQLConnector.checked_upsert`

    # Results Logging
     - None needed
    '''
    def __init__(self):
        '''`init`(self)
        ---
        <hr>
        
        Initializes GetClosedShipmentsFromRMI Pipeline 
        
        Sets
        ---
        >>> self.rmi = RMIAPI(self)
        >>> self.transformer = Transform(self)
        >>> self.payload_template = ["fromDate", "toDate"]
        '''
        super().__init__('rmi-shipments')
        self.rmi = RMIAPI(self)
        self.transformer = Transform(self)
        self.payload_template = ["fromDate", "toDate"]


    def extract(self):
        data_extract = self.rmi.target_api('ClosedShipmentsV1', self.payload_template)
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