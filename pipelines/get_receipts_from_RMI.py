from pipelines import Pipeline
from connectors import RMIAPI, SQLConnector
from transform.rmi_receipt_pull import Transform

class GetReceiptsFromRMI(Pipeline):
    '''`GetReceiptsFromRMI`(Pipeline)
    ---
    <hr>

    Hits RMI's *Receipts* endpoint, retrieves all Receipts and upsert to **rmi_Receipts** in db_CentralStore

    # Extraction
     - Extract ClosedShipments data via :class:`~connectors.rmi_api.RMIAPI`.:meth:`~connectors.rmi_api.RMIAPI.target_api`

    # Transformation
     - Transforms extracted data into format needed for upsert to **rmi_Receipts**

    # Load
     - Upserts data to **rmi_Receipts** via :meth:`~connectors.sql.SQLConnector.checked_upsert`

    # Logging
     - None needed
    '''
    def __init__(self):
        '''`init`(self)
        ---
        <hr>
        
        Initializes GetReceiptsFromRMI Pipeline 
        
        Sets
        ---
        >>> self.rmi = RMIAPI(self)
        >>> self.transformer = Transform(self)
        >>> self.payload_template = ["fromDate", "toDate"]
        '''
        super().__init__('rmi-receipts')
        self.rmi = RMIAPI(self)
        self.transformer = Transform(self)
        self.payload_template = ["fromDate", "toDate"]
        


    def extract(self):
        data_extract = self.rmi.target_api('Receipts', self.payload_template)
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform_receipts(data_extract)
        return data_transformed
    

    def load(self, data_transformed):
        total = len(data_transformed)
        self.logger.info(f'{total} rows to upsert')
        data_loaded = self.centralstore.checked_upsert_paginated('rmi_Receipts', data_transformed)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass