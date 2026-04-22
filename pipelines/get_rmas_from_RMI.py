

from pipelines import Pipeline
from connectors import RMIAPI, SQLConnector
from transform.rmi_receipt_pull import Transform


class GetRMAsFromRMI(Pipeline):
    '''`GetRMAsFromRMI`(Pipeline)
    ---
    <hr>

    Hits RMI's *RMAs* endpoint, retrieves all RMAs and upsert to **rmi_RMAs** in db_CentralStore

    # Extraction
     - Extract RMAs data via :class:`~connectors.rmi_api.RMIAPI`.:meth:`~connectors.rmi_api.RMIAPI.target_api`

    # Transformation
     - Transforms extracted data into format needed for upsert to **RMAStatus**

    # Load
     - Upserts data to **RMAStatus** via :meth:`~connectors.sql.SQLConnector.checked_upsert`

    # Results Logging
     - None needed
    '''
    def __init__(self):
        '''`__init__`(self)
        ---
        <hr>
        
        Initializes GetRMAsFromRMI Pipeline 
        
        Sets
        ---
        >>> self.rmi = RMIAPI(self)
        >>> self.transformer = Transform(self)  
        >>> self.payload_template = ["lastModifiedDateFrom", "lastModifiedDateTo"]   
        '''
        super().__init__('rmi-rmas')
        self.rmi = RMIAPI(self)
        self.transformer = Transform(self)
        self.payload_template = ["lastModifiedDateFrom", "lastModifiedDateTo"]        


    def extract(self):
        data_extract = self.rmi.target_api(endpoint='RMAs', payload=self.payload_template, lookback_window_days=21)
        return data_extract
    
    def transform(self, data_extract):
        data_transformed = self.transformer.transform_status_records(data_extract)
        return data_transformed


    def load(self, data_transformed):
        data_loaded = self.centralstore.checked_upsert('rmi_RMAStatus', data_transformed)
        bp = 'here'
        return data_transformed

    def log_results(self, data_loaded):
        pass