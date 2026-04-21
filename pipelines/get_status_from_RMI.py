if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from pipelines import Pipeline
from connectors import RMIAPI
from transform.rmi_receipt_pull import Transform

class GetStatusFromRMI(Pipeline):
    '''`GetStatusFromRMI`(Pipeline)
    ---
    <hr>

    After :class:`~pipelines.stage_rmi_status_retrieval.StageRMIStatusRetrieval` is run, re initialize GetStatusFromRMI for each RMANumber.

    # Extraction
     - Extract ClosedShipments data via :class:`~connectors.rmi_api.RMIAPI`.:meth:`~connectors.rmi_api.RMIAPI.get_rma`
     - This pulls the status for each RMANumber we send

    # Transformation
     - Transforms extracted data into format needed for upsert to **rmi_RMAStatus**

    # Load
     - Upserts data to **rmi_RMAStatus** via :meth:`~connectors.sql.SQLConnector.checked_upsert`

    # Logging
     - None needed
    '''
    def __init__(self):
        super().__init__('rmi-status')
        self.rmi = RMIAPI(self)
        # self.data = self.centralstore.query_db(self.centralstore.queries.StatusCheckRMI.query).to_series().to_list()
        self.transformer = Transform(self)

    def extract(self):  # type: ignore[override]
        data_extract = self.rmi.get_rma(self.rma_number)
        return data_extract
    
    def transform(self, data_extract):
        if data_extract == {'message': 'Bad Request', 'status': 400}:
            return data_extract
        data_transformed = self.transformer.transform_status_records([data_extract])
        return data_transformed
    
    def load(self, data_transformed):
        if data_transformed == {'message': 'Bad Request', 'status': 400}:
            return data_transformed
        data_loaded = self.centralstore.checked_upsert('rmi_RMAStatus', data_transformed)
        return data_transformed
    
    def _re_init(self, rma_number):
        self.rma_number = rma_number
        
    def log_results(self, data_loaded):
        pass
    


# if __name__ == '__main__':
#     test = GetStatusFromRMI()
#     extracted = []
#     transformed = []
#     loaded = []
#     for RMANumber in test.data:
#         test.logger.info(RMANumber)
#         data_extract = test.extract(RMANumber)
#         data_transformed = test.transform(data_extract)
#         data_loaded = test.load(data_transformed)
        
#         bp = 'here'

    