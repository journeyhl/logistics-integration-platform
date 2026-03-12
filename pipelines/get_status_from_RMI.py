if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from pipelines import Pipeline
from connectors import RMIAPIConnector
from transform.rmi_receipt_pull import Transform

class GetStatusFromRMI(Pipeline):
    def __init__(self):
        super().__init__('rmi_status')
        self.rmi = RMIAPIConnector(self)
        with open('sql/StatusCheckRMI.sql', 'r') as f:
            self.query = f.read()
        self.data = self.centralstore.query_db(self.query).to_series().to_list()
        self.transformer = Transform(self)

    def extract(self, RMANumber):
        data_extract = self.rmi.get_rma(RMANumber)
        return data_extract
    
    def transform(self, data_extract):
        if data_extract == {'message': 'Bad Request', 'status': 400}:
            return data_extract
        data_transformed = self.transformer.transform_status_records(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        if data_extract == {'message': 'Bad Request', 'status': 400}:
            return data_transformed
        data_loaded = self.centralstore.checked_upsert('rmi_RMAStatus', data_transformed)
        return data_loaded
    
    def log_results(self):
        pass
    


if __name__ == '__main__':
    test = GetStatusFromRMI()
    extracted = []
    transformed = []
    loaded = []
    for RMANumber in test.data:
        data_extract = test.extract(RMANumber)
        data_transformed = test.transform(data_extract)
        data_loaded = test.load(data_transformed)
        bp = 'here'

    