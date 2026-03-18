if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from pipelines import Pipeline
import polars as pl
from transform.create_acu_receipt import Transform
from load.shipment_api import Load

class CreateAcuReceipt(Pipeline):
    def __init__(self):
        super().__init__('create_receipts')
        self.transformer = Transform(self)
        self.loader = Load(self)
        

    def extract(self):
        central_extract = self.centralstore.query_db(self.centralstore.queries.ReturnsPendingReciept.query)
        acu_extract = self.acudb.query_db(self.acudb.queries.OpenRCsNoReceipt.query)
        data_extract = {
            'central_extract': central_extract,
            'acu_extract': acu_extract
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        data_transformed = self.transformer.transform(data_extract)       
        return data_transformed
    
    def load(self, data_transformed):

        data_loaded = []
        self.loader.load(data_transformed)
        return data_loaded
    
    def log_results(self, data_loaded):
        return data_loaded



if __name__ == '__main__':
    test = CreateAcuReceipt()
    tester = test.run()
    bp = 'here'