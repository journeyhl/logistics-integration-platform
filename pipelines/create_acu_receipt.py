if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from pipelines import Pipeline
import polars as pl

class CreateAcuReceipt(Pipeline):
    def __init__(self):
        super().__init__('create_receipts')
        

    def extract(self):
        data_extract = self.centralstore.query_db(self.centralstore.queries.ReturnsPendingReciept.query)
        bp = 'here'
        return data_extract

    def transform(self, data_extract: pl.DataFrame):
        data_transformed = data_extract.sql('select distinct RMANumber, CustomerRef from self')

        bp = 'here'
        return data_transformed
    
    def load(self, data_transformed):
        data_loaded = data_transformed
        return data_loaded
    
    def log_results(self, data_loaded):
        return data_loaded



if __name__ == '__main__':
    test = CreateAcuReceipt()
    tester = test.run()
    bp = 'here'