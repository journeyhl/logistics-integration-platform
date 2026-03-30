if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))

from pipelines import Pipeline
import polars as pl
from transform.create_acu_receipt import Transform
from load.shipment_api import Load
from connectors import AcumaticaAPI
import json

class CreateAcuReceipt(Pipeline):
    '''CreateAcuReceipt
===
Queries CentralStore for any RMA orders with an RMAStatus of CLOSED and a DFStatus of RECEIVED
    * These orders should be Receipted in Acumatica if it's not already done so

Queries Acudb for any RC Orders that are pending Receipt creation

Matches Orders across datasets to find any Acumatica Orders that are ready to be Receipted.

*For each* Matched Order:
 * Check if it has a *Receipt*(Shipment) or not via *Acumatica API*
    * If Receipt, retrieve details via *Acu API*
    * If no Receipt, create one via *Acu API* then retrieve details
 * For *each line* on the Shipment:
    * Verify the **Reason Code** is set to **RETURN**. If not, update via *Acu API*
 * If there's no **Package** *or* the # of lines on the Package != Line Details, create Package
 * Verify Shipment Details and Package Items and Quantities match
 * If all checks are passed, Confirm Shipment

    '''
    def __init__(self):
        super().__init__('create_receipts')
        self.acu_api = AcumaticaAPI(self)
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

        self.loader.load(data_transformed)
        return self.acu_api.data_log
    
    def log_results(self, data_loaded):
        self.acu_api._logout()

        self.logger.info(f'Logging acu_api interactions...')
        for entry in data_loaded:
            entry['Payload'] = json.dumps(entry['Payload'])
        self.centralstore.checked_upsert('_util.acu_api_log', data_loaded)
        pass



if __name__ == '__main__':
    test = CreateAcuReceipt()
    tester = test.run()
    bp = 'here'