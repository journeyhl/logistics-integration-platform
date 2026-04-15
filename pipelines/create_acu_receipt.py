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
    '''`CreateAcuReceipt`(Pipeline:)
    ---
    <hr>

    Pipeline to create a Receipt for any RC type Orders in Acumatica that, from RMI, have an *RMAStatus* of **CLOSED** and a *DFStatus* of **RECEIVED**

    # Extraction
     - Queries CentralStore for any RMA orders with an RMAStatus of CLOSED and a DFStatus of RECEIVED
        - These orders should be Receipted in Acumatica if it's not already done so
     - Queries Acudb for any RC Orders that are pending Receipt creation

    # Transformation
     - Matches Orders across datasets to find any Acumatica Orders that are ready to be Receipted.

    # Load
     - ***For each*** matched order:
        - Check if it has a *Receipt*(Shipment) or not via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.sales_order_get_shipment`
        - If Receipt: 
            - Retrieve details via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.shipment_details`
            - Determine if we should add package or try to retrieve details
                - Add package via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.add_package`
                - Retrieve details via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.get_package_details`
        - If **NO** Receipt:
            - Create Receipt(shipment) via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.order_create_receipt`
            - Check for Shipment on Order via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.sales_order_get_shipment`
            - Retrieve details via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.shipment_details`
            - Add package via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.add_package`
        - For each line:
            - Check that the Reason Code is set to **RETURN**. If not, update via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.update_reason_code`
        - Check if Shipment is ready to be confirmed by verifying Shipment Details and Package Items and Quantities match
        - If ready, Confirm Shipment via :class:`~connectors.acu_api.AcumaticaAPI`.:meth:`~connectors.acu_api.AcumaticaAPI.confirm_shipment`

    # Results Logging
     - Upserts Acumatica API interactions to **_util.acu_api_log** 
    '''
    def __init__(self):
        super().__init__('create-receipts')
        self.acu_api = AcumaticaAPI(self)
        self.transformer = Transform(self)
        self.loader = Load(self)
        

    def extract(self):
        central_extract = self.centralstore.query_to_dataframe(query=self.centralstore.queries.ReturnsPendingReciept)
        acu_extract = self.acudb.query_to_dataframe(query=self.acudb.queries.OpenRCsNoReceipt)
        data_extract = {
            'central_extract': central_extract,
            'acu_extract': acu_extract
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        data_transformed = self.transformer.transform(data_extract)       
        return data_transformed
    
    def load(self, data_transformed):

        self.loader.load_receipts(data_transformed)
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