
from typing import Any

from pipelines import Pipeline
from connectors import RedStagAPI, AcumaticaAPI
from transform.redstag_inventory import Transform



class RedStagInventory(Pipeline):
    '''`RedStagInventory`(Pipeline:)
    ---
    <hr>

    Pipeline to retrieve inventory levels from RedStag 3PL through their API and load to db_CentralStore (**RedstagInventorySummary** and **RedstagInventoryDetail**)
    
    # Extraction
     - Extracts detailed inventory data from RedStag via :class:`~connectors.redstag_api.RedStagAPI`.:meth:`~connectors.redstag_api.RedStagAPI.target_api`

    # Transformation
     - Transforms response from RedStag to a dictionary containing two lists of dicts for upsert to **RedstagInventorySummary** and **RedstagInventoryDetail**

    # Load
     - Loads Inventory Summary and Detail level data to **RedstagInventorySummary** and **RedstagInventoryDetail** via :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.checked_upsert`
     
    # Results Logging
     - None needed
    '''

    def __init__(self):
        super().__init__('redstag-inventory')
        self.transformer = Transform(self)
        self.redstag = RedStagAPI(self)
        self.acu_api = AcumaticaAPI(self)
        self.payload_target = [
            "inventory.detailed",
            [
                None, #Specific SKUS
                None  #Updated Since value. If populated, will only return values updated since that date.
            ]
        ]
        # self.loader = Load(self)

    def extract(self):
        data_extract = self.redstag.target_api(payload_target=self.payload_target, operation='inventory.detailed')
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform_inventory(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        summary = data_transformed['item_summary']
        self.centralstore.checked_upsert(table_name='RedstagInventorySummary', data=summary)
        detail = data_transformed['item_detail']
        self.centralstore.checked_upsert(table_name='RedstagInventoryDetail', data=detail)
        
        return data_transformed
    
    def log_results(self, data_loaded):
        pass
