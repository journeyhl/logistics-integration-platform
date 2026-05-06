
from pipelines.base import Pipeline
from connectors import AfterShip, AcumaticaAPI
from transform.aftership import Transform
from datetime import timedelta

class AfterShipToDbc(Pipeline):
    '''`AfterShipToDbc`(Pipeline)
    ---
    <hr>

    Pipeline to retrieve data from AfterShip and upsert to **acu.AftershipExport** and **acu.AftershipExportDetail**
    
    # Extraction
     - **aftership_extract**: Retrieves tracking data updated within the last 90 days from AfterShip via :class:`~connectors.aftership.AfterShip`.:meth:`~connectors.aftership.AfterShip.retrieve_trackings`       

    # Transformation
     - For each row that is returned, format it accordingly for ***acu.AftershipExport***.
     - For each *checkpoint* in a row's **`checkpoints`** value, format the checkpoint data accordingly for ***acu.AftershipExportDetail***

    # Load
     - Upsert formatted lists of dicts to respective tables, **acu.AftershipExport** and **acu.AftershipExportDetail**, via :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.checked_upsert_paginated`

    # Results Logging
     - None needed
    '''
    def __init__(self):
        super().__init__('aftership-to-dbc')
        self.aftership = AfterShip(self)
        self.acuapi = AcumaticaAPI
        self.transformer = Transform(self)
        pass



    def extract(self):
        data_extract = self.aftership.retrieve_trackings(updated_window=timedelta(days=90), pipeline_name = self.pipeline_name)
        return data_extract

    def transform(self, data_extract):
        data_transformed = self.transformer.transform_aftership_to_db(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        aftership_export = data_transformed['aftership_export']
        aftership_export_detail = data_transformed['aftership_export_detail']
        self.centralstore.checked_upsert_paginated('acu.AftershipExportv2', aftership_export)
        self.centralstore.checked_upsert_paginated('acu.AftershipExportDetailv2', aftership_export_detail)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass
