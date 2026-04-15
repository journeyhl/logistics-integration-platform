from . import Pipeline
import polars as pl

class AcumaticaDeletions(Pipeline):
    '''`AcumaticaDeletions`(Pipeline)
    ---
    <hr>
    
    Pipeline to extract deleted records from **SOOrder**, **SOLine**, **SOOrderShipment** and **SOShipment** in *Acumatica* and load them to ***db_CentralStore***
    
    # Extraction
     - Returns records from **AcumaticaDb** that were deleted in Acumatica within the past two hours from tracked tables with triggers enabled
     - Tracked Tables:
        - **SOOrderDeletions**
        - **SOLineDeletions**
        - **SOShipmentDeletions**
        - **SOOrderShipmentDeletions**


    # Transformation
     - Transforms *polars DataFrames* retrieved by :meth:`~extract` to a list of dicts

    # Load
     - Using :class:`~connectors.sql.SQLConnector`.:meth:`~connectors.sql.SQLConnector.checked_upsert`, upsert to respective **_util** table
        - **SOOrderDeletions** -> ***_util.SOOrderDeletions***
        - **SOLineDeletions** -> ***_util.SOLineDeletions***
        - **SOShipmentDeletions** -> ***_util.SOShipmentDeletions***
        - **SOOrderShipmentDeletions** -> ***_util.SOOrderShipmentDeletions***

    # Results Logging
     - None needed
    '''
    def __init__(self):
        super().__init__('acumatica-deletions')


    def extract(self):
        data_extract = {
            'SOOrderDeletions': self.acudb.query_to_dataframe(self.acudb.queries.SOOrderDeletions),
            'SOLineDeletions': self.acudb.query_to_dataframe(self.acudb.queries.SOLineDeletions),
            'SOShipmentDeletions': self.acudb.query_to_dataframe(self.acudb.queries.SOShipmentDeletions),
            'SOOrderShipmentDeletions': self.acudb.query_to_dataframe(self.acudb.queries.SOOrderShipmentDeletions),
        }
        return data_extract

    def transform(self, data_extract: dict[str, pl.DataFrame]):
        data_transformed = {
            item: dataframe.to_dicts()
        for item, dataframe in data_extract.items()
        }

        return data_transformed
    
    def load(self, data_transformed: dict[str, list]):
        data_loaded = data_transformed
        for item, dict_list in data_transformed.items():
            if len(dict_list) > 0:
                self.centralstore.checked_upsert(f'_util.{item}', dict_list)
        else:
            self.logger.info(f'_util.{item}: No rows to load to CentralStore')
        return data_loaded
    
    def log_results(self, data_loaded):
        pass