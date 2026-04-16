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

    # Clean
     - After deletions are loaded, check **acu.SalesOrders** and **acu.Shipments** against our deletion tables and reconcile.

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
        self.clean()
        return data_loaded
    
    def clean(self):
        cleaners = [
            {
                'name': 'sales_order_del',
                'table': 'acu.SalesOrders',
                'query': 'select s.* from acu.SalesOrders s inner join _util.SOOrderDeletions d on s.OrderNumber = d.OrderNbr',
                'delete_cmd': '''
                    delete from s
                    from acu.SalesOrders s
                    inner join _util.SOOrderDeletions d on s.OrderNumber = d.OrderNbr
                '''
            },
            {
                'name': 'sales_order_line_del',
                'table': 'acu.SalesOrders',
                'query': 'select s.* from acu.SalesOrders s inner join _util.SOLineDeletions d on s.OrderNumber = d.OrderNbr and s.LineNbr = d.LineNbr',
                'delete_cmd': '''
                    delete from s
                    from acu.SalesOrders s
                    inner join _util.SOLineDeletions d on s.OrderNumber = d.OrderNbr and s.LineNbr = d.LineNbr
                '''
            },
            {
                'name': 'ship_del',
                'table': 'acu.Shipments',
                'query': 'select s.* from acu.Shipments s inner join _util.SOShipmentDeletions d on s.ShipmentNbr = d.ShipmentNbr',
                'delete_cmd': '''
                    delete from s
                    from acu.Shipments s
                    inner join _util.SOShipmentDeletions d on s.ShipmentNbr = d.ShipmentNbr
                '''
            },
        ]
        for cleaner in cleaners:
            self.logger.info(f'Cleaning {cleaner['table']}')
            results = self.centralstore.query_db(cleaner['query'])
            rows = results.height
            self.logger.info(f'{rows} rows need to be deleted from {cleaner['table']} via {cleaner['name']} cleaner')
            if rows > 0:
                self.centralstore.raw_execute(cleaner['delete_cmd'])
                self.logger.info(f'{rows} rows were deleted from {cleaner['table']}')
            else:
                bp = 'here'

        bp = 'here'

    def log_results(self, data_loaded):
        pass