from pipelines import Pipeline
from connectors import AcumaticaAPI
from transform.cleaner import Transform
import polars as pl

class SalesOrderCleaner(Pipeline):    
    '''`SalesOrderCleaner`(Pipeline)
    ---
    <hr>
    
    Pipeline that looks for out of sync data in db_CentralStore and deletes any rows found
    
    # Extraction
     - Returns records from **acu.SalesOrders**, joined with itself on **OrderNumber = OrderNumber** and **Status *!=* Status**

    # Transformation
     - Transforms the extracted data into a string of OrderNbrs and queries AcumaticaDb for the current status of the orders that were extracted
     - Joins the *polars DataFrame* from AcumaticaDb and the original data_Extract on **OrderNbr**
     - Iterates through the joined DataFrame and adds each Order to its respective status in `tracking_dict`
     - For each item in the populated `tracking_dcit`, hit :meth:`~transform.cleaner.Transform.parse_orders` to format each Status's deletion command and a count of the orders in that status
        

    # Load
     - For each Status that has orders to clean, execute the `delete_cmd` in the dictionary that item contains

    # Results Logging
     - None needed
    '''
    def __init__(self):
        super().__init__('sales-orders-cleaner')
        self.transformer = Transform(self)


    def extract(self):
        data_extract = self.centralstore.query_to_dataframe(self.centralstore.queries.SalesOrderCleaner)
        return data_extract

    def transform(self, data_extract: pl.DataFrame):
        data_transformed = self.transformer.transform(data_extract)
        return data_transformed
    
    def load(self, data_transformed):
        self.logger.info(f'Cleaning out of sync orders in acu.SalesOrders...')
        data_loaded = data_transformed
        for status_key, data_dict in data_transformed.items():
            self.logger.info(f'{data_dict['orders_in_list']} orders out of sync. Deleting any rows not in {status_key} status.')
            self.centralstore.raw_execute(data_dict['delete_cmd'])
            bp = 'here'
        return data_loaded
    
    def log_results(self, data_loaded):
        pass
