from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.sales_order_cleaner import SalesOrderCleaner
import logging
import polars as pl
class Transform:
    def __init__(self, pipeline: SalesOrderCleaner):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract: pl.DataFrame):
        '''`transform`(self, data_extract: *pl.DataFrame*)
        ---
        <hr>
        
        Formats `query_where` from **data_extract**

        Queries AcumaticaDb using `query_where`

        Join both DataFrames on OrderNbr
        
        ### Downstream Calls 
         #### :meth:`~.clean`
            - Iterates through the joined DataFrame and adds each Order to its respective status in `tracking_dict`
            - For each item in the populated `tracking_dcit`, hit :meth:`~.parse_orders` to format each Status's deletion command and a count of the orders in that status
            
        <hr>
        
        Parameters
        ---
        :param (*pl.DataFrame*) `data_extract`: Orders from db_CentralStore that we need to clean
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (dict): **deletions_dict** (Each Status, containing its respective delete command and a count of the orders in that status)
        '''
        query_where = f"where s.CompanyID = 2 and s.OrderNbr in('{"', '".join([d['OrderNbr'] for d in data_extract.to_dicts()])}')"
        query = f"select s.OrderNbr, j.Status, j.cStatus from SOOrder s inner join jjStatusLookup j on s.Status = j.cStatus and j.Tbl = 'SOOrder' {query_where}"
        acu_extract = self.pipeline.acudb.query_db(query=query)
        self.combo_extract = data_extract.join(acu_extract, on = 'OrderNbr', how='inner', suffix='_acu')
        data_transformed = self.clean()
        bp = 'here'
        return data_transformed


    def clean(self) -> dict:
        '''`clean`()
        ---
        <hr>
        
        Iterates through the joined DataFrame and adds each Order to its respective status in `tracking_dict`

        For each item in the populated `tracking_dcit`, hit :meth:`~.parse_orders` to format each Status's deletion command and a count of the orders in that status
        
        <hr>
        
        Returns
        ---
        :return `deletions_dict` (dict): Each Status, containing its respective delete command and a count of the orders in that status

        '''
        tracking_dict = {
            'Awaiting Payment': [],
            'Back Order': [],
            'Canceled': [],
            'Completed': [],
            'On Hold': [],
            'Open': [],
            'Pending Approval': [],
            'Risk Hold': [],
            'Shipping': [],
        }
        for order in self.combo_extract.iter_rows(named = True):
            tracking_dict[order['Status_acu']].append(order['OrderNbr'])
            bp = 'here'
        bp = 'here'

        deletions_dict = {}
        for status_key, order_list in tracking_dict.items():
            if len(order_list) > 0:
                deletions_dict[status_key] = self.parse_orders(status_key, order_list)
        return deletions_dict

    
    def parse_orders(self, status_key: str, order_list: list) -> dict:
        '''`parse_orders`(status_key: *_type_*, order_list: *_type_*, )
        ---
        <hr>
        
        Generates `data_dict`, or each Status's deletion command and count of orders in a dictionary
            
        <hr>
        
        Parameters
        ---
        :param (*str*) `status_key`: Status of all orders in `order_list`
        :param (*list*) `order_list`: List of OrderNbrs in a particular status
        
        <hr>
        
        Returns
        ---
        :return `data_dict` (dict): dict containing Status's deletion command and count of orders
        '''
        query_where = f"where Status != '{status_key}' and OrderNumber in('{"', '".join(order_list)}')"
        delete_cmd = f"delete from acu.SalesOrders {query_where}"
        data_dict = {
            'delete_cmd': delete_cmd,
            'orders_in_list': len(order_list)
        }
        return data_dict