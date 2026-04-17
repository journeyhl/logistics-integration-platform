from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.kustomer import SendOrderDetailsToKustomer
import logging
import time
import polars as pl
import json
class Load:
    '''Load
    ===
    <hr>

    Class for smart handling of Acumatica API interactions 
    
    '''
    def __init__(self, pipeline: SendOrderDetailsToKustomer):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.load')


    def landing(self, data_transformed: list[dict]):
        data_filtered = []
        for order in data_transformed:
            json_order = json.dumps(order, default=str)
            order['payload'] = json.loads(json_order)
            check_query = f'''
select *
from K_OrderIngest k
where k.OrderNbr = {order['OrderNbr']} and k.AcuStatus = {order['OrderStatus']} and k.jsonData != cast({json_order} as nvarchar(max))
'''
            last_sent_payload = self.pipeline.centralstore.query_db(check_query)
            if last_sent_payload.height > 0:
                data_filtered.append(order)
                posted += 1



    def send_payloads(self, data_transformed: list[dict]):
        for order in data_transformed:

            bp = 'here'
