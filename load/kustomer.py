from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.kustomer import SendOrderDetailsToKustomer
import logging
import time
import polars as pl
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
        self.logger.info(f'Filtering {len(data_transformed)} orders...')
        for order in data_transformed:
            json_order = json.dumps(order, default=str)
            order['payload'] = json.loads(json_order)
            json_order_escaped = json_order.replace("'", "''")
            check_query = f'''
select *
from json.K_OrderIngest k
where k.OrderNbr = %s and k.AcuStatus = %s and k.jsonData != cast(%s as nvarchar(max))
'''
            last_sent_payload = self.pipeline.centralstore.query_db(
                check_query, 
                params=(order['OrderNbr'], order['OrderStatus'], json_order),
                log_str= order['OrderNbr'])
            if last_sent_payload.height > 0:
                data_filtered.append(order)
        self.logger.info(f'Filtered {len(data_transformed)} orders to {len(data_filtered)}')

        sql_log = self.send_payloads(data_filtered)
        return sql_log




    def send_payloads(self, data_filtered: list[dict]):
        sql_log = []
        for order in data_filtered:
            payload_with_data ={
                'OrderNbr': order['OrderNbr'],
                'execution_payload': order['payload'],
                'log_update_success': f'{order['OrderNbr']} sent successfully!',
                'log_update_error': f'Failed to send {order['OrderNbr']}'
            }
            order['DatetimeSent'] = datetime.now(ZoneInfo('America/New_York'))
            response = self.pipeline.api.target_api(payload_data = payload_with_data, operation = 'post', descr = 'Send Order data to Kustomer')
            order['ResponseText'] = response.text
            sql_log.append(self.format_db_row(order))
        return sql_log


    def format_db_row(self, order):
        sql_row = {
            'OrderNbr': order['OrderNbr'],
            'jsonData': order['payload'],
            'AcuStatus': order['OrderStatus'],
            'DatetimeSent': order['DatetimeSent'],
            'ResponseText': order['ResponseText']
        }
        return sql_row
