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
        excluding = 0
        for order in data_transformed:
            json_order = json.dumps(order, default=str)
            order['payload'] = json.loads(json_order)
            json_order_escaped = json_order.replace("'", "''")
            check_query = f'''
select *
from K_OrderIngest k
where k.OrderNbr = %s and k.AcuStatus = %s and k.jsonData != cast(%s as nvarchar(max))
'''
            last_sent_check = self.pipeline.acudb.query_db(
                check_query, 
                params=(order['OrderNbr'], order['OrderStatus'], json_order), log_str='Log here not there'
            )
            if last_sent_check.height > 0:
                data_filtered.append(order)
                log_str = f'Will send {order['OrderNbr']}.         To send: {len(data_filtered)}'

            else:
                excluding += 1
                log_str = f'Excluding {order['OrderNbr']}.         Exclude: {excluding}'
            self.logger.info(log_str)
    
        self.logger.info(f'Total: {len(data_transformed)}')
        self.logger.info(f'Sending: {len(data_filtered)}')
        self.logger.info(f'Excluded: {excluding}')
        

        sql_log = self.send_payloads(data_filtered)
        return sql_log




    def send_payloads(self, data_filtered: list[dict]):
        sql_log = []
        for i, order in enumerate(data_filtered):
            payload_with_data ={
                'OrderNbr': order['OrderNbr'],
                'execution_payload': order['payload'],
                'log_update_success': f'{order['OrderNbr']} sent successfully! {len(data_filtered) - i - 1} orders remain',
                'log_update_error': f'Failed to send {order['OrderNbr']}. {len(data_filtered) - i - 1} orders remain'
            }
            order['DatetimeSent'] = datetime.now(ZoneInfo('America/New_York'))
            response = self.pipeline.api.target_api(payload_data = payload_with_data, operation = 'post', descr = 'Send Order data to Kustomer')
            order['ResponseText'] = response.text
            sql_log.append(self.format_db_row(order))
            if i % 10 == 0:
                self.pipeline.acudb.checked_upsert(f'K_OrderIngest', sql_log)
                sql_log.clear()
        return sql_log


    def format_db_row(self, order):
        sql_row = {
            'OrderNbr': order['OrderNbr'],
            'jsonData': order['payload'],
            'AcuStatus': order['OrderStatus'],
            'DatetimeSent': order['DatetimeSent'],
            'ResponseText': order['ResponseText'],
            'Status': order['Status'],
            'LastChecked': datetime.now(ZoneInfo('America/New_York'))
        }
        return sql_row
