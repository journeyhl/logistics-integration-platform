from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.address_validator import AddressValidator
import logging
import time
import polars as pl

class Load:
    '''Load
    ===
    <hr>    

    Class for smart handling of Acumatica API interactions 
    
    '''
    def __init__(self, pipeline: AddressValidator):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.load')

    def landing(self, data_transformed: list):
        for order_avs in data_transformed:
            self.logger.info(f'{order_avs['OrderNbr']}: Beginning target run')
            light_payload = {
                'key': f'{order_avs['OrderNbr']}',
                'target_api_update_payload': order_avs['update_order_address_payload'],
                'log_update_error': f"Issue Overriding & Updating {order_avs['OrderNbr']}aqqqqqqqqqqqqqqqqqqqqqqqqq en and updated successfully!",
                'log_validation_error': f"Issue validating {order_avs['OrderNbr']}'s Addresses",
                'log_validation_success': f"{order_avs['OrderNbr']}'s Addresses were validated successfully!",
                'acu_api_data_log': order_avs['acu_api_log_update_override'],
            }
            order_avs['validate_address'] = self.pipeline.acu_api.target_api(endpoint='/SalesOrder', payload_data=light_payload, operation='put', descr='Override & Update')
            bp = 'here'
            # order_avs['validate_address'] = self.pipeline.acu_api.update_customer_address(order_avs['update_address_payload'])
            if order_avs['validate_address']:
                self.pipeline.acu_api.validate_order_address(order_avs)
                time.sleep(1)
                self.pipeline.acu_api.order_remove_hold(order_avs)
                time.sleep(1)
                self.pipeline.acu_api.order_create_shipment(order_avs)
                bp = 'here'