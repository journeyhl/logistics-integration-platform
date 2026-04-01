from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.redstag_send_shipments import SendRedStagShipments
import logging
import time

class Load:
    '''Load
    ===
    <hr>

    Class for smart handling of Acumatica API interactions 
    
    '''
    def __init__(self, pipeline: SendRedStagShipments):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')

    def send_shipments(self, data_transformed):
        data_loaded = []
        for shipment, data in data_transformed.items():
            self.logger.info(f'Sending {shipment} to RedStag')
            self.create_response = self.pipeline.redstag.target_api(payload_target=data['order_create_payload'], operation=f'{shipment}, order.create')
            shipment_data = self.pipeline.acu_api.shipment_details(data)
            bp = 'here'
