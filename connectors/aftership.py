from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.aftership_send import SendToAfterShip
import logging
import requests
from config.settings import AFTERSHIP

class AfterShip:
    def __init__(self, pipeline: SendToAfterShip):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.rmi_api')
        self.api_key = AFTERSHIP['api_key']
        self.headers = {
            "as-api-key": self.api_key,
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self._set_endpoints()
        pass


    def _set_endpoints(self):
        self.tracking_endpoint = 'https://api.aftership.com/tracking/2026-01/trackings'




    def post_data(self, endpoint: str, payload: dict):
        try:
            response = self.session.post(url = endpoint, headers = self.headers, json = payload)
            if response.status_code == 400:
                self.parse_bad_tracking_response(response.json(), payload)
                return {}
            else:
                self.logger.info(f'Successfully posted to Aftership')
        except Exception as e:
            self.logger.error(f'Error getting response from Aftership (post_data)')
        try:
            jresponse = response.json()
            self._parse_good_tracking_response(jresponse = jresponse)
            return jresponse
        except:
            self.logger.error(f'Error parsing response from Aftership (post_data)')
        return {}
    

    def _parse_good_tracking_response(self, jresponse: dict):
        response_code = jresponse['meta']['code']
        msg = jresponse['meta'].get('message')
        jresponse = jresponse['data']
        log_row = {
            'ShipmentNbr': jresponse['order_id'],
            'OrderNbr': jresponse['order_number'],
            'Tracking': jresponse['tracking_number'],
            'ResponseCode': response_code,
            'Message': msg
            
        }
        self.pipeline.centralstore.checked_upsert('_util.AfterShipLog', [log_row])
        bp = 'here'



    def parse_bad_tracking_response(self, jresponse: dict, payload: dict):
        response_code = jresponse['meta']['code']
        msg = jresponse['meta']['message']
        log_row = {
            'ShipmentNbr': payload['order_id'],
            'OrderNbr': payload['order_number'],
            'Tracking': payload['tracking_number'],
            'ResponseCode': str(response_code),
            'Message': msg            
        }
        self.logger.warning(f'{msg}: {payload['order_number']}-{payload['order_id']}-{payload['tracking_number']}')
        self.pipeline.centralstore.checked_upsert(table_name='_util.AfterShipLog', data=[log_row])
        bp = 'here'