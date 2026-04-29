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
        self._set_endpoints
        pass


    def _set_endpoints(self):
        self.tracking_endpoint = 'https://api.aftership.com/tracking/2026-01/trackings'




    def post_data(self, endpoint: str, payload: dict):
        try:
            response = self.session.post(url = endpoint, headers = self.headers, json = payload)
            self.logger.info(f'Successfully posted to Aftership')
        except Exception as e:
            self.logger.error(f'Error getting response from Aftership (post_data)')
        try:
            jresponse = response.json()            
            return jresponse
        except:
            self.logger.error(f'Error parsing response from Aftership (post_data)')
        return {}