from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines import SendHubSpotOrderData
from config.settings import HUBSPOT
import requests
import logging

class HubSpotAPI:
    def __init__(self, pipeline: SendHubSpotOrderData):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.hubspot_api')
        self.base_url = 'https://api.hubapi.com'
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {HUBSPOT["access_token"]}',
            'Content-Type': 'application/json',
        })
        self.logger.info('[AUTH]  HubSpot session initialized.')