from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines import SendHubSpotOrderData
from config.settings import CRITEO
import logging

class HubSpotAPI:
    def __init__(self, pipeline: SendHubSpotOrderData):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.hubspot_api')
