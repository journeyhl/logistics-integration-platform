
import requests
import logging
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time
class Teams:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.teams')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.teams')
        pass


    def _auth(self):
        bp = 'here'