
import requests
import logging
from config.settings import TEAMS

class Teams:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        if isinstance(pipeline, str):
            self.logger = logging.getLogger(f'{pipeline}.teams')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.teams')
        self.config = TEAMS
        self.webhook_url = TEAMS['webhook_url'] or ''
        self.tenant_id = self.config['tenant_id']
        self.client_id = self.config['client_id']
        self.client_secret = self.config['client_secret']
        self.user_id = self.config['user_id']
        self.sp_object_id = self.config['sp_object_id']
        self.webhook_url = TEAMS['webhook_url'] or ''

        if not self.webhook_url:
            raise ValueError('TEAMS_WEBHOOK_URL is not set')

    def send_message(self, message: str):
        self.logger.info('Sending Teams message...')
        resp = requests.post(self.webhook_url, json={'text': message})
        resp.raise_for_status()
        self.logger.info('Teams message sent')