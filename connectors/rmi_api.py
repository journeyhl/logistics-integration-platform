import requests
import logging
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from config.settings import RMI

class RMIAPI():

    def __init__(self, pipeline):
        """`init`(self, pipeline: *Pipeline | str*)
        ---
        <hr>
        
        Initializes RMIAPI connector and Authenticates
            
        <hr>
        
        Parameters
        ---
        :param (*Pipeline | str*) `pipeline`: Pipeline the connector belongs to
        
        <hr>
        
        Sets
        ---
        >>> self.logger = logging.getLogger(f'{pipeline.pipeline_name}.rmi_api')
        >>> self.base_uri = 'https://api.backtracksrl.com/'
        >>> self.auth_type = 'Token'
        >>> self.headers = {
            'Content-Type': 'application/json', 
            'ident': '64A648DD-E186-42E1-8A46-23D76A401FF0'
        }

        >>> self.username = RMI['username']
        >>> self.password = RMI['password']
        >>> self.session = requests.Session()
        >>> self._auth()
        
        **self._auth** Authenticates using creds from :data:`~config.settings.RMI`
        """
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.rmi_api')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.rmi_api')
        self.base_uri = 'https://api.backtracksrl.com/'
        self.auth_type = 'Token'
        self.headers = {
            'Content-Type': 'application/json', 
            'ident': '64A648DD-E186-42E1-8A46-23D76A401FF0'
        }

        self.username = RMI['username']
        self.password = RMI['password']
        self.session = requests.Session()
        self._auth()
        pass

    def _auth(self):
        '''`_auth`(self)
        ===
        <hr>
        
        Logins into RMI's API. If response, gets a token and sets **self.token** 
        '''
        auth_url = f'{self.base_uri}Auth/Login'
        body = {
            "name": self.username,
            "password": self.password 
        }

        try:
            response = self.session.post(
                url = auth_url,
                headers = self.headers,
                json=body
            )
            j_response = json.loads(response.text)
            self.token = j_response['token']
            bp = 'here'
            self.logger.info('RMI API is online. Logged into RMI and authenticated successfully')
        except Exception as e:
            self.logger.critical(f'Could not login to RMI API!')
            raise


    def target_api(self, endpoint: str, payload: list, lookback_window_days: int = 21):
        bp = 'here'
        url = f'{self.base_uri}api/{endpoint}'
        from_date = datetime.today() - timedelta(days=lookback_window_days)
        from_date = from_date.date().strftime('%Y-%m-%dT%H:%M:%SZ')
        to_date = datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%dT%H:%M:%SZ')
        headers = {
            **self.headers, 
            "Accept": "application/json",
            "authorization": f"Bearer {self.token}",
            "User-Agent": "axios/1.12.2",
            "Content-Length": "71",
            "Accept-Encoding": "gzip, compress, deflate, br",
        }
        body = {
            payload[0]: from_date,
            payload[1]: to_date
        }
        try:
            response = self.session.post(
                url = url,
                headers = headers,
                json = body
            )
            json_response = json.loads(response.text)
            bp = 'here'
            return json_response
        except Exception as e:
            bp = 'here'


    def get_rma(self, rma_number):
        url = f'{self.base_uri}api/RMA'
        from_date = datetime.today() - timedelta(days=180)
        from_date = from_date.date().strftime('%Y-%m-%dT%H:%M:%SZ')
        to_date = datetime.now(ZoneInfo('America/New_York')).strftime('%Y-%m-%dT%H:%M:%SZ')
        headers = {
            **self.headers, 
            "Accept": "application/json",
            "authorization": f"Bearer {self.token}",
            "User-Agent": "axios/1.12.2",
            "Content-Length": "71",
            "Accept-Encoding": "gzip, compress, deflate, br",
        }
        body = {
            "fromDate": from_date,
            "toDate": to_date
        }
        try:
            response = self.session.post(
                url = url,
                headers = headers,
                json = body,
                params={'RMANumber': f'{rma_number}'}
            )
            json_response = json.loads(response.text)
            if json_response == {'message': 'Bad Request', 'status': 400}:
                self.logger.error(f'{json_response['status']} ERROR: {json_response['message']}')
            bp = 'here'
            return json_response
        except Exception as e:
            bp = 'here'

