
import requests
import logging
import json
from datetime import datetime, timedelta

class RMIAPIConnector():

    def __init__(self, pipeline):
        self.pipeline = pipeline
        try:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
        except Exception as e:
            self.logger = logging.getLogger()
        self.uri = 'https://api.backtracksrl.com/'
        self.auth_type = 'Token'
        self.headers = {
            'Content-Type': 'application/json', 
            'ident': '64A648DD-E186-42E1-8A46-23D76A401FF0'
        }

        self.username = RMI['username']
        self.password = RMI['password']
        self.token_test = '0%8SAB9b1QCQ3R2g'

        self._auth()
        pass

    def _auth(self):
        auth_url = f'{self.uri}Auth/Login'
        body = {
            "name": self.username,
            "password": self.password 
        }

        try:
            response = requests.post(
                url = auth_url,
                headers = self.headers,
                json=body
            )
            j_response = json.loads(response.text)
            self.token = j_response['token']
            bp = 'here'
        except Exception as e:
            bp = 'here'


    def closed_shipments(self):
        url = f'{self.uri}api/ClosedShipmentsV1'
        from_date = datetime.today() - timedelta(days=3)
        from_date = from_date.date().strftime('%Y-%m-%dT%H:%M:%SZ')
        to_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
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
            response = requests.post(
                url = url,
                headers = headers,
                json = body
            )
            json_response = json.loads(response.text)
            bp = 'here'
        except Exception as e:
            bp = 'here'



    def get_receipts(self):
        url = f'{self.uri}api/Receipts'
        from_date = datetime.today() - timedelta(days=20)
        from_date = from_date.date().strftime('%Y-%m-%dT%H:%M:%SZ')
        to_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
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
            response = requests.post(
                url = url,
                headers = headers,
                json = body
            )
            json_response = json.loads(response.text)
            bp = 'here'
            return json_response
        except Exception as e:
            bp = 'here'



if __name__ == '__main__':
    import sys
    from pathlib import Path
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from config.settings import RMI
    rmi = RMIAPIConnector('woohoo')
    test1 = rmi.get_receipts()
    test = rmi.closed_shipments()
    bp = 'here'
else:    
    from config.settings import RMI