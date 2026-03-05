from config.settings import ACUMATICA_API
import requests
import logging
class AcumaticaAPI:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
        self.version = '22.200.001'
        self.auth_type = 'Cookie'
        self.uri = 'https://erp.journeyhl.com/entity'
        self.endpoint_name = 'Containers'
        self.base_uri = f'{self.uri}/{self.endpoint_name}/{self.version}'
        self.username = ACUMATICA_API['username']
        self.password = ACUMATICA_API['password']
        self.company = 'JHL'
        self.session = requests.Session()
        self._auth()

    def _auth(self):
        auth_url = 'https://erp.journeyhl.com/entity/auth/login'
        body = {
            "name": self.username,
            "password": self.password,
            "company": self.company
        }
        try:
            response = self.session.post(url=auth_url, json=body)
            response.raise_for_status()
        except Exception as e:
            self._logout()

    def sent_to_wh(self, ShipmentNbr, CustomerID):
        body = {
            "CustomerID": { "value": f"{CustomerID}" },
            "ShipmentNbr": { "value": f"{ShipmentNbr}" },
            "custom": {
                "Document": {
                    "AttributeSHP2WH": {
                        "value": True
                    }
                }
            } 
        }
        try:
            response = self.session.put(f'{self.base_uri}/Shipment', json=body)
            status_code = response.status_code
            if status_code == 200:
                self.logger.info(f'{ShipmentNbr} marked as SentToWH successfully!')
            else:              
                self.logger.error(f'{ShipmentNbr} failed when updating SentToWH! Error {status_code}')
                
        except Exception as e:
            bp = 'handle this'
        return status_code, body


    def rc_sent_to_wh(self, OrderNbr, OrderType, CustomerID):
        body = {
            "CustomerID": { "value": {f"CustomerID"} },
            "OrderType": {"value": {f"OrderType"}},
            "OrderNbr": { "value": {f"OrderNbr"}},
            "custom": {
                "Document": {
                    "AttributeRCSHP2WH": {
                        "value": True
                    }
                }
            } 
        }
        try:
            response = self.session.put(f'{self.base_uri}/Order', json=body)
            status_code = response.status_code
        except Exception as e:
            bp = 'here'



    def _logout(self):
        self.session.post('https://erp.journeyhl.com/entity/auth/logout')
        self.logger.info('Logged out of Acumatica API session')