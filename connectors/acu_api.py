from config.settings import ACUMATICA_API
import requests
class AcumaticaAPI:
    def __init__(self, pipeline):
        self.pipeline = pipeline
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
            bp = 'here'
        except Exception as e:
            bp = 'here'
        finally:
            self._logout()

    def _logout(self):
        self.session.post('https://erp.journeyhl.com/entity/auth/logout')