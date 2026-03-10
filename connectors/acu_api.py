from config.settings import ACUMATICA_API
import requests
import logging
import json

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
            self.parse_response(response, {'type': 'Shipment', 'attribute': 'AttributeSHP2WH'})
        except Exception as e:
            bp = 'handle this'
        return self.status_description, body


    def rc_sent_to_wh(self, OrderNbr, OrderType, CustomerID):
        body = {
            "CustomerID": { "value": f"{CustomerID}" },
            "OrderType": {"value": f"{OrderType}"},
            "OrderNbr": { "value": f"{OrderNbr}"},
            "custom": {
                "Document": {
                    "AttributeRCSHP2WH": {
                        "value": True
                    }
                }
            } 
        }
        try:
            response = self.session.put(f'{self.base_uri}/SalesOrder', json=body)
            self.parse_response(response, {'type': 'Order', 'attribute': 'AttributeRCSHP2WH'})
            return self.status_description, body
        except Exception as e:
            bp = 'here'
            raise




    def parse_response(self, response: requests.Response, entity_type: dict):
        status_code = response.status_code
        if status_code == 200:
            self.acu_response = json.loads(response.text)
            for key, value in self.acu_response.items():
                if "{'value'" in str(value):
                    self.acu_response[key] = value['value']
                    bp = 'here'
                else:
                    self.acu_response[key] = value

            is_sent = self.acu_response['custom']['Document'][entity_type['attribute']]['value']
            if self.acu_response['Status'] != 'Open':
                self.status_description = f'{self.acu_response[f'{entity_type['type']}Nbr']} is {self.acu_response['Status']}! SentToWH = {is_sent}'
                self.logger.error(self.status_description)
            elif is_sent:
                self.status_description = f'{self.acu_response[f'{entity_type['type']}Nbr']} has been sent!' 
                self.logger.info(self.status_description)
            else:
                self.status_description = f'{self.acu_response[f'{entity_type['type']}Nbr']} was not sent and is in Open status!'
                self.logger.error(self.status_description)
        else:
            
            bp = 'here'
        



    def _logout(self):
        self.session.post('https://erp.journeyhl.com/entity/auth/logout')
        self.logger.info('Logged out of Acumatica API session')