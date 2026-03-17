from config.settings import ACUMATICA_API
import requests
import logging
import json
from datetime import datetime, timedelta
class AcumaticaAPI:
    def __init__(self, pipeline):
        if type(pipeline) == str:            
            self.logger = logging.getLogger(f'{pipeline}.acu_api')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
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

#region SalesOrder
    def sales_order_create_receipt(self, order_data):
        self.logger.info(f'Creating Receipt for {order_data['OrderNbr']}')
        body = {
            "entity":{
                "CustomerID": { "value": f"{order_data['AcctCD']}" },
                "OrderType": {"value": f"{order_data['OrderType']}"},
                "OrderNbr": { "value": f"{order_data['OrderNbr']}"}
            }
        }
        try:
            response = self.session.post(f'{self.base_uri}/SalesOrder/SalesOrderCreateReceipt', json=body)
            bp = 'here'
        except Exception as e:
            bp = 'here'
        bp = 'here'

    def sales_order_get_shipment(self, order_data):
        self.logger.info(f'Checking for any shipments on {order_data['OrderNbr']}')
        try:
            response = self.session.get(f'{self.base_uri}/SalesOrder/{order_data['OrderType']}/{order_data['OrderNbr']}?$expand=Shipments')
            order_info = response.json()
        except Exception as e:
            order_info = None
            self.logger.error(f'Error getting {order_data['OrderNbr']}')
        
        if order_info:
            self.logger.info(f'{order_data['OrderNbr']} parsed successfully. Status: {order_info['Status']['value']}, Shipments: {len(order_info['Shipments'])}')
            if len(order_info['Shipments']) > 1:
                shipment = next((shipment for shipment in order_info['Shipments']
                                    if datetime.strptime(shipment['LastModifiedDateTime']['value'][:-6], '%Y-%m-%dT%H:%M:%S.%f') >= datetime.now() - timedelta(minutes=10)
                                    ), None)
            elif len(order_info['Shipments']) != 0:
                shipment = order_info['Shipments'][0]
            else:
                shipment = None
            if shipment:
                shipment_data = {
                    'ShipmentNbr': shipment['ShipmentNbr'].get('value'),
                    'CustomerID': order_info['CustomerID'].get('value'),
                    'Description': order_info['Description'].get('value'),
                    'ExtRefNbr': order_info['ExternalRef'].get('value'),
                    'OrderNbr': order_data['OrderNbr'],
                    'OrderType': order_data['OrderType']
                }
                self.logger.info(f'{shipment['ShipmentNbr'].get('value')} found for {order_data['OrderNbr']}')
                return shipment_data
            else:
                self.logger.warning(f'No Shipment found for {order_data['OrderNbr']}')
                
        shipment_data = {
            'ShipmentNbr': None,
            'CustomerID': None,
            'Description': None,
            'ExtRefNbr': None,
            'OrderNbr': order_data['OrderNbr'],
            'OrderType': order_data['OrderType']
        }
        return shipment_data
    
    
    def rc_sent_to_wh(self, OrderNbr, OrderType, CustomerID):
        '''rc_sent_to_wh`(self, OrderNbr, OrderType, CustomerID)`
        ===
        
        * Marks an *Order*'s **RC Ship to Warehouse** attribute to true. (AttributeRCSHP2WH)

        * API Method: **PUT**
        
        Parameters
        ---
        <hr>

            __OrderNbr__ (str): OrderNbr of Order to update (AR078365)
            __OrderType__ (str): OrderType of Order to update (RC)
            __CustomerID__ (str): CustomerID or AcctCD of Customer on Order (C0090306, C0067451)
        
        Returns
        ---
        <hr>

            __self.status_description__ (str): Details of interaction with Acumatica API
            __body__ (dict): Dictionary of what was sent to Acumatica API

        '''
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
        except Exception as e:
            self.status_description = 'FAILURE'
            bp = 'here'
        return self.status_description, body
    #endregion SalesOrder




    #region Shipment

    def shipment_details(self, shipment_data: dict):
        '''shipment_details`(self, shipment_data)`
        ===
        * Gets Customer details

        * API Method: **GET**

        Parameters
        ---
        <hr>

            __shipment_data__ (dict): Dictionary containing details of Shipment
        
        Returns
        ---
        <hr>

            __response.json()__ (dict): Parsed Dictionary of response from API
        '''
        try:
            response = self.session.get(f'{self.base_uri}/Shipment/{shipment_data['ShipmentNbr']}?$expand=Details,Packages')
            response_details = self.parse_shipment_details(shipment_data, response)
            # if response_details['package_count'] == 0:
            bp = 'here'
        except Exception as e:
            self.logger.error(f'Error getting packages for {shipment_data['ShipmentNbr']} ({shipment_data['OrderNbr']})')
        bp = 'here'

    def add_package(self, shipment_data):
        bp = 'here'

    def sent_to_wh(self, ShipmentNbr, CustomerID):
        '''sent_to_wh`(self, ShipmentNbr, CustomerID)`
        ===
        * Marks a *Shipment*'s **Ship to Warehouse** attribute to true. (AttributeSHP2WH)

        * API Method: **PUT**

        Parameters
        ---
        <hr>

            __ShipmentNbr__ (str): ShipmentNbr of Shipment to update
            __CustomerID__ (str): CustomerID or AcctCD of Customer on Shipment (C0090306, C0067451)
        
        Returns
        ---
        <hr>

            __self.status_description__ (str): Details of interaction with Acumatica API
            __body__ (dict): Dictionary of what was sent to Acumatica API

        '''
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
            self.status_description = 'FAILURE'
        return self.status_description, body

    #endregion Shipment



#region Utility
    def parse_shipment_details(self, shipment_data, response):
        try:
            response = response.json()
        except Exception as e:
            self.logger.error(f'Could not parse response!')
            return {
                'data': response,
                'status': 'Error'
            }
        package_count = response['PackageCount']['value']
        line_count = len(response['Details'])
        details = [{
            'LineNbr': line['LineNbr']['value'],
            'InventoryCD': line['InventoryID']['value'],
            'LineNbr': line['LineNbr']['value'],
        } for line in response['Details']]

        shipment_details = {
            **shipment_data,
            'package_count': package_count,
            'line_count': line_count,
            'details': details
        }
        self.logger.info(f'{response['ShipmentNbr']['value']} - Packages: {package_count}. Lines: {line_count}')
        return shipment_details

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
            self.status_description = 'FAILURE'
            bp = 'here'
#endregion Utility


#region Customer/Contact
    def customers(self, query=None, limit=100):
        '''customers`(self, query=None, limit=100)`
        ===
        * Gets Customer details

        * API Method: **GET**

        Parameters
        ---
        <hr>

            __query__ (str | None): Query to filter response (default = None)
            __limit__ (int): Number of results to limit to (default = 100)
        
        Returns
        ---
        <hr>

            __response.json()__ (dict): Parsed Dictionary of response from API
        '''
        params = {
            "$expand": "MainContact,MainContact/Address",
            "$select": "CustomerID,CustomerName,CustomerClass,MainContact/Email,"
                       "MainContact/Phone1,MainContact/Address/AddressLine1,"
                       "MainContact/Address/AddressLine2,MainContact/Address/City,"
                       "MainContact/Address/State,MainContact/Address/PostalCode,"
                       "CreatedDateTime,LastModifiedDateTime",
            "$top": str(limit),
        }
        if query:
            params["$filter"] = query
        response = self.session.get(f'{self.base_uri}/Customer', params=params)
        response.raise_for_status()
        return response.json()

    def contact(self, query=None, limit=100):
        '''contact`(self, query=None, limit=100)`
        ===
        * Gets Customer Contact details

        * API Method: **GET**

        Parameters
        ---
        <hr>

            __query__ (str | None): Query to filter response (default = None)
            __limit__ (int): Number of results to limit to (default = 100)
        
        Returns
        ---
        <hr>

            __response.json()__ (dict): Parsed Dictionary of response from API
        '''
        params = {
            "$expand": "Address",
            "$select": "ContactID,FirstName,LastName,Email,Phone1,Phone2,"
                    "JobTitle,Status,BusinessAccount,"
                    "Address/AddressLine1,Address/AddressLine2,"
                    "Address/City,Address/State,Address/PostalCode",
            "$filter": query,
            "$top": str(limit),
        }
        if query:
            params["$filter"] = query
        response = self.session.get(f'{self.base_uri}/Contact', params=params)
        response.raise_for_status()
        return response.json()
    #endregion Customer/Contact


#region Authentication/Logout
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

    def _logout(self):
        self.session.post('https://erp.journeyhl.com/entity/auth/logout')
        self.logger.info('Logged out of Acumatica API session')
#endregion Authentication/Logout
