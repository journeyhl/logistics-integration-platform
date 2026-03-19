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
    def sales_order_create_receipt(self, order_data: dict):
        '''sales_order_create_receipt`(self, order_data)`
    ===
    Creates Receipt for Open SalesOrder

    For **RC Orders only**


    Parameters
    -------------
    <hr>

        __order_data__ (dict): Dictionary of Order data. Must contain OrderType, OrderNbr, and AcctCD

    '''
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
            self.logger.info(f'{response.status_code} {response.reason}')
        except Exception as e:
            bp = 'here'


    def sales_order_get_shipment(self, order_data):
        '''sales_order_get_shipment`(self, order_data)`
    ===
    Returns Shipment details for a given *Sales Order*


    Parameters
    -------------
    <hr>

        __order_data__ (dict): Dictionary of Order data. Must contain OrderType and OrderNbr


    Returns
    -------------
    <hr>

        __shipment_data__ (dict): Formatted dict of Shipment data for order. If no shipment, Shipment data is None

    '''
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
        * Gets Shipment details for a given *Shipment*

        * API Method: **GET**

        Parameters
        ---
        <hr>

        __shipment_data__ (dict): Dictionary containing details of Shipment
        - Required

            - **ShipmentNbr**
        
        Returns
        ---
        <hr>

            __response.json()__ (dict): Parsed Dictionary of response from API
        '''
        self.logger.info(f'Retrieving Shipment Details from Acu API for {shipment_data['ShipmentNbr']}')
        try:
            response = self.session.get(f'{self.base_uri}/Shipment/{shipment_data['ShipmentNbr']}?$expand=Details/Allocations,Packages')
            response_details = self.parse_shipment_details(shipment_data, response)
            # if response_details['package_count'] == 0:
            return response_details
        except Exception as e:
            self.logger.error(f'Error getting packages for {shipment_data['ShipmentNbr']} ({shipment_data['OrderNbr']})')
            return {}
        bp = 'here'

    def add_package(self, shipment_data: dict):
        '''add_package`(self, shipment_data)`
    ===
    Given a Shipment, creates and packs a package


    Parameters
    -------------
    <hr>

    __shipment_data__ (dict): Shipment data. Must include Line details
    - Required

     - **ShipmentNbr**
     - **ExtRefNbr**
     - **details**
     - **details.InventoryCD**
     - **details.Qty**
     - **details.SplitLineNbr**


    Returns
    -------------
    <hr>

        __shipment_data__ (dict): Shipment data with package details

    '''
        self.logger.info(f'Adding package to {shipment_data['ShipmentNbr']}')
        now = datetime.now().strftime('%m/%d/%Y %H:%M:%S')
        descr = f'Package added via API @ {now}'
        body = {
            "ShipmentNbr": { "value": f"{shipment_data['ShipmentNbr']}" },
            "Packages": [
                {
                "BoxID": { "value": "DEFAULT BOX" },
                "TrackingNbr": { "value": f"{shipment_data['ExtRefNbr']}" },
                "Description": { "value": f"{descr}" },
                "Weight": { "value": 0 },
                "UOM": { "value": "LBS" },
                
                }
            ]
        }
        body['Packages'][0]['PackageContents'] = [
            {
                "InventoryID": { "value": line['InventoryCD'] },
                "Quantity": { "value": line['Qty'] },
                "UOM": { "value": "EA" },
                "ShipmentSplitLineNbr": { "value": line['SplitLineNbr'] }
            }
            for line in shipment_data['details']
        ]
        shipment_data = self.get_package_details(shipment_data, body)
        return shipment_data


    def get_package_details(self, shipment_data, body=None):
        '''get_package_details`(self, order_data)`
    ===
    Given a Shipment, returns Package details


    Parameters
    -------------
    <hr>

        __shipment_data__ (dict): Dictionary of Shipment data
        __body__ (dict): JSON payload sent to API


    Returns
    -------------
    <hr>

        __shipment_data__ (dict): shipment_data with Package data

         - Required: **ShipmentNbr**

    '''
        verb = 'added to'
        if body == None:
            verb = 'retrieved from'
            body = {
                "ShipmentNbr": { "value": f"{shipment_data['ShipmentNbr']}" },
            }
        response = self.session.put(url=f'{self.base_uri}/Shipment?$expand=Packages/PackageContents', json=body)
        json_response = response.json()
        if response.ok:            
            self.logger.info(f'Package {verb} {shipment_data['ShipmentNbr']}!')
            shipment_data = {**shipment_data, 'Packages': json_response['Packages'], 'package_count': json_response['PackageCount']['value']}
        if not response.ok:
            self.logger.error(f'get_package_details failed ({response.status_code}): {json_response['error']}')
        return shipment_data

    def confirm_shipment(self, shipment_data: dict):
        '''confirm_shipment`(self, shipment_data)`
    ===
    Given a Shipment in Open status, confirms it.


    Parameters
    -------------
    <hr>

        __shipment_data__ (dict): Dictionary of Shipment data.

         - Required: **ShipmentNbr**



    '''
        self.logger.info(f'Confirming Shipment {shipment_data['ShipmentNbr']}')
        body = {
            "entity": {
                "Type": {
                    "value": "Shipment"
                },
                "ShipmentNbr": {
                    "value": f"{shipment_data['ShipmentNbr']}"
                }
            }
        }
        response = self.session.post(url=f'{self.base_uri}/Shipment/ConfirmShipment', json=body)
        self.logger.info(f'{response.status_code} {response.reason}')
        bp = 'here'

    def update_reason_code(self, shipment_data: dict, line_data: dict):
        
        '''update_reason_code`(self, shipment_data, line_data)`
    ===
    Updates Reason Code on the line of a Shipment to *RETURN*


    Parameters
    -------------
    <hr>

        __shipment_data__ (dict): Shipment data
        __line_data__ (dict): For each line on Shipment, line_data is that line's data dict


    Returns
    -------------
    <hr>

        __line_data__ (dict): Line data with updated Reason Code

    '''
        self.logger.info(f'Updating ReasonCode on line {line_data['LineNbr']} of {shipment_data['ShipmentNbr']} from {line_data['ReasonCode']} to RETURN')
        body = {
            "ShipmentNbr": { "value": f"{shipment_data['ShipmentNbr']}" },
            "Details": [
                {
                "LineNbr":    { "value": line_data['LineNbr'] },
                "ReasonCode": { "value": "RETURN" }
                }
            ]
        }
        response = self.session.put(url=f'{self.base_uri}/Shipment', json=body)
        if response.ok:
            line_data['ReasonCode'] = 'RETURN'
        return line_data

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
            'Qty': line['ShippedQty']['value'],
            'SplitLineNbr': line['Allocations'][0]['SplitLineNbr']['value'],
            'ReasonCode': line['ReasonCode']['value'],
            'id': line['id']
        } for line in response['Details']]

        shipment_details = {
            **shipment_data,
            'Status': response['Status']['value'],
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
