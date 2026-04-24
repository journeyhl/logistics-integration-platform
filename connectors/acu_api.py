from config.settings import ACUMATICA_API
import requests
import logging
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
class AcumaticaAPI:
    def __init__(self, pipeline):
        """`init`(self, pipeline: *Pipeline | str*)
        ---
        <hr>
        
        Initializes AcumaticaAPI connector and Authenticates

        <hr>
        
        Parameters
        ---
        :param (*Pipeline | str*) `pipeline`: Pipeline the connector belongs to
        
        <hr>
        
        Sets
        ---
        >>> self.logger = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
        >>> self.pipeline = pipeline
        >>> self.version = '22.200.001'
        >>> self.auth_type = 'Cookie'
        >>> self.uri = 'https://erp.journeyhl.com/entity'
        >>> self.endpoint_name = 'pyplatform'
        >>> self.base_uri = f'{self.uri}/{self.endpoint_name}/{self.version}'
        >>> self.username = ACUMATICA_API['username']
        >>> self.password = ACUMATICA_API['password']
        >>> self.company = 'JHL'
        >>> self.data_log = []
        >>> self.session = requests.Session()
        >>> self._auth() #logs into Acumatica API
        
        **self._auth** Authenticates using creds from :data:`~config.settings.ACUMATICA_API`
        """
        self.pipeline = pipeline    
        if type(pipeline) == str:            
            self.logger = logging.getLogger(f'{pipeline}.acu_api')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
        self.version = '22.200.001'
        self.auth_type = 'Cookie'
        self.uri = 'https://erp.journeyhl.com/entity'
        self.endpoint_name = 'pyplatform'
        self.base_uri = f'{self.uri}/{self.endpoint_name}/{self.version}'
        self.username = ACUMATICA_API['username']
        self.password = ACUMATICA_API['password']
        self.company = 'JHL'
        self.data_log = []
        self.session = requests.Session()
        self._auth()

#region SalesOrder
    def order_create_receipt(self, order_data: dict):
        '''order_create_receipt`(self, order_data)`
    ---
    <hr>

    Creates Receipt for Open SalesOrder

    For **RC Orders only**

    <hr>

    Parameters
    -------------

    :param (*dict*) `order_data`: Dictionary of Order data. Must contain OrderType, OrderNbr, and AcctCD

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
            response_str = f'{response.status_code} {response.reason}'
            
            self.logger.info(response_str)
        except Exception as e:
            bp = 'here'
        self.data_log.append({
            'Entity': 'SalesOrder',
            'KeyValue': order_data['OrderNbr'],
            'Operation': f'POST - Create Receipt',
            'Payload': body,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })

    def order_create_shipment(self, order_data: dict):
        '''`order_create_shipment`(self, order_data: *dict*)
        ---
        <hr>
        
        Creates Shipment for Open SalesOrder
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_data`: Dictionary of Order data. Must contain OrderType, OrderNbr, and AcctCD
        
        <hr>
        
        Returns
        ---
        '''
        self.logger.info(f'Creating Shipment for {order_data['OrderNbr']}')
        body = {
            "entity":{
                "CustomerID": { "value": f"{order_data['AcctCD']}" },
                "OrderType": {"value": f"{order_data['OrderType']}"},
                "OrderNbr": { "value": f"{order_data['OrderNbr']}"}
            }
        }
        try:
            response = self.session.post(f'{self.base_uri}/SalesOrder/SalesOrderCreateShipment', json=body)
            response_str = f'{response.status_code} {response.reason}'
        except Exception as e:
            bp = 'here'
        if response_str and response_str == '202 Accepted':
            self.logger.info(f'{order_data['OrderNbr']}: Shipment created! {response_str}')
        else:
            self.logger.warning(response_str)            
        self.data_log.append({
            'Entity': 'SalesOrder',
            'KeyValue': order_data['OrderNbr'],
            'Operation': f'POST - Create Shipment',
            'Payload': body,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })

    def sales_order_get_shipment(self, order_data):
        '''`sales_order_get_shipment`(self, order_data)
    ---
    <hr>
    Returns Shipment details for a given *Sales Order*

    <hr>

    Parameters
    -------------

    :param (*dict*) `order_data`: Dictionary of Order data. Must contain OrderType and OrderNbr

    <hr>

    Returns
    -------------

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
                                    if datetime.strptime(shipment['LastModifiedDateTime']['value'][:-6], '%Y-%m-%dT%H:%M:%S.%f') >= datetime.now(ZoneInfo('America/New_York')) - timedelta(minutes=10)
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
    
    
    def rc_send_to_wh(self, OrderNbr, OrderType, CustomerID):
        '''`rc_send_to_wh`(self, OrderNbr, OrderType, CustomerID)
        ---
        <hr>
        
        * Marks an *Order*'s **RC Ship to Warehouse** attribute to true. (AttributeRCSHP2WH)

        * API Method: **PUT**
        
        <hr>
        
        Parameters
        ---
        :param (*str*) `OrderNbr`: OrderNbr of Order to update (AR078365)
        :param (*str*) `OrderType`: OrderType of Order to update (RC)
        :param (*str*) `CustomerID`: CustomerID or AcctCD of Customer on Order (C0090306, C0067451)
        
        <hr>

        Returns
        ---

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
                    },
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
            try:                    
                self._logout()
                self._auth()
                response = self.session.put(f'{self.base_uri}/SalesOrder', json=body)
                self.parse_response(response, {'type': 'Order', 'attribute': 'AttributeRCSHP2WH'})
            except Exception as e:
                self.status_description = 'FAILURE'
                bp = 'here'
        self.data_log.append({
            'Entity': 'SalesOrder',
            'KeyValue': OrderNbr,
            'Operation': 'PUT - Mark RC Order as Sent To WH',
            'Payload': body,
            'Response': self.status_description,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        return self.status_description, body
    

    
    def validate_order_address(self, order_data: dict):
        """`validate_order_address`(self, order_data: *dict*)
        ---
        <hr>
        
        Given a dict containing **OrderType** and **OrderNbr**, attempts to validate a Sales Order's addresses
        
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_data`: dict containing **OrderNbr** and **OrderType**
        
        <hr>
        
        Returns
        ---
        """        
        payload = {
            "entity": {
                "Type": {
                    "value": "SalesOrder"
                },
                "OrderType": {
                    "value": order_data['OrderType']
                },
                "OrderNbr": {
                    "value": order_data['OrderNbr']
                }
            }
        }

        response = self.session.post(f'{self.base_uri}/SalesOrder/ValidateAddresses', json=payload)
        response_str = f'{response.status_code} {response.reason}'
        if response_str == '204 No Content':
            self.logger.info(f'{order_data['OrderNbr']}: Addresses validated!')
            response_str = '204 No Content (SUCCESS)'
        else:
            self.logger.error(f'{order_data['OrderNbr']}: Issue validating addresses!')

        self.data_log.append({
            'Entity': 'SalesOrder',
            'KeyValue': order_data['OrderNbr'],
            'Operation': f'POST - Validate Address',
            'Payload': payload,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        bp = 'here'



    def target_api(self, endpoint: str, payload_data: dict, operation: str = 'put', descr: str = None): #type: ignore
        """`target_api`(self, endpoint: *str*, payload_data: *dict*, operation: *str*, descr: *str*)
        ---
        <hr>
        
        Allows us to target the acumatica api with multiple endpoints in one method
        
        <hr>
        
        Parameters
        ---
        :param (*str*) `endpoint`: The endpoint to append to the base URI.
        :param (*dict*) `payload_data`: Dictionary containing **target_api_update_payload**, **log_update_success**, **log_update_error**, **acu_api_data_log**    
        :param (*str*) `operation`: API Operation (**PUT**, **POST**, **GET**)
        :param (*str*) `descr`: What the payload will do (**Override & Update**)

        <hr>
        
        Returns
        ---
        :return `return_bool` (bool): If **descr == 'Override & Update'**, return **True** if success, else **False**
        """        
        if operation == 'put':
            response = self.session.put(f'{self.base_uri}{endpoint}', json=payload_data['target_api_update_payload'])
        elif operation == 'post':
            response = self.session.post(f'{self.base_uri}{endpoint}', json=payload_data['target_api_update_payload'])
        response_str = f'{response.status_code}: {response.reason}'

        if descr == 'Override & Update':
            try:
                json_response = response.json()
                self.logger.info(payload_data['log_update_success'])
                return_bool = True
            except Exception as e:
                self.logger.info(payload_data['log_update_error'])
                return_bool = False
            self.data_log.append({
                **payload_data['acu_api_data_log'],
                'Response': response_str,
                'Timestamp': datetime.now(ZoneInfo('America/New_York'))
            })
            return return_bool
        

    def order_remove_hold(self, order_data: dict):
        self.logger.info(f'{order_data['OrderNbr']}: Removing Order from hold')
        bp = 'here'   
        payload = {
            "entity": {
                "Type": {
                    "value": "SalesOrder"
                },
                "OrderType": {
                    "value": order_data['OrderType']
                },
                "OrderNbr": {
                    "value": order_data['OrderNbr']
                }
            }
        }

        response = self.session.post(f'{self.base_uri}/SalesOrder/ReleaseFromHold', json=payload)
        response_str = f'{response.status_code} {response.reason}'        
        if response_str == '204 No Content':
            self.logger.info(f'{order_data['OrderNbr']} removed from hold successfully!')
            response_str = '204 No Content (SUCCESS)'
        else:
            self.logger.error(f'{order_data['OrderNbr']}: Issue removing from hold')
        
        self.data_log.append({
            'Entity': 'SalesOrder',
            'KeyValue': order_data['OrderNbr'],
            'Operation': f'POST - Remove Hold',
            'Payload': payload,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })

    #endregion SalesOrder




    #region Shipment

    def shipment_details(self, shipment_data: dict):
        '''shipment_details`(self, shipment_data)`
        ---
        <hr>

        * Gets Shipment details for a given *Shipment*

        * API Method: **GET**

        <hr>

        Parameters
        ---
        :param (*dict*) shipment_data: Dictionary containing details of Shipment

            - Required: **ShipmentNbr**
        
        <hr>

        Returns
        ---

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

    def shipment_details_attr(self, shipment_data: dict):
        '''shipment_details`(self, shipment_data)`
        ---
        <hr>

        * Gets Shipment details for a given *Shipment*, along with whether or not it was Sent to WH

        * API Method: **GET**
        
        <hr>

        Parameters
        ---

        :param (*dict*) `shipment_data`: Dictionary containing details of Shipment
        :type shipment_data: dict
        
        - Required
            - **ShipmentNbr**, **OrderNbr**

        <hr>
        
        Returns
        ---

            `response.json()` (dict): Parsed Dictionary of response from API
        '''
        self.logger.info(f'Retrieving Shipment Details from Acu API for {shipment_data['ShipmentNbr']}')
        try:
            response = self.session.get(f'{self.base_uri}/Shipment/{shipment_data['ShipmentNbr']}?$custom=Document.AttributeSHP2WH&$expand=Details/Allocations,Packages')
            response_details = self.parse_shipment_details(shipment_data, response)
            # if response_details['package_count'] == 0:
            return response_details
        except Exception as e:
            self.logger.error(f'Error getting packages for {shipment_data['ShipmentNbr']} ({shipment_data['OrderNbr']})')
            return {}
        
    def add_package(self, shipment_data: dict):
        '''add_package`(self, shipment_data: *dict*)`
        ---
        <hr>

        Given a Shipment, creates and packs a package

        <hr>

        Parameters
        ---
        :param (*dict*) `shipment_data`: Shipment data. Must include Line details

        Required:
        - **ShipmentNbr**
        - **ExtRefNbr** or **TrackingNbr**
        - **details**
        - **details.InventoryCD**
        - **details.Qty**
        - **details.SplitLineNbr**

        <hr>

        Returns
        -------------

            `shipment_data` (dict): Shipment data with package details

        '''
        self.logger.info(f'Adding package to {shipment_data['ShipmentNbr']}')
        now = datetime.now(ZoneInfo('America/New_York')).strftime('%m/%d/%Y %H:%M:%S')
        descr = f'Package added via API @ {now}'
        tracking_nbr = shipment_data['ExtRefNbr'] if shipment_data.get('ExtRefNbr') else shipment_data['TrackingNbr'] if shipment_data.get('TrackingNbr') else 'No ExtRefNbr for tracking'
        body = {
            "ShipmentNbr": { "value": f"{shipment_data['ShipmentNbr']}" },
            "Packages": [
                {
                "BoxID": { "value": "DEFAULT BOX" },
                "TrackingNbr": { "value": f"{tracking_nbr}" },
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


    def add_package_v2(self, shipment_data: dict):
        '''`add_package_v2`(self, shipment_data: *dict*):
        ---
        <hr>
        
        Revised version of add_package. Used in RedStag.

        Instead of formatting the package within function like `add_package`, format it beforehand and pass the completed payload inside *shipment_data*
        
        <hr>
        
        Parameters
        ----------
        
        :param `shipment_data`: Dictionary of Shipment data
        :type shipment_data: dict

        - Required: **ShipmentNbr**, **PackagePayload**'''
        
        self.logger.info(f'Adding package to {shipment_data['ShipmentNbr']}')
        shipment_data = self.get_package_details(shipment_data, shipment_data['PackagePayload'])
        bp = 'here'
        return shipment_data

    def get_package_details(self, shipment_data, body=None):
        '''`get_package_details`(self, shipment_data)
        ---
        <hr>
    
        Given a Shipment, returns Package details


        Parameters
        -------------
        <hr>

            `shipment_data` (dict): Dictionary of Shipment data
            `body` (dict): JSON payload sent to API


        Returns
        -------------
        <hr>

        `shipment_data` (dict): shipment_data with Package data
            - Required: **ShipmentNbr**
            '''
        verb = 'added to'
        now = ' now '
        if body == None:
            verb = 'retrieved from'
            now = ' '
            body = {
                "ShipmentNbr": { "value": f"{shipment_data['ShipmentNbr']}" },
            }
        response = self.session.put(url=f'{self.base_uri}/Shipment?$expand=Packages/PackageContents', json=body)
        json_response = response.json()
        if response.ok:            
            self.logger.info(f'Package data {verb} {shipment_data['ShipmentNbr']}!')
            shipment_data = {**shipment_data, 'Packages': json_response['Packages'], 'package_count': json_response['PackageCount']['value']}
            response_str = f'{response.status_code} {response.reason}. {shipment_data['ShipmentNbr']}{now}has {json_response['PackageCount']['value']} packages.'
            self.logger.info(response_str)
        if not response.ok:
            self.logger.error(f'get_package_details failed ({response.status_code}): {json_response.get('error')}')
            response_str = f'{response.status_code} {json_response.get('error')}'
            self.logger.warning(response_str)

        self.data_log.append({
            'Entity': 'Shipment',
            'KeyValue': shipment_data['ShipmentNbr'],
            'Operation': f'PUT: Package {verb} {shipment_data['ShipmentNbr']}!',
            'Payload': body,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        return shipment_data

    def confirm_shipment(self, shipment_data: dict):
        '''`confirm_shipment`(self, shipment_data: *dict* )
        ---
        <hr>
        
        Confirms Shipment
        <hr>

        Parameters
        -------------

        :param `shipment_data`: Dictionary of Shipment data.
        :type shipment_data: dict
        
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
        response_str = f'{response.status_code} {response.reason}'
        self.logger.info(f'{response.status_code} {response.reason}')
        bp = 'here'
        self.data_log.append({
            'Entity': 'Shipment',
            'KeyValue': shipment_data['ShipmentNbr'],
            'Operation': 'POST - Confirm Shipment',
            'Payload': body,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })


    def update_reason_code(self, shipment_data: dict, line_data: dict):    
        '''`update_reason_code`(self, shipment_data: *dict*, line_data: *dict*)
        ---
        <hr>
        
        Updates Reason Code on the line of a Shipment to *RETURN*
        
        <hr>
        
        Parameters
        ----------
        
        :param `shipment_data`: Shipment data
        :type shipment_data: dict
        :param `line_data`: For each line on Shipment, line_data is that line's data dict
        :type line_data: dict
        
        <hr>
        
        Returns
        ----------
        
        :return `line_data` (*dict*): Line data with updated Reason Code'''
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
        response_str = f'{response.status_code} {response.reason}'
        self.data_log.append({
            'Entity': 'Shipment',
            'KeyValue': shipment_data['ShipmentNbr'],
            'Operation': 'PUT - Update ReasonCode on Shipment Line',
            'Payload': body,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        return line_data

    def send_to_wh(self, ShipmentNbr, CustomerID):
        '''`send_to_wh`(ShipmentNbr, CustomerID, )
        ---
        <hr>
        
        _summary_
        
        <hr>
        
        Parameters
        ---
        
        :param `ShipmentNbr`: _description_
        :type ShipmentNbr: _type_
        :param `CustomerID`: _description_
        :type CustomerID: _type_
        
        <hr>
        
        Returns
        ---
        :return `self.status_description` (*str*): Details of interaction with Acumatica API

        :return `body` (*dict*): Dictionary of what was sent to Acumatica API'''
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
            try:                    
                self._logout()
                self._auth()
                response = self.session.put(f'{self.base_uri}/Shipment', json=body)
                self.parse_response(response, {'type': 'Shipment', 'attribute': 'AttributeSHP2WH'})
            except Exception as e:
                self.status_description = 'FAILURE'
                bp = 'here'
        self.data_log.append({
            'Entity': 'Shipment',
            'KeyValue': ShipmentNbr,
            'Operation': 'PUT - Mark Shipment as Sent to WH',
            'Payload': body,
            'Response': self.status_description,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        return self.status_description, body

    def rc_send_to_wh_v2(self, OrderNbr: str, OrderType: str, CustomerID: str, attribute_payload: dict = {}):
        '''`rc_send_to_wh_v2`(self, OrderNbr, OrderType, CustomerID)
        ---
        <hr>
        
        * Marks an *Order*'s **RC Ship to Warehouse** attribute to true. (**AttributeRCSHP2WH**)
        - Additionally, sends the payload included in **attribute_payload**
            - If none, only **AttributeRCSHP2H** is updated

        * API Method: **PUT**
        
        <hr>
        
        Parameters
        ---
        :param (*str*) `OrderNbr`: OrderNbr of Order to update (AR078365)
        :param (*str*) `OrderType`: OrderType of Order to update (RC)
        :param (*str*) `CustomerID`: CustomerID or AcctCD of Customer on Order (C0090306, C0067451)
        
        <hr>

        Returns
        ---

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
                    },
                    **attribute_payload
                }
            } 
        }
        
        self.logger.info(f'Sending attribute_payload to Acumatica for Order {OrderNbr}')
        try:
            response = self.session.put(f'{self.base_uri}/SalesOrder', json=body)
            self.parse_response(response, {'type': 'Order', 'attribute': 'AttributeRCSHP2WH'})
        except Exception as e:
            try:                    
                self._logout()
                self._auth()
                response = self.session.put(f'{self.base_uri}/SalesOrder', json=body)
                self.parse_response(response, {'type': 'Order', 'attribute': 'AttributeRCSHP2WH'})
            except Exception as e:
                self.status_description = 'FAILURE'
                bp = 'here'
        self.data_log.append({
            'Entity': 'SalesOrder',
            'KeyValue': OrderNbr,
            'Operation': 'PUT - Mark RC Order as Sent To WH',
            'Payload': body,
            'Response': self.status_description,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        return self.status_description, body
    

    def send_to_wh_v2(self, ShipmentNbr: str, CustomerID: str, attribute_payload: dict = {}):
        '''`sent_to_wh_v2`(self, ShipmentNbr: *str*, CustomerID: *str*, attribute_payload: *dict*)
        ===
        * Marks a *Shipment*'s **Ship to Warehouse** attribute to true. (AttributeSHP2WH)

        * Receives a dynamic payload for additional attribute values to populate

        * API Method: **PUT**
        
        <hr>

        Parameters
        ---
        :param `ShipmentNbr`: ShipmentNbr of Shipment to update
        :type ShipmentNbr: str
        :param `CustomerID`: CustomerID or AcctCD of Customer on Shipment (C0090306, C0067451)
        :type CustomerID: str
        :param `attribute_payload`: Additional Attribute values to populate on Shipment
        :type attribute_payload: dict
            
        <hr>

        Returns
        ---
        :return `self.status_description` (*str*): Details of interaction with Acumatica API

        :return `body` (*dict*): Dictionary of what was sent to Acumatica API'''
        body = {
            "CustomerID": { "value": f"{CustomerID}" },
            "ShipmentNbr": { "value": f"{ShipmentNbr}" },
            "custom": {
                "Document": {
                    "AttributeSHP2WH": {
                        "value": True
                    },
                    **attribute_payload
                }
            } 
        }
        try:
            self.logger.info(f'Sending attribute_payload to Acumatica for Shipment {ShipmentNbr}')
            response = self.session.put(f'{self.base_uri}/Shipment', json=body)
            self.parse_response(response, {'type': 'Shipment', 'attribute': 'AttributeSHP2WH',})
        except Exception as e:
            try:                    
                self._logout()
                self._auth()
                response = self.session.put(f'{self.base_uri}/Shipment', json=body)
                self.parse_response(response, {'type': 'Shipment', 'attribute': 'AttributeSHP2WH'})
            except Exception as e:
                self.status_description = 'FAILURE'
                bp = 'here'
        self.data_log.append({
            'Entity': 'Shipment',
            'KeyValue': ShipmentNbr,
            'Operation': 'PUT - Mark Shipment as Sent to WH',
            'Payload': body,
            'Response': self.status_description,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        return self.status_description, body
    #endregion Shipment



#region Utility
    def parse_shipment_details(self, shipment_data: dict, response: requests.Response):
        '''`parse_shipment_details`(self, shipment_data, response)
        ===
        Given the Acumatica API's *response*, convert to JSON and combine with shipment_data.

        API response contains the detailed information regarding the shipment, including line details

        <hr>

        Parameters
        ---
        :param `shipment_data`: Dictionary containing details of Shipment
        :type shipment_data: dict
        :param `response`: Response from Acumatica API (Shipment endpoint)
        :type response: requests.Response

         
        <hr>

        Returns
        ---

         `shipment_details` (*dict*): **shipment_data**, but with added *Status*, *package_count*, *line_count* and *details* values
        
         - **shipment_details['*Status*']**

            - Status of Shipment in Acumatica   
            
         - **shipment_details['*package_count*']**

            - How many packages are currently on the Shipment
            
         - **shipment_details['*line_count*']**

            - How many lines are currently on the shipment
            
         - **shipment_details['*details*']**
        
            - LineNbr, InventoryCD, Qty, SplitLineNbr, ReasonCode and id *for each line* on Shipment
        '''
        try:
            json_response = response.json()
        except Exception as e:
            self.logger.error(f'Could not parse response!')
            return {
                'data': response,
                'status': 'Error'
            }
        package_count = json_response['PackageCount']['value']
        line_count = len(json_response['Details'])
        details = [{
            'LineNbr': line['LineNbr']['value'],
            'InventoryCD': line['InventoryID']['value'],
            'Qty': line['ShippedQty']['value'],
            'SplitLineNbr': line['Allocations'][0]['SplitLineNbr']['value'],
            'ReasonCode': line['ReasonCode']['value'],
            'id': line['id']
        } for line in json_response['Details']]

        shipment_details = {
            **shipment_data,
            'Status': json_response['Status']['value'],
            'package_count': package_count,
            'line_count': line_count,
            'details': details
        }
        self.logger.info(f'{json_response['ShipmentNbr']['value']} - Packages: {package_count}. Lines: {line_count}')
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
                self.status_description = f'{self.acu_response[f'{entity_type['type']}Nbr']} has been marked as sent in Acumatica!' 
                self.logger.info(self.status_description)
            else:
                self.status_description = f'{self.acu_response[f'{entity_type['type']}Nbr']} was not marked as sent and is in Open status!'
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
            self.logger.info('Acumatica API is online. Logged into Acumatica and authenticated successfully')
        except Exception as e:
            self._logout()

    def _logout(self):
        self.session.post('https://erp.journeyhl.com/entity/auth/logout')
        self.logger.info('Logged out of Acumatica API session')
#endregion Authentication/Logout




#region wip
    def update_customer_address(self, payload: dict):
        response = self.session.put(f'{self.base_uri}/Customer', json=payload)
        try:
            json_response = response.json()
        except Exception as e:
            self.logger.error(f'Issue updating address')
            return False
        response_str = f'{response.status_code}: {response.reason}'
        self.data_log.append({
            'Entity': 'Customer',
            'KeyValue': payload['CustomerID']['value'],
            'Operation': f'PUT - Update Address',
            'Payload': payload,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        bp = 'here'
        return True





#endregion



#region Delete?

    def validate_customer_address(self, customer_data: dict):
        '''**WORK IN PROGRESS**
        ===
        '''
        payload = {
            "entity": {
                "CustomerID": {
                    "value": customer_data['AcctCD']
                },
            }
        }

        response = self.session.post(f'{self.base_uri}/Customer/validateCustomerAddresses', json=payload)
        response_str = f'{response.status_code}: {response.reason}'
        if response_str == '204: No Content':
            self.logger.info(f"{customer_data['AcctCD']}'s address was validated successfully!")
        else:
            self.logger.error(f"Issue validating {customer_data['AcctCD']}'s address")
            bp = 'here'
        self.data_log.append({
            'Entity': 'Customer',
            'KeyValue': customer_data['AcctCD'],
            'Operation': f'POST - Validate Address',
            'Payload': payload,
            'Response': response_str,
            'Timestamp': datetime.now(ZoneInfo('America/New_York'))
        })
        bp = 'here'
#endregion Delete?