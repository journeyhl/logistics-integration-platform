from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.redstag_send_shipments import SendRedStagShipments
import polars as pl
import logging
from datetime import datetime

class Transform:
    '''Transform

    
    Should return and populate the rsOrderID value to Acumatica afterwards
    '''
    def __init__(self, pipeline: SendRedStagShipments):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass

    
    def transform(self, data_extract: pl.DataFrame) -> dict:
        '''`transform`(self, data_extract: *pl.DataFrame*)
        ===
        <hr>

        Transforms **data_extract**.

        If there's no RedStag OrderID value in Acumatica, lookup the ShipmentNbr at RedStag to confirm it hasn't been sent yet

        Parameters
        ===
        <hr>
        
        :param data_extract: DataFrame extracted from AcumaticaDb
        :type data_extract: *pl.DataFrame*
        
        Returns
        ===
        <hr>

         **self.shipments_done** (list[str: dict])
        >>> self.shipments_done[shipment_nbr] = {
                'ShipmentNbr': shipment_nbr,
                'CustomerID': customer_id,
                'lookup_payload': self.lookup_payload,
                'order_create_payload': self.order_create_payload,
                'execution_payload': self.lookup_payload if self.order_create_payload == None else self.order_create_payload
            }

        '''
        self.shipments_done = {}
        for shipment in data_extract.iter_rows(named=True):
            shipment_nbr = shipment['ShipmentNbr']
            customer_id =  shipment['CustomerID']
            if self.shipments_done.get(f'{shipment['ShipmentNbr']}') == None:
                rs_order_id = shipment['rsOrderID']
                if not rs_order_id:
                    bp = 'here'
                    lookup_response = self.transform_lookup_payload(shipment_nbr)
                    if len(self.lookup_result) == 0:
                        self.transform_order_create_payload(shipment, data_extract)
                        bp = 'here'
                    else:
                        self.order_create_payload = None
                        self.logger.info(f'Shipment already found at RedStag!')
                    bp = 'here'
            self.shipments_done[shipment_nbr] = {
                'ShipmentNbr': shipment_nbr,
                'CustomerID': customer_id,
                'lookup_payload': self.lookup_payload,
                'order_create_payload': self.order_create_payload,
                'execution_payload': self.lookup_payload if self.order_create_payload == None else self.order_create_payload,
                'execution_operation': f'{shipment_nbr}, order.' + f'{'search' if self.order_create_payload == None else 'create'}'
            }
        return self.shipments_done

    def _check_shipvia(self, iterator: int, item: dict, item_payload: list):
        '''`_check_shipvia`(self, iterator: **int**, item: **dict**, item_payload: **list**)
        ---
        <hr>

        Utility function to determine what ShipVia value should be set on the parent level on multi-item Shipments.

        Function is NOT called for the first row of a Shipment, but for every subsequent row.
         - If a Shipment has 3 lines, `_check_shipvia()` will be called twice

        :param iterator: What row of the Shipment we're on
        :type iterator: *int*
        :param item: A single line on a Shipment
        :type item: *dict*
        :param item_payload: Full item payload containing data for all items on Shipment
        :type item_payload: *list*
        :return: If the line's ShipVia equals the shipVia of the line at the index specifiied in **iterator**, return True. Otherwise, False
        :rtype: Boolean (True | False)
        '''
        return item == item_payload[iterator]['shipVia']

    def _determine_shipvia(self, shipment: dict, item_payload: list):
        '''`_determine_shipvia`(self, shipment: **dict**, item_payload: **list**)
        ---
        <hr>

        Following a call to `self._check_shipvia()`, this function determintes what the parent ShipVia value should be

        :param shipment: dict containing Shipment information
        :type shipment: dict
        :param item_payload: list of dicts containing Item information. 1 dict per item
        :type item_payload: list
        :return item_payload: Edited item_payload, shipVia replaced if needed
        :rtype: *list*
        :return ship_via: ShipVia value to be used in **order_create_payload**
        :rtype: *str*
        '''
        if len(item_payload) == 1:
            ship_via = shipment['rsShipVia']
            line_str = 'line'
        else:
            self.logger.info(f'Determining ShipVia value...')
            line_str = 'lines'
            for i, item in enumerate(item_payload[1:]):
                for j in range(i+1):
                    same_shipvia = self._check_shipvia(j, item, item_payload)
                    if not same_shipvia and item['shipVia'] == 'cheapest_ALL':
                        ship_via = item_payload[j]['shipVia']
                    elif not same_shipvia:
                        ship_via = item['shipVia']
                    else:
                        ship_via = shipment['rsShipVia']
                    bp = 'here'

        self.logger.info(f'{shipment['ShipmentNbr']} has {len(item_payload)} {line_str}. ShipVia: {ship_via}')
        for item in item_payload:
            if item['shipVia'] != ship_via:
                item['shipVia'] = ship_via
        return item_payload, ship_via
    


    def transform_order_create_payload(self, shipment: dict, data_extract: pl.DataFrame):
        '''`transform_order_create_payload`(self, shipment: **dict**, data_extract: **pl.DataFrame**)
        ---
        <hr>

        Transforms **shipment** to format needed for posting an **Order** *(Shipment in Acumatica)* to RedStag via API

        

        Parameters
        ===
        <hr>

        :param shipment: Dictionary of Shipment data. Must contain **ShipmentNbr**, **ShipToName**, **ShipToAddress1**, **ShipToCity**, **ShipToState**, **ShipToZip**,, **ShipToCountry**,, **ShipToPhone**,
        :type shipment: _dict_
        
        :param data_extract: Original DataFrame from extract. Will use it to check for other rows on shipment
        :type data_extract: *pl.DataFrame*

            
        Sets
        ===
        <hr>
         
         self.order_create_payload
        '''
        
        item_payload =  [
				{
					"sku": shipment_line['InventoryCD'],
					"qty": shipment_line['ShippedQty'],
					"item_ref": shipment_line['InventoryCD'],
					"shipVia": shipment_line['rsShipVia']
				}
                for shipment_line in data_extract.iter_rows(named=True)
            if shipment['ShipmentNbr'] == shipment_line['ShipmentNbr']
        ]
        item_payload, ship_via = self._determine_shipvia(shipment=shipment, item_payload=item_payload)
        reference_numbers = {} if shipment['OrigOrderType'] != 'RT' else shipment['CustomerOrderNbr']

        self.order_create_payload = [            
            "order.create",
            [
                "journeyhealth",
                item_payload,
                {
                    "lastname": shipment['ShipToName'],
                    "street1": shipment['ShipToAddress1'],
                    "city": shipment['ShipToCity'],
                    "region": shipment['ShipToState'],
                    "postcode": shipment['ShipToZip'],
                    "country": shipment['ShipToCountry'],
                    "telephone": shipment['ShipToPhone'],
                },
                {
                    "unique_id": shipment['ShipmentNbr'],
                    "shipping_method": ship_via,
                    "other_shiping_options": [
                        {
                            "reference_numbers": reference_numbers
                        }
                    ]
                }
            ]
        ]
    
    def transform_acu_attribute_payload(self, data: dict) -> dict:
        
        '''`transform_sent_to_wh_payload`(self, data: **dict**)
        ---
        <hr>

        Once we have the order_id (*rsOrderID*) from RedStag, we can mark the shipment as Sent to WH.


        Parameters
        ===
        <hr>

        :param data: dictionary of data for Shipment. Must contain **data_3pl**, which is a response from RedStag (and potentially other 3PLs for posterity)
        :type data: _dict_


        
        Returns
        ===
        <hr>
        
        :return attribute_payload: dictionary of data to append to SendToWH Acu api action
        :rtype attribute_payload: _dict_
        '''
        bp = 'here'
        
        attribute_payload = {
            'AttributeRSORDERID': {
                'value': data['data_3pl']['order_id']
            },
            'AttributeRSORDERNUM': {
                'value': data['ShipmentNbr']
            },
            'AttributeCOURCODE': {
                'value': data['data_3pl']['shipping_description']
            },
            'AttributeCOURNAME': {
                'value': data['data_3pl']['shipping_method']
            },
            'AttributeSHP2WHDT': {
                'value': datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            },
        }
        return attribute_payload





    #region Lookup
    def transform_lookup_payload(self, shipment_nbr: str):
        '''`transform_payload_lookup`(self, shipment_nbr: **str**)
        ---
        <hr>

        Transforms **data_extract** to format needed for looking up order information via *RedStag API*

        
        Parameters
        ===
        <hr>

        :param shipment_nbr: ShipmentNbr in Acumatica
        :type shipment_nbr: *str*
        

        Sets & Returns
        ===
        <hr>

         **self.lookup_response** (**dict**): Response from RedStag
>>> self.lookup_response = {
  'results': [
              {
                  'order_id': '21240680', 
                  'unique_id': '078890', 
                  'order_ref': None, 
                  'state': 'new'
              }
          ],
  'totalCount': 1,
  'numPages': 1
}
        '''
        self.lookup_payload = [    
            "order.search",
            [
                {
                    "unique_id" : {
                        "eq" : shipment_nbr
                        }
                },
                None,
                [
                    "*", 
                    "items",
                    "shipments",
                    "packages",
                    "tracking_numbers",
                    "shipping_address",
                    "status_history",
                ]
            ]
        ]
        self.lookup_response = self.pipeline.redstag.target_api(self.lookup_payload, f'{shipment_nbr}, order.search')
        self.transform_lookup_response()
        self.logger.info(f'{self.lookup_response['totalCount']} Shipments found at RedStag')
        return self.lookup_response
    
    
    def transform_lookup_response(self):
        '''`transform_lookup_response`(self)
        ===
        
        <hr>

        Parses and formats response from RedStag order lookup 

        Sets
        ===
        <hr>

         **self.lookup_result** (*list*): List of results from RedStag lookup

        >>> self.lookup_result = [
            {
              'ShipmentNbr': '078890', 
              'rsOrderId': '21240680', 
              'State': 'new'
            }
        ]
        '''
        if len(self.lookup_response['results']) > 0:
            self.lookup_result = self.lookup_response['results'][0]
        else:
            self.lookup_result = []
            # for result in self.lookup_response['results']:
            #     self.lookup_result.append({
            #         'ShipmentNbr': result['unique_id'],
            #         'rsOrderId': result['order_id'],
            #         'State': result['state'],
            #         'Carrier': result['shipping_description'],
            #         'CarrierCode': result['shipping_method'],
            #         'Items': result['items'],
            #         'Shipments': result['shipments']
            #     })
    #endregion Lookup

