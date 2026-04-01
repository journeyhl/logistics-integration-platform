from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.redstag_send_shipments import SendRedStagShipments
import polars as pl
import logging


class Transform:
    '''Transform

    
    Should return and populate the rsOrderID value to Acumatica afterwards
    '''
    def __init__(self, pipeline: SendRedStagShipments):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass

    
    def transform(self, data_extract: pl.DataFrame):
        '''`transform`(self, data_extract: *pl.DataFrame*)
        ===
        <hr>

        Transforms **data_extract**.

        If there's no RedStag OrderID value in Acumatica, lookup the ShipmentNbr at RedStag to confirm it hasn't been sent yet

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
                        self.logger.info(f'Shipment already found at RedStag!')
                    bp = 'here'
            self.shipments_done[shipment_nbr] = {
                'ShipmentNbr': shipment_nbr,
                'CustomerID': customer_id,
                'lookup_payload': self.lookup_payload,
                'order_create_payload': self.order_create_payload,
            }
        return self.shipments_done


    def transform_order_create_payload(self, shipment: dict, data_extract: pl.DataFrame):
        '''`transform_order_create_payload`(self)
        ===
        <hr>

        Transforms **shipment** to format needed for posting an **Order** *(Shipment in Acumatica)* to RedStag via API

        This function acts as the driver for the order_create transform and the payload itself is transformed in **transform_order_create_payload**
        

        Parameters
        -------------
        <hr>

            __shipment__ (**dict**): Dictionary of Shipment data. Must contain ** **,

            __data_extract__ (**pl.DataFrame**): Original DataFrame from extract. Will use it to check for other rows on shipment'''
        item_payload =  [
				{
					"sku": shipment_line['InventoryCD'],
					"qty": shipment_line['ShippedQty'],
					"item_ref": shipment_line['InventoryCD'],
					"shipVia": shipment_line['ShipVia']
				}
                for shipment_line in data_extract.iter_rows(named=True)
            if shipment['ShipmentNbr'] == shipment_line['ShipmentNbr']
        ]
        line_str = 'line' if len(item_payload) == 1 else 'lines'
        self.logger.info(f'{shipment['ShipmentNbr']} has {len(item_payload)} {line_str}')

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
                    "shipping_method": "cheapest_ALL",
                    "other_shiping_options": [
                        {
                            "reference_numbers": {}
                        }
                    ]
                }
            ]
        ]
        
        bp = 'here'
    






    #region Lookup
    def transform_lookup_payload(self, shipment_nbr: str):
        '''`transform_payload_lookup`(self, shipment_nbr: *str*)
        ===
        <hr>

        Transforms **data_extract** to format needed for looking up order information via *RedStag API*

        
        Parameters
        ===
        <hr>

         **shipment_nbr** (*str*): ShipmentNbr in Acumatica

        Sets
        ===
        <hr>

         **self.lookup_response** (*dict*): Response from RedStag
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
                    "state"
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
        self.lookup_result = []
        if len(self.lookup_response['results']) > 0:
            for result in self.lookup_response['results']:
                self.lookup_result.append({
                    'ShipmentNbr': result['unique_id'],
                    'rsOrderId': result['order_id'],
                    'State': result['state']
                })
    #endregion Lookup

