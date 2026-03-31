from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.redstag_send_shipments import SendRedStagShipments
import polars as pl
import logging


class Transform:
    def __init__(self, pipeline: SendRedStagShipments):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass

    
    def transform(self, data_extract: pl.DataFrame):
        '''`transform_order_create`(self)
        ===
        <hr>

        Transforms **data_extract**.

        If there's no RedStag OrderID value in Acumatica, lookup the ShipmentNbr at RedStag to confirm it hasn't been sent yet

        '''
        for shipment in data_extract.iter_rows(named=True):
            shipment_nbr = shipment['ShipmentNbr']
            test = data_extract.sql(f'select count(ShipmentNbr) from self where ShipmentNbr = ShipmentNbr')
            rs_order_id = shipment['rsOrderID']
            if not rs_order_id:
                bp = 'here'
                lookup = self.transform_lookup(shipment_nbr)
                if len(lookup) == 0:
                    self.transform_order_create(shipment)
                bp = 'here'
            bp = 'here'


    def transform_lookup(self, shipment_nbr: str):
        '''`transform_order_create`(self)
        ===
        <hr>

        Transforms **data_extract** to format needed for looking up order information via *RedStag API*

        Sets
        ===

         **self.pipeline.lookup** to results dict within response from RedStag
        '''
        payload_target = [    
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
        json_response = self.pipeline.redstag.target_api(payload_target, f'{shipment_nbr}, order.search')
        self.pipeline.lookup = json_response['results']
        self.logger.info(f'{self.pipeline.lookup['totalCount']} Shipments found')
        return self.pipeline.lookup



    def transform_order_create(self, shipment: dict, data_extract: pl.DataFrame):
        '''`transform_order_create`(self)
        ===
        <hr>

        Transforms **shipment** to format needed for posting an **Order** *(Shipment in Acumatica)* to RedStag via API

        

    Parameters
    -------------
    <hr>

        __shipment__ (**dict**): Dictionary of Shipment data. Must contain ** **,

        __data_extract__ (**pl.DataFrame**): Original DataFrame from extract. Will use it to check for other rows on shipment


        '''
        payload_target = [            
		"order.create",
		[
			"journeyhealth",
			[
				{
					"sku": "08918LFT",
					"qty": 1,
					"item_ref": "[2]",
					"shipVia": "GROUND"
				}
			],
			{
				"lastname": "Carolyn Buden",
				"street1": "65 Stonebridge way",
				"city": "berlin",
				"region": "CT",
				"postcode": "06037-2517",
				"country": "US",
				"telephone": "8605059556"
			},
			{
				"unique_id": "078828",
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


