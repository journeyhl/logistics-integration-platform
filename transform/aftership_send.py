from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.aftership_send import SendToAfterShip
import logging
import polars as pl
from datetime import datetime
class Transform:
    def __init__(self, pipeline: SendToAfterShip):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self._set_state_map()
        pass
    
    def transform(self, data_extract: dict[str, pl.DataFrame]):
        slugs_extract = data_extract['slugs_extract']
        shipment_extract = data_extract['shipment_extract']
        shipment_extract_filtered = shipment_extract.join(data_extract['log_extract'], how='anti', on=['ShipmentNbr', 'OrderNbr', 'Tracking'])
        self.logger.info(f'{shipment_extract.height} Shipments extracted, {shipment_extract.height - shipment_extract_filtered.height} filtered')
        data_transformed = []
        for i, row in enumerate(shipment_extract.iter_rows(named = True)):
            cclass = row['CustomerClass']
            row['formatted'] = {
                "tracking_number": row['Tracking'],
                "slug": row['Slug'],
                "order_id": row['ShipmentNbr'],
                "customer_name": row['Customer'],
                "origin_country_iso3": 'USA' if row['OriginCountry'] == 'US' else row['OriginCountry'],
                "origin_state": self.states.get(row['OriginState']),
                "origin_city": row['OriginCity'],
                "origin_postal_code": row['OriginZip'],
                "origin_raw_location": f'{row['OriginLine1']} {row['OriginLine2']}' if row['OriginLine2'] != None else row['OriginLine1'],
                "destination_country_iso3": 'USA' if row['DestinationCountry'] == 'US' else row['DestinationCountry'],
                "destination_state": self.states.get(row['DestinationState']),
                "destination_city": row['DestinationCity'],
                "destination_postal_code": row['DestinationZip'],
                "destination_raw_location": f'{row['DestinationLine1']} {row['DestinationLine2']}' if row['DestinationLine2'] != None else row['DestinationLine1'],
                "order_number": row['OrderNbr'],
                "order_date": row['OrderDate'].strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                "custom_fields": {
                    "first_name": row['first_name'],
                    "last_name": row['last_name'],
                    "product_title": row['product_title']
                },
                "customers": [
                    {
                        "role": "buyer",
                        "name": row['Customer'],
                        "email": row['Email'],
                        "phone_number": row['phone_number'],
                        "language": "en"
                    }
                ],
                "smses": [
                    row['phone_number']
                ],    
                "shipment_tags": [
                    cclass,
                    row['OrderNbr'],
                    row['InventoryCD']
                ],
            }
            data_transformed.append(row)
        return data_transformed



    def _set_state_map(self):
        self.states = {
        'AL': "Alabama",
        'AK': "Alaska",
        'AZ': "Arizona",
        'AR': "Arkansas",
        'CA': "California",
        'CO': "Colorado",
        'CT': "Connecticut",
        'DE': "Delaware",
        'FL': "Florida",
        'GA': "Georgia",
        'HI': "Hawaii",
        'ID': "Idaho",
        'IL': "Illinois",
        'IN': "Indiana",
        'IA': "Iowa",
        'KS': "Kansas",
        'KY': "Kentucky",
        'LA': "Louisiana",
        'ME': "Maine",
        'MD': "Maryland",
        'MA': "Massachusetts",
        'MI': "Michigan",
        'MN': "Minnesota",
        'MS': "Mississippi",
        'MO': "Missouri",
        'MT': "Montana",
        'NE': "Nebraska",
        'NV': "Nevada",
        'NH': "New Hampshire",
        'NJ': "New Jersey",
        'NM': "New Mexico",
        'NY': "New York",
        'NC': "North Carolina",
        'ND': "North Dakota",
        'OH': "Ohio",
        'OK': "Oklahoma",
        'OR': "Oregon",
        'PA': "Pennsylvania",
        'RI': "Rhode Island",
        'SC': "South Carolina",
        'SD': "South Dakota",
        'TN': "Tennessee",
        'TX': "Texas",
        'UT': "Utah",
        'VT': "Vermont",
        'VA': "Virginia",
        'WA': "Washington",
        'WV': "West Virginia",
        'WI': "Wisconsin",
        'WY': "Wyoming"
        }        
