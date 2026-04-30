from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines import SendToAfterShip, AfterShipRetrieval, UpdateAfterShip
import logging
import polars as pl
from datetime import datetime
class Transform:
    def __init__(self, pipeline: SendToAfterShip | AfterShipRetrieval | UpdateAfterShip):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self._set_state_map()
        pass
    
    def transform_send(self, data_extract: dict[str, pl.DataFrame], data_transformed = []):
        slugs_extract = data_extract['slugs_extract']
        shipment_extract = data_extract['shipment_extract']
        shipment_extract2 = shipment_extract.join(
            other = slugs_extract,
            how = 'left',
            on='Slug'
        )
        log_extract = data_extract['log_extract']
        old_aftership_records = data_extract['old_aftership_records']

        shipment_extract_log_filter = shipment_extract.join(log_extract, how='anti', on=['ShipmentNbr', 'OrderNbr', 'Tracking'])
        shipment_extract_old_record_filter = shipment_extract_log_filter.join(old_aftership_records, how='anti', on=['OrderNbr', 'Tracking'])
        self.logger.info(f'{shipment_extract.height} Shipments extracted, {shipment_extract.height - shipment_extract_old_record_filter.height} filtered')
        for i, row in enumerate(shipment_extract_old_record_filter.iter_rows(named=True)):
            row = self.iterate_rows(i = i, row = row, additional_extract = shipment_extract_old_record_filter)            
            data_transformed.append(row)
        return data_transformed


    def transform_update(self, data_extract, data_transformed = []):
        slugs_extract = data_extract['slugs_extract']
        shipment_extract = data_extract['shipment_extract']
        aftership_extract = pl.DataFrame(data_extract['aftership_extract'], infer_schema_length=None)
        aftership_extract_joined = aftership_extract.join(
            other=shipment_extract, 
            how = 'inner', 
            left_on=['tracking_number', 'order_number'], 
            right_on=['Tracking', 'OrderNbr']
        )
        for i, row in enumerate(aftership_extract_joined.iter_rows(named=True)):
            row = self.iterate_rows(i = i, row = row, additional_extract = aftership_extract)            
            data_transformed.append(row)
        bp = 'here'
        data_transformed_customers = {
            dt['id']: {
                'order': dt['order_number'],
                'shipment': dt['ShipmentNbr'],
                'tracking': dt['tracking_number'], 
                'payload': {
                    'customers': dt['formatted']['customers'],
                    "shipment_tags": [
                        dt['CustomerClass']
                    ],

                }
                
            } 
        for dt in data_transformed}
        return data_transformed_customers




    def iterate_rows(self, i: int, row: dict, additional_extract):
        cclass = row['CustomerClass']
        row['formatted'] = {
            "tracking_number": row['Tracking'] if row.get('Tracking') else row['tracking_number'],
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
            "order_number": row['OrderNbr'] if row.get('OrderNbr') else row['order_number'],
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
                    "email": row['Email'] if row['Email'] != None and ';' not in row['Email'] else None if row['Email'] == None else row['Email'].split(';')[0],
                    "phone_number": row['phone_number'],
                    "language": "en"
                }
            ],
            "smses": [
                row['phone_number']
            ],
            "shipment_tags": [
                cclass,
                    row['ItemClassDescr']
            ],
        }
        return row










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
