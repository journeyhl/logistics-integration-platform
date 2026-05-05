from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines import SendToAfterShip, UpdateAfterShip
import logging
import polars as pl
from datetime import datetime
class Transform:
    def __init__(self, pipeline: SendToAfterShip | UpdateAfterShip):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self._set_state_map()
        pass
    
    def transform_send(self, data_extract: dict[str, pl.DataFrame], data_transformed = []):
        slugs_extract = data_extract['slugs_extract']
        shipment_extract = data_extract['shipment_extract']
        log_extract = data_extract['log_extract']
        old_aftership_records = data_extract['old_aftership_records']

        shipment_extract_log_filter = shipment_extract.join(log_extract, how='anti', on=['ShipmentNbr', 'OrderNbr', 'Tracking'])
        shipment_extract_old_record_filter = shipment_extract_log_filter.join(old_aftership_records, how='anti', on=['OrderNbr', 'Tracking'])
        self.logger.info(f'{shipment_extract.height} Shipments extracted, {shipment_extract.height - shipment_extract_old_record_filter.height} filtered')
        for i, row in enumerate(shipment_extract_old_record_filter.iter_rows(named=True)):
            row = self.iterate_rows(row = row)
            data_transformed.append(row)
        return data_transformed


    def transform_update(self, data_extract, data_transformed = []):
        '''`transform_update`(data_extract: *_type_*, )
        ---
        <hr>
        
        Method that drives the filtering and transformation of :class:`~pipelines.aftership_update.UpdateAfterShip` pipeline
        
        ### Downstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*_type_*) `data_extract`: _description_
        
        <hr>
        
        Returns
        ---
        :return `data_transformed_customers` (_type_): _description_
        '''
        slugs_extract = data_extract['slugs_extract']
        shipment_extract = data_extract['shipment_extract']
        aftership_extract = pl.DataFrame(data_extract['aftership_extract'], infer_schema_length=None)
        aftership_extract_joined = aftership_extract.join(
            other=shipment_extract, 
            how = 'inner', 
            left_on=['tracking_number', 'order_number'], 
            right_on=['Tracking', 'OrderNbr']
        )
        self.logger.info(f'Filtered from {aftership_extract.height} records to {aftership_extract_joined.height}')
        for i, row in enumerate(aftership_extract_joined.iter_rows(named=True)):
            row = self.iterate_rows(row = row)            
            data_transformed.append(row)
        bp = 'here'


        data_transformed_customers = {}
        for dt in data_transformed:
            customer_diff = False
            tag_diff = False
            if dt['formatted']['customers'] == None and dt['formatted']['shipment_tags'] == None:
                continue
            if dt['formatted']['customers'] != None:
                customer_diff = self.compare(aftership_value=dt['customers'], sql_value=dt['formatted']['customers'])
            if dt['formatted']['shipment_tags'] != None:
                tag_diff = self.compare(aftership_value=dt['shipment_tags'], sql_value=dt['formatted']['shipment_tags'])
            if customer_diff or tag_diff:
                data_transformed_customers[dt['id']] = {
                    'order': dt['order_number'],
                    'shipment': dt['ShipmentNbr'],
                    'tracking': dt['tracking_number'], 
                    'payload': {}
                }
                if customer_diff:
                    after = dt['customers']
                    db = dt['formatted']['customers']
                    data_transformed_customers[dt['id']]['payload']['customers'] = dt['formatted']['customers']
                if tag_diff:
                    after = dt['shipment_tags']
                    db = dt['formatted']['shipment_tags']
                    data_transformed_customers[dt['id']]['payload']['shipment_tags'] = dt['formatted']['shipment_tags']

        # data_transformed_customers = {
        #     dt['id']: {
        #         'order': dt['order_number'],
        #         'shipment': dt['ShipmentNbr'],
        #         'tracking': dt['tracking_number'], 
        #         'payload': {
        #             'customers': dt['formatted']['customers'],
        #             "shipment_tags": [
        #                 dt['CustomerClass'],
        #                 dt['ItemClassDescr'],
        #                 dt['PackageValue']
        #             ],
        #         }                
        #     }
        # for dt in data_transformed if dt['customers'] != dt['formatted']['customers'] or dt['shipment_tags'] != dt['formatted']['shipment_tags']
        # }
        self.logger.info(f'Filtered again from {len(data_transformed)} records to {len(data_transformed_customers)}')
        return data_transformed_customers


    def compare(self, aftership_value, sql_value):
        def normalize(v):
            if isinstance(v, str):
                return v.lower()
            if isinstance(v, list):
                return [normalize(x) for x in v]
            if isinstance(v, dict):
                return {k: normalize(val) for k, val in v.items()}
            return v
        return normalize(aftership_value) != normalize(sql_value)



    def iterate_rows(self, row: dict):
        '''`iterate_rows`(self, i: *int*, row: *dict*)
        ---
        <hr>
        
        Method to transform each row with an iterative loop.
        
        ### Upstream Calls 
         #### :meth:`~transform_update`
            - Drives transformation of :class:`~pipelines.aftership_update.UpdateAfterShip`
         #### :meth:`~transform_send`
            - Drives transformation of :class:`~pipelines.aftership_update.SendToAfterShip`
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `row`: a single duct within a list/Dataframe
        
        <hr>
        
        Returns
        ---
        :return `row` (dict): Transformed `row` parameter
        '''
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
                    "email": row['Email'] if row['Email'] != None and ';' not in row['Email'] else None if row['Email'] == None else row['Email'].split(';')[0],
                    "language": "en",
                    "name": row['Customer'],
                    "phone_number": row['phone_number'],
                    "role": "buyer",
                }
            ],
            "smses": [
                row['phone_number']
            ],
            "shipment_tags": [
                cclass,
                row['ItemClassDescr'],
                row['PackageValue']
            ].sort(),
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
