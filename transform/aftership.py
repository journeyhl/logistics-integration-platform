from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines import SendToAfterShip, UpdateAfterShip, AfterShipToDbc
import logging
import polars as pl
from datetime import datetime
from zoneinfo import ZoneInfo

class Transform:
    def __init__(self, pipeline: SendToAfterShip | UpdateAfterShip | AfterShipToDbc):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        self._set_state_map()
        self._set_tzoffset_map()
        self.data_transformed = []
        pass
    
    def _lander(self, data_extract:dict):
        '''`_lander`(self, data_extract: *dict*, )
        ---
        <hr>
        
        Parses the `data_extract` dict and sets each extracted dataset as an instance attribute.
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `data_extract`: data extracted from the ***extract*** function in any of the AfterShip Pipelines (:class:`~pipelines.aftership_send.SendToAfterShip`, :class:`~pipelines.aftership_update.UpdateAfterShip`, :class:`~pipelines.aftership_to_dbc.AfterShipToDbc`)
        
        <hr>
        
        Sets
        ---
        #### :attr:`~shipment_extract` 
         - Used by :meth:`~transform_send` and :meth:`~transform_update`
        #### :attr:`~log_extract`
         - Used by :meth:`~transform_send`
        #### :attr:`~old_aftership_records`
         - Used by :meth:`~transform_send`
        #### :attr:`~aftership_extract`
         - Used by :meth:`~transform_update`
        #### :attr:`~slugs_extract` 
         - Not used at the moment
        '''
        self.slugs_extract: pl.DataFrame = data_extract['slugs_extract'] if data_extract.get('slugs_extract') else pl.DataFrame()
        self.shipment_extract:          pl.DataFrame = data_extract['shipment_extract'] if data_extract.get('shipment_extract') else pl.DataFrame()
        self.log_extract:               pl.DataFrame = data_extract['log_extract'] if data_extract.get('log_extract') else pl.DataFrame()
        self.old_aftership_records:     pl.DataFrame = data_extract['old_aftership_records'] if data_extract.get('old_aftership_records') else pl.DataFrame()
        self.aftership_extract:         pl.DataFrame = pl.DataFrame(data_extract['aftership_extract'], infer_schema_length=None) if data_extract.get('aftership_extract') else pl.DataFrame()


    def transform_send(self, data_extract: dict[str, pl.DataFrame], data_transformed = []):
        '''`transform_send`(self, data_extract: *dict[str, pl.DataFrame]*, )
        ---
        <hr>
        
        Method that drives the filtering and transformation of :class:`~pipelines.aftership_send.SendToAfterShip` pipeline
            
        <hr>
        
        Parameters
        ---
        :param (*dict[str, pl.DataFrame]*) `data_extract`: data extracted from :class:`~pipelines.aftership_send.SendToAfterShip`.:meth:`~pipelines.aftership_send.SendToAfterShip.extract`
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (list): formatted list of tracking data ready to be sent to AfterShip
        '''
        self._lander(data_extract=data_extract)
        shipment_extract_log_filter = self.shipment_extract.join(self.log_extract, how='anti', on=['ShipmentNbr', 'OrderNbr', 'Tracking'])
        shipment_extract_old_record_filter = shipment_extract_log_filter.join(self.old_aftership_records, how='anti', on=['OrderNbr', 'Tracking'])
        self.logger.info(f'{self.shipment_extract.height} Shipments extracted, {self.shipment_extract.height - shipment_extract_old_record_filter.height} filtered')
        for i, row in enumerate(shipment_extract_old_record_filter.iter_rows(named=True)):
            row = self.iterate_rows(row = row)
            data_transformed.append(row)
        return data_transformed


    def transform_update(self, data_extract: dict):
        '''`transform_update`(self, data_extract: *_type_*, )
        ---
        <hr>
        
        Method that drives the filtering and transformation of :class:`~pipelines.aftership_update.UpdateAfterShip` pipeline
            
        <hr>
        
        Parameters
        ---
        :param (*_type_*) `data_extract`: data extracted from :class:`~pipelines.aftership_update.UpdateAfterShip`.:meth:`~pipelines.aftership_update.UpdateAfterShip.extract`
        
        <hr>
        
        Returns
        ---
        :return `data_transformed_customers` (list): formatted and filted list of tracking data ready to be sent to AfterShip
        '''
        self._lander(data_extract = data_extract)
        aftership_extract_joined = self.aftership_extract.join(
            other=self.shipment_extract, 
            how = 'inner', 
            left_on=['tracking_number', 'order_number'], 
            right_on=['Tracking', 'OrderNbr']
        )
        self.logger.info(f'Filtered from {self.aftership_extract.height} records to {aftership_extract_joined.height}')
        for i, row in enumerate(aftership_extract_joined.iter_rows(named=True)):
            row = self.iterate_rows(row = row)            
            self.data_transformed.append(row)

        data_transfiltered = self.filter_update()       
        self.logger.info(f'Filtered again from {len(self.data_transformed)} records to {len(data_transfiltered)}')
        return data_transfiltered

    def filter_update(self) -> dict:
        '''`filter_update`()
        ---
        <hr>
        
        After formatting all of our extracted rows, this method filters our list down to only those whose data in Acumatica differs from that of AfterShip
        
        ### Upstream Calls 
         #### :meth:`~transform_update`
            - Called to filter out any rows who have the same data in both extracted datasets
            
        <hr>
        
        Returns
        ---
        :return `data_transfiltered` (dict): Filtered list of transformed data
        '''
        data_transfiltered = {}
        for dt in self.data_transformed:
            customer_diff = False
            tag_diff = False
            if dt['formatted']['customers'] == None and dt['formatted']['shipment_tags'] == None:
                continue
            if dt['formatted']['customers'] != None:
                customer_diff = self.compare(aftership_value=dt['customers'], sql_value=dt['formatted']['customers'])
            if dt['formatted']['shipment_tags'] != None:
                tag_diff = self.compare(aftership_value=dt['shipment_tags'], sql_value=dt['formatted']['shipment_tags'])
            if customer_diff or tag_diff:
                data_transfiltered[dt['id']] = {
                    'order': dt['order_number'],
                    'shipment': dt['ShipmentNbr'],
                    'tracking': dt['tracking_number'], 
                    'payload': {}
                }
                if customer_diff:
                    after = dt['customers']
                    db = dt['formatted']['customers']
                    data_transfiltered[dt['id']]['payload']['customers'] = dt['formatted']['customers']
                if tag_diff:
                    after = dt['shipment_tags']
                    db = dt['formatted']['shipment_tags']
                    data_transfiltered[dt['id']]['payload']['shipment_tags'] = dt['formatted']['shipment_tags']
        return data_transfiltered

    def compare(self, aftership_value, sql_value) -> bool:
        '''`compare`(self, aftership_value, sql_value)
        ---
        <hr>
        
        Used by :meth:`~transform_update` to determine if there's a difference between the two passed parameters
        
        ### Upstream Calls 
         #### :meth:`~transform_update`
            - Calls :meth:`~compare` to check if we should update `customers` and `shipment_tags`
            
        <hr>
        
        Parameters
        ---
        :param (*_type_*) `aftership_value`: value from aftership_extract to compare against up to date sql_value
        :param (*_type_*) `sql_value`: value from shipment_extract to compare against existing aftership value
        
        <hr>
        
        Returns
        ---
        :return `difference` (bool): Is there a diference between the values of the two parameters passed?
        '''
        def normalize(v):
            if isinstance(v, str):
                return v.lower()
            if isinstance(v, list):
                return [normalize(x) for x in v]
            if isinstance(v, dict):
                return {k: normalize(val) for k, val in v.items()}
            return v
        difference = normalize(aftership_value) != normalize(sql_value)
        return difference



    def transform_aftership_to_db(self, data_extract: list):
        '''`transform_aftership_to_db`(self, data_extract: *list*)
        ---
        <hr>
        
        Transform data extracted from Aftership via :class:`~pipelines.aftership_to_dbc.AfterShipToDbc`.:meth:`~pipelines.aftership_to_dbc.AfterShipToDbc.extract` to format required by **acu.AftershipExport** and **acu.AftershipExportDetail**
        
            
        <hr>
        
        Parameters
        ---
        :param (*list*) `data_extract`: Data extracted from AfterShip via :class:`~connectors.aftership.AfterShip`
        
        <hr>
        
        Returns
        ---
        :return `data_transformed` (dict): dict containing lists ready for insert to ***acu.AftershipExport*** and ***acu.AftershipExportDetail***
        '''
        aftership_export = []
        aftership_export_detail = []
        for row in data_extract:
            phone = row['customers'][0]['phone_number'].replace('+', '')
            offset_updated = row['updated_at'][-6:]
            offset_created = row['created_at'][-6:]
            frow = {
                'OrderNbr': row['order_number'],
                'Tracking': row['tracking_number'],
                'ID': row['id'],
                'ShipmentNbr': row['order_id'] if row['order_id'] != None else '' ,
                'Phone': phone,
                'Product': row['custom_fields']['product_title'],
                'ShipmentType': row['shipment_type'],
                'ShipmentWeight': row['shipment_weight'].get('value') if row['shipment_weight'] else None,
                'Link': row['courier_tracking_link'],
                'DestLine1': row['destination_raw_location'],
                'DestCity': row['destination_city'],
                'DestState': row['destination_state'],
                'DestZip': row['destination_postal_code'],
                'Tag': row['tag'],
                'Subtag': row['subtag'],
                'SubtagMessage': row['subtag_message'],
                'LastUpdateTime': datetime.strptime(row['updated_at'],  f'%Y-%m-%dT%H:%M:%S{offset_updated}').replace(tzinfo=ZoneInfo(self.timezones[offset_updated])).astimezone(ZoneInfo('America/New_York')),
                'CreatedTime': datetime.strptime(row['created_at'], f'%Y-%m-%dT%H:%M:%S{offset_created}').replace(tzinfo=ZoneInfo(self.timezones[offset_created])).astimezone(ZoneInfo('America/New_York')),
                
            }
            aftership_export.append(frow)
            if row.get('checkpoints'):
                checkpoints = row['checkpoints']
                for checkpoint in checkpoints:
                    offset_chk = checkpoint['checkpoint_time'][-6:]
                    offset_chk_created = checkpoint['created_at'][-6:]
                    fdetail_row = {
                        'OrderNbr': row['order_number'],
                        'Tracking': row['tracking_number'],
                        'ID': row['id'],
                        'ShipmentNbr': row['order_id'] if row['order_id'] != None else '',
                        'CheckpointTime': datetime.strptime(checkpoint['checkpoint_time'], f'%Y-%m-%dT%H:%M:%S{offset_chk}').replace(tzinfo=ZoneInfo(self.timezones[offset_chk])).astimezone(ZoneInfo('America/New_York')),
                        'Message': checkpoint['message'],
                        'Tag': checkpoint['tag'],
                        'Subtag': checkpoint['subtag'],
                        'SubtagMessage': checkpoint['subtag_message'],
                        'City': checkpoint['city'],
                        'State': checkpoint['state'],
                        'Zip': checkpoint['postal_code'],
                        'Slug': checkpoint['slug'],
                        'RawTag': checkpoint['raw_tag'],
                        'Source': checkpoint['source'],
                        'CheckpointCreatedTime': datetime.strptime(checkpoint['created_at'], f'%Y-%m-%dT%H:%M:%S{offset_chk_created}').replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/New_York')),
                    }
                    aftership_export_detail.append(fdetail_row)
        data_transformed = {
            'aftership_export': aftership_export,
            'aftership_export_detail': aftership_export_detail,
        }

        return data_transformed
            
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





    def _set_tzoffset_map(self):
        self.timezones = {
            '-10:00': 'America/Honolulu',
            '-09:30': 'Pacific/Marquesas',
            '-09:00': 'America/Anchorage',
            '-08:00': 'America/Los_Angeles',
            '-07:00': 'America/Denver',
            '-06:00': 'America/Chicago',
            '-05:00': 'America/New_York',
            '-04:00': 'America/New_York',   #Eastern Daylight
            '-03:30': 'America/St_Johns',
            '-03:00': 'America/Sao_Paulo',
            '-02:00': 'America/Noronha',
            '-01:00': 'Atlantic/Azores',
            '+00:00': 'UTC',
            '+01:00': 'Europe/London',
            '+02:00': 'Europe/Paris',
            '+03:00': 'Europe/Moscow',
            '+04:00': 'Asia/Dubai',
        }




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
