import logging
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract: dict[str, pl.DataFrame]):
        central_transformed = data_extract['central_extract']
        redstag_transformed = self.transform_redstag_events(data_extract['redstag_event_extract'])
        rmi_extract = data_extract['rmi_extract']
        acu_transformed = data_extract['acu_extract']
        data_transformed = []
        for acu_shipment in acu_transformed.iter_rows(named=True):
            matches = [cs_shipment for cs_shipment in central_transformed.iter_rows(named=True)
                    if acu_shipment['ShipmentNbr'] == cs_shipment['ShipmentNbr'].replace('-1', '').replace('-2', '').replace('-3', '')
                    and acu_shipment['ShipmentLineNbr'] == cs_shipment['ShipLineNbr']
                    and acu_shipment['SplitLineNbr'] == cs_shipment['SplitLineNbr']
                    and acu_shipment['InventoryCD'] == cs_shipment['InventoryCD']
            ]
            matches_redstag = [rs_shipment for rs_shipment in redstag_transformed
                    if acu_shipment['ShipmentNbr'] == rs_shipment['ShipmentNbr'].replace('-1', '').replace('-2', '').replace('-3', '')
                    and acu_shipment['InventoryCD'] == rs_shipment['InventoryCD']                
            ]
            matches_rmi = [rmi_shipment for rmi_shipment in rmi_extract.iter_rows(named=True)
                    if acu_shipment['ShipmentNbr'] == rmi_shipment['RMANumber']
                    and acu_shipment['InventoryCD'] == rmi_shipment['InventoryCD']                
            ]
            self.shipment_formatted = {
                    'ShipmentNbr': acu_shipment['ShipmentNbr'],
                    'InventoryCD': acu_shipment['InventoryCD'],
                    'OrderQty': acu_shipment['OrderQty'],
                    'ShipQty': acu_shipment['ShipQty'],
                    'SplitLineNbr': acu_shipment['SplitLineNbr']
            }
            if matches_redstag != []:
                log_str = f'{len(matches_redstag)} matches' if len(matches_redstag) > 1 else f'{len(matches_redstag)} matches'
                self.logger.info(f'Found {log_str} matches from RedStagEvents')
                shipment_formatted = self.smash_rs_matches(acu_shipment, matches_redstag)
                data_transformed.extend(shipment_formatted)
                
            if matches != []:
                log_str = f'{len(matches)} matches' if len(matches) > 1 else f'{len(matches)} matches'
                self.logger.info(f'Found {log_str} from PackShipmentRedStag')
                shipment_formatted = self.smash_def_matches(acu_shipment, matches = matches)                
                data_transformed.extend(shipment_formatted)

            if matches_rmi != []:
                log_str = f'{len(matches_rmi)} matches' if len(matches_rmi) > 1 else f'{len(matches_rmi)} matches'
                self.logger.info(f'Found {log_str} from PackShipmentRMI')
                shipment_formatted = self.smash_rmi_matches(acu_shipment, matches_rmi)
                data_transformed.extend(shipment_formatted)


        self.logger.info(f'Matched {len(data_transformed)} rows')
        shipments = self.group_tracking(data_transformed)
        return shipments
    

    def smash_def_matches(self, acu_shipment: dict, matches: list):
        if len(matches) == 1:
            match = matches[0]
            shipment_formatted = [{
                **self.shipment_formatted,
                'InventoryCD_3pl': match['InventoryCD'],
                'Qty_3pl': match['rsQty'],
                'TrackingNbr_3pl': match['TrackingNbr'],
                'ItemsOnPackage_3pl': match['ItemsOnPackage'],
                'MaxPackageNum_3pl': match['MaxPackageNum'],
                'Courier_3pl': match['CourierName'],
                'Instructions_3pl': match['Complete'],
            }]
            return shipment_formatted
        
        formatted_matches = []
        for match in matches:
            formatted_matches.append({
                **self.shipment_formatted,
                'InventoryCD_3pl': match['InventoryCD'],
                'Qty_3pl': match['rsQty'],
                'TrackingNbr_3pl': match['TrackingNbr'],
                'ItemsOnPackage_3pl': match['ItemsOnPackage'],
                'MaxPackageNum_3pl': match['MaxPackageNum'],
                'Courier_3pl': match['CourierName'],
                'Instructions_3pl': match['Complete'],
            })
            bp = 'here'
        return formatted_matches

    def smash_rs_matches(self, acu_shipment: dict, matches: list):
        if len(matches) == 1:
            match = matches[0]
            shipment_formatted = [{
                **self.shipment_formatted,
                'InventoryCD_3pl': match['InventoryCD'],
                'Qty_3pl': match['Qty'],
                'TrackingNbr_3pl': match['TrackingNbr'],
                'ItemsOnPackage_3pl': match['order_item_qty'],
                'Courier_3pl': match['Courier'],
            }]
            return shipment_formatted
        formatted_matches = []
        for match in matches:
            formatted_matches.append({
                **self.shipment_formatted,
                'InventoryCD_3pl': match['InventoryCD'],
                'Qty_3pl': match['Qty'],
                'TrackingNbr_3pl': match['TrackingNbr'],
                'ItemsOnPackage_3pl': match['order_item_qty'],
                'Courier_3pl': match['Courier'],
            })
            bp = 'here'
        return formatted_matches

    def smash_rmi_matches(self, acu_shipment: dict, matches: list):
        if len(matches) == 1:
            match = matches[0]
            shipment_formatted = [{
                **self.shipment_formatted,
                'InventoryCD_3pl': match['InventoryCD'],
                'Qty_3pl': match['QtyShipped'],
                'TrackingNbr_3pl': match['Tracking'],
                'ItemsOnPackage_3pl': match['Lines'],
                'Courier_3pl': match['CarrierCode'],
            }]
            return shipment_formatted
        formatted_matches = []
        for match in matches:
            formatted_matches.append({
                **self.shipment_formatted,
                'InventoryCD_3pl': match['InventoryCD'],
                'Qty_3pl': match['QtyShipped'],
                'TrackingNbr_3pl': match['Tracking'],
                'ItemsOnPackage_3pl': match['Lines'],
                'Courier_3pl': match['CarrierCode'],
            })
            bp = 'here'
        return formatted_matches


    def group_tracking(self, data_transformed: list):
        '''`group_tracking`(self, data_transformed: *list*))
        ---
        <hr>
        
        Adds one entry per shipment to shipments dictionary, the key being the **ShipmentNbr**
        
        <hr>
        
        Parameters
        ----------
        
        :param data_transformed: list of dicts containing Game info.
        :type data_transformed: list
        
        <hr>
        
        Returns
        ----------
        
        :return shipments (*dict*): Dictionary of Shipments, the key value being the ShipmentNbr. Prevents duplicating the same shipment


         shipments must contain the following entries: **ShipmentNbr**, **PackagePayload**, **_FriendlyPackagePayload**     

         **PackagePayload**: payload to be sent to Acumatica API via add_package_v2

         **_FriendlyPackagePayload**: More readable version of PackagePayload, not for use.
            
            
        >>> shipments = {
            'ShipmentNbr': '078983',
            'PackagePayload': {
                'ShipmentNbr': {'value': '078983'}, 
                'Packages': [
                    {
                        'BoxID': {'value': 'DEFAULT BOX'}, 
                        'TrackingNbr': {'value': '380175841075'}, 
                        'Description': {'value': 'Package added via API @ 04/02/2026 11:01:03'}, 
                        'Weight': {'value': 0}, 
                        'UOM': {'value': 'LBS'}, 
                        'PackageContents': [
                            {
                                'InventoryID': {'value': '09054'}, 
                                'Quantity': {'value': '2'}, 
                                'UOM': {'value': 'EA'}, 
                                'ShipmentSplitLineNbr': {'value': 2}
                            }
                        ]
                    }
                ]
            },
            '_FriendlyPackagePayload': [{...}]
        }
        '''
        shipments = {}
        self.package_contents = {}
        for i, line in enumerate(data_transformed):
            if shipments.get(f'{line['ShipmentNbr']}') == None:
                package_payload = self._format_package(line, data_transformed)
                friendly_package_payload = self._format_friendly_package_payload(line, data_transformed)
                shipments[f'{line['ShipmentNbr']}'] = {
                    'ShipmentNbr': line['ShipmentNbr'],
                    'PackagePayload': package_payload,
                    '_FriendlyPackagePayload': friendly_package_payload
                }
        
        return shipments
    
    def _format_package(self, shipment_line_data: dict, shipment_data: list):
        '''`_format_package`(self, shipment_line_data: *dict*, shipment_data: *list*)
        ---
        <hr>
        
        For the Shipment passed in **shipment_line_data**, formats the Acumatica API payload to add package(s) to an Acumatica shipment.
        
        <hr>
        
        Parameters
        ----------
        
        :param shipment_line_data: dict containing data for a single line from a Shipment
        :type shipment_line_data: *dict*
        :param shipment_data: Full list of Shipments that meet the criteria to be packed
        :type shipment_data: *list*
        
        <hr>
        
        Returns
        ----------        
        
        :return package_payload (*dict*): *dictionary containing ShipmentNbr and Packages, a list of all packages to be added to Acumatica.*
        '''
        now = datetime.now(ZoneInfo('America/New_York')).strftime('%m/%d/%Y %H:%M:%S')
        descr = f'Package added via API @ {now}'
        matched_shipment_data = [diff_track_nbr for diff_track_nbr in shipment_data if shipment_line_data['ShipmentNbr'] == diff_track_nbr['ShipmentNbr']]
        # for pkg_line_data in shipment_data:
        bp = 'here'
        packages = []

        if shipment_line_data['ShipmentNbr'] == '079754':
            bp = 'here'
        for i, line in enumerate(matched_shipment_data):
            if self.package_contents.get((line['ShipmentNbr'], line['TrackingNbr_3pl'])) == None:
                distinct_items = len({line['InventoryCD'] for line in matched_shipment_data})
                lines = len(matched_shipment_data)
                
                self.package_contents[(line['ShipmentNbr'], line['TrackingNbr_3pl'])] = [{
                        "InventoryID": { "value": pkg_line_data['InventoryCD'] },
                        "Quantity": { "value": pkg_line_data['Qty_3pl'] },
                        "UOM": { "value": "EA" },
                        "ShipmentSplitLineNbr": { "value": pkg_line_data['SplitLineNbr']}
                    }
                    for j, pkg_line_data in enumerate(matched_shipment_data) if line['TrackingNbr_3pl'] == pkg_line_data['TrackingNbr_3pl'] and line['ShipmentNbr'] == pkg_line_data['ShipmentNbr']
                ]
                package = {
                    "BoxID": { "value": "DEFAULT BOX" },
                    "TrackingNbr": { "value": f"{line['TrackingNbr_3pl']}" },
                    "Description": { "value": f"{descr}" },
                    "Weight": { "value": 0 },
                    "UOM": { "value": "LBS" },
                    "PackageContents": self.package_contents[(line['ShipmentNbr'], line['TrackingNbr_3pl'])]
                }
                packages.append(package)
                bp = 'here'
        package_payload = {
            "ShipmentNbr": { "value": f"{shipment_line_data['ShipmentNbr']}" },
            "Packages": packages
        }

        return package_payload
    

    def _format_friendly_package_payload(self, shipment_line_data, shipment_data):
        '''
        Not for use, just for ease of debugging/logging. Contains same info as _format_package(), but formatted for readibility
        '''
        now = datetime.now(ZoneInfo('America/New_York')).strftime('%m/%d/%Y %H:%M:%S')
        descr = f'Package added via API @ {now}'
        friendly_packages =  [
                {
                    "TrackingNbr": f"{line['TrackingNbr_3pl']}",
                    "Description": descr,
                    "PackageContents": [
                        {
                            "InventoryID": pkg_line_data['InventoryCD'],
                            "Quantity": pkg_line_data['Qty_3pl'],
                            "ShipmentSplitLineNbr": pkg_line_data['SplitLineNbr']
                        }
                        for pkg_line_data in shipment_data
                    if line['TrackingNbr_3pl'] == pkg_line_data['TrackingNbr_3pl']  
                    ]
                }
                for line in shipment_data
            if shipment_line_data['ShipmentNbr'] == line['ShipmentNbr']
        ]        
        return friendly_packages
    


    def transform_redstag_events(self, redstag_extract: pl.DataFrame):
        redstag_events = []
        for row in redstag_extract.iter_rows(named=True):
            try:
                tracking_nbrs = json.loads(row['TrackingNumbers'])
            except:
                tracking_nbrs = []
            if len(tracking_nbrs) == 0:
                continue
            if len(tracking_nbrs) > 1:
                bp = 'here'
            packages = json.loads(row['Packages'])
            items = json.loads(row['Items'])
            trackers = json.loads(row['Trackers'])
            if len(items) == 1:
                redstag_row = {
                    'ShipmentNbr': row['ShipmentNbr_3pl'],
                    'InventoryCD': items[0]['sku'],
                    'TrackingNbr': tracking_nbrs[0] if len(tracking_nbrs) == 1 else tracking_nbrs[1],
                    'Qty': items[0]['quantity'],
                    'Courier': packages[0]['manifest_courier'],
                    'order_item_qty': items[0]['order_item_qty']
                }
                redstag_events.append(redstag_row)
            else:
                for i, item in enumerate(items):
                    redstag_row = {
                        'ShipmentNbr': row['ShipmentNbr_3pl'],
                        'InventoryCD': item['sku'],
                        'TrackingNbr': tracking_nbrs[0] if len(tracking_nbrs) == 1 else tracking_nbrs[i],
                        'Qty': item['quantity'],
                        'Courier': packages[0]['manifest_courier'] if len(packages) == 1 else packages[i]['manifest_courier'],
                        'order_item_qty': item['order_item_qty']
                    }
                    redstag_events.append(redstag_row)
                    bp = 'here'
            bp = 'here'
        bp = 'here'
        return redstag_events
    