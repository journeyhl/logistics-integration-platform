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
            match = next((
                cs_shipment for cs_shipment in central_transformed.iter_rows(named=True)
                    if acu_shipment['ShipmentNbr'] == cs_shipment['ShipmentNbr'].replace('-1', '').replace('-2', '').replace('-3', '')
                    and acu_shipment['ShipmentLineNbr'] == cs_shipment['ShipLineNbr']
                    and acu_shipment['SplitLineNbr'] == cs_shipment['SplitLineNbr']
                    and acu_shipment['InventoryCD'] == cs_shipment['InventoryCD']
                )
            , None)
            match_redstag = next((
                rs_shipment for rs_shipment in redstag_transformed
                    if acu_shipment['ShipmentNbr'] == rs_shipment['ShipmentNbr'].replace('-1', '').replace('-2', '').replace('-3', '')
                    and acu_shipment['InventoryCD'] == rs_shipment['InventoryCD']
                )
            , None)
            match_rmi = next((
                rmi_shipment for rmi_shipment in rmi_extract.iter_rows(named=True)
                    if acu_shipment['ShipmentNbr'] == rmi_shipment['RMANumber']
                    and acu_shipment['InventoryCD'] == rmi_shipment['InventoryCD']
                )
            , None)

            if match != None:
                shipment_formatted = {
                    'ShipmentNbr': acu_shipment['ShipmentNbr'],
                    'InventoryCD': acu_shipment['InventoryCD'],
                    'OrderQty': acu_shipment['OrderQty'],
                    'ShipQty': acu_shipment['ShipQty'],
                    'SplitLineNbr': acu_shipment['SplitLineNbr'],
                    'InventoryCD_3pl': match['InventoryCD'],
                    'Qty_3pl': match['rsQty'],
                    'TrackingNbr_3pl': match['TrackingNbr'],
                    'ItemsOnPackage_3pl': match['ItemsOnPackage'],
                    'MaxPackageNum_3pl': match['MaxPackageNum'],
                    'Courier_3pl': match['CourierName'],
                    'Instructions_3pl': match['Complete'],
                }
                data_transformed.append(shipment_formatted)
            elif match_redstag != None:
                shipment_formatted = {
                    'ShipmentNbr': acu_shipment['ShipmentNbr'],
                    'InventoryCD': acu_shipment['InventoryCD'],
                    'OrderQty': acu_shipment['OrderQty'],
                    'ShipQty': acu_shipment['ShipQty'],
                    'SplitLineNbr': acu_shipment['SplitLineNbr'],
                    'InventoryCD_3pl': match_redstag['InventoryCD'],
                    'Qty_3pl': match_redstag['Qty'],
                    'TrackingNbr_3pl': match_redstag['TrackingNbr'],
                    'ItemsOnPackage_3pl': match_redstag['order_item_qty'],
                    'Courier_3pl': match_redstag['Courier'],
                }
                data_transformed.append(shipment_formatted)
            elif match_rmi != None:
                shipment_formatted = {
                    'ShipmentNbr': acu_shipment['ShipmentNbr'],
                    'InventoryCD': acu_shipment['InventoryCD'],
                    'OrderQty': acu_shipment['OrderQty'],
                    'ShipQty': acu_shipment['ShipQty'],
                    'SplitLineNbr': acu_shipment['SplitLineNbr'],
                    'InventoryCD_3pl': match_rmi['InventoryCD'],
                    'Qty_3pl': match_rmi['QtyShipped'],
                    'TrackingNbr_3pl': match_rmi['Tracking'],
                    'ItemsOnPackage_3pl': match_rmi['Lines'],
                    'Courier_3pl': match_rmi['CarrierCode'],
                }
                if match_rmi['Lines'] > 1:
                    bp = 'here'
                data_transformed.append(shipment_formatted)


        self.logger.info(f'Matched {len(data_transformed)} rows')
        shipments = self.group_tracking(data_transformed)
        return shipments
    

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
        if self.package_contents.get((shipment_line_data['ShipmentNbr'], shipment_line_data['TrackingNbr_3pl'])) == None:
            self.package_contents[(shipment_line_data['ShipmentNbr'], shipment_line_data['TrackingNbr_3pl'])] = [{
                    "InventoryID": { "value": pkg_line_data['InventoryCD'] },
                    "Quantity": { "value": pkg_line_data['Qty_3pl'] },
                    "UOM": { "value": "EA" },
                    "ShipmentSplitLineNbr": { "value": pkg_line_data['SplitLineNbr']}
                }
                for pkg_line_data in shipment_data if shipment_line_data['TrackingNbr_3pl'] == pkg_line_data['TrackingNbr_3pl'] and shipment_line_data['ShipmentNbr'] == pkg_line_data['ShipmentNbr']
            ]

        package_payload = {
            "ShipmentNbr": { "value": f"{shipment_line_data['ShipmentNbr']}" },
            "Packages": [
                {
                    "BoxID": { "value": "DEFAULT BOX" },
                    "TrackingNbr": { "value": f"{shipment_line_data['TrackingNbr_3pl']}" },
                    "Description": { "value": f"{descr}" },
                    "Weight": { "value": 0 },
                    "UOM": { "value": "LBS" },
                    "PackageContents": self.package_contents[(shipment_line_data['ShipmentNbr'], shipment_line_data['TrackingNbr_3pl'])]
                }
            ]
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
                    'TrackingNbr': tracking_nbrs[0],
                    'Qty': items[0]['quantity'],
                    'Courier': packages[0]['manifest_courier'],
                    'order_item_qty': items[0]['order_item_qty']
                }
                redstag_events.append(redstag_row)
            else:
                bp = 'here'
            bp = 'here'
        bp = 'here'
        return redstag_events