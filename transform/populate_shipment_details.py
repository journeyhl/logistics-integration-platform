import logging
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract: dict[str, pl.DataFrame]):

        central_transformed = data_extract['central_extract']
        acu_transformed = data_extract['acu_extract']
        data_transformed = []
        for acu_shipment in acu_transformed.iter_rows(named=True):
            match = next((
                cs_shipment for cs_shipment in central_transformed.iter_rows(named=True)
                    if acu_shipment['ShipmentNbr'] == cs_shipment['ShipmentNbr'].replace('-1', '').replace('-2', '').replace('-3', '')
                    and acu_shipment['ShipmentLineNbr'] == cs_shipment['ShipLineNbr']
                    and acu_shipment['SplitLineNbr'] == cs_shipment['SplitLineNbr']
                    and acu_shipment['InventoryCD'] == cs_shipment['InventoryCD']
                    ), None)
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
        self.logger.info(f'Matched {len(data_transformed)} rows')
        shipments = self.group_tracking(data_transformed)
        return shipments
    

    def group_tracking(self, data_transformed):
        '''`group_tracking`(self, data_transformed)
    ===
    <hr>

    Groups shipment lines by **ShipmentNbr** and **TrackingNbr**
        '''
        shipments = {}
        for i, line in enumerate(data_transformed):
            if shipments.get(f'{line['ShipmentNbr']}') == None:
                package_payload = self._format_package(line, data_transformed)
                friendly_package_payload = self._format_friendly_package_payload(line, data_transformed)
                # package_payload = self._format_package_contents(line, data_transformed, package_payload)
                shipments[f'{line['ShipmentNbr']}'] = {
                    'ShipmentNbr': line['ShipmentNbr'],
                    'PackagePayload': package_payload,
                    '_FriendlyPackagePayload': friendly_package_payload
                }

                bp = 'here'
                # shipments[line['ShipmentNbr']][line['TrackingNbr_3pl']] = {
                #     'ShipmentNbr': line['ShipmentNbr'],
                #     'TrackingNbr': line['TrackingNbr_3pl'],
                #     'Courier': line['Courier_3pl'],

                # }
            bp = 'here'
        bp = 'here'
        
        return shipments
    
    def _format_package(self, shipment_line_data, shipment_data,):
        now = datetime.now(ZoneInfo('America/New_York')).strftime('%m/%d/%Y %H:%M:%S')
        descr = f'Package added via API @ {now}'
        package_payload = {
            "ShipmentNbr": { "value": f"{shipment_line_data['ShipmentNbr']}" },
            "Packages": [
                {
                    "BoxID": { "value": "DEFAULT BOX" },
                    "TrackingNbr": { "value": f"{line['TrackingNbr_3pl']}" },
                    "Description": { "value": f"{descr}" },
                    "Weight": { "value": 0 },
                    "UOM": { "value": "LBS" },
                    "PackageContents": [
                        {
                            "InventoryID": { "value": pkg_line_data['InventoryCD'] },
                            "Quantity": { "value": pkg_line_data['Qty_3pl'] },
                            "UOM": { "value": "EA" },
                            "ShipmentSplitLineNbr": { "value": pkg_line_data['SplitLineNbr']}
                        }
                        for pkg_line_data in shipment_data
                    if line['TrackingNbr_3pl'] == pkg_line_data['TrackingNbr_3pl']  
                    ]
                }
                for line in shipment_data
            if shipment_line_data['ShipmentNbr'] == line['ShipmentNbr']
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