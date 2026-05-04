from datetime import datetime
import logging
from zoneinfo import ZoneInfo

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform_receipts(self, data_extract):
        table_rows = []
        for item in data_extract:
            row = {
                'RMANumber': item['rmaNumber'],
                'ReceiptDate': datetime.strptime(item['receiptDate'], '%Y-%m-%dT%H:%M:%SZ'),
                'ReceiptID': item['receiptId'],
                'RMAID': item['rmaId'],
                'RMALineID': item['rmaLineId'],
                'Qty': item['quantity'],
                'InventoryCD': item['itemNumber'],
                'Location': item['location'],
                'ItemType': item['itemType'],
                'ItemCategory': item['category'],
                'Descr': item['description'],
                'Price': item['price'],
                'Cost': item['cost']
                
            }
            table_rows.append(row)
        return table_rows
    
    def transform_closed_shipments(self, data_extract):
        table_rows = []
        for item in data_extract:
            if item['rmaNumber'] == '081290':
                bp = 'here'
            for line in item['shipLines']:
                row = {
                    'RMANumber': item['rmaNumber'],
                    'RMAID': item['rmaId'],
                    'RMALineID': line['rmaLineId'],
                    'RMAType': item['rmaType'],
                    'CreateDate': datetime.strptime(item['createDate'], '%Y-%m-%dT%H:%M:%SZ'),
                    'ShipDate': datetime.strptime(item['shipDate'], '%Y-%m-%dT%H:%M:%SZ'),
                    'InventoryCD': line['itemNum'],
                    'QtyShipped': line['qtyShipped'],
                    'QtyToShip': line['qtytoShip'],
                    'Location': line['location'],
                    'ItemCategory': line['category'],
                    'Descr': line['model'],
                    'Carrier': item['carrier'],
                    'CarrierCode': item['carrierCode'],
                    'Priority': item['priority'],
                    'Tracking': item['trackingNum'],
                    'FreightCost': item['freightCost'],
                    'OutboundShipMethod': item['outboundShipMethod']                    
                }
                if len(item['shipLines']) == 0:
                    bp = 'here'
                table_rows.append(row)
        return table_rows
    


    def transform_status_records(self, data_extract):
        table_rows = []
        for item in data_extract:
            for line in item['rmaLines']:
                if item['rmaTypeName'] == 'W':
                    prefix = 'rp'
                elif item['rmaTypeName'] == '3':
                    prefix = 'df'
                else:
                    bp = 'here'
                row = {
                    'RMANumber': item['rmaNumber'],
                    'RMAID': item['rmaId'],
                    'RMALineID': line['rmaLineID'],
                    'RMAType': item['rmaTypeName'],
                    'RMAStatus': item['rmaStatus'],
                    'CustomerRef': item['customerRef'],
                    'RMALineNbr': line['rmaLineNumber'],
                    'LineStatus': line[f'{prefix}Status'],
                    'InventoryCD': line[f'{prefix}Item'],
                    'Qty': line[f'{prefix}QuantityExp'],
                    'Descr': line['dfModelNum'] if item['rmaTypeName'] == '3' else None,
                    'RMATypeName': item['rmaTypeDescription'],
                    'Carrier': item['inboundShipCarrier'] if item['rmaTypeName'] == 'W' else None,
                    'Priority': item['inboundShipPriority'] if item['rmaTypeName'] == 'W' else None,
                    'CreateDate': datetime.strptime(item['rmaCreateDate'], '%Y-%m-%dT%H:%M:%SZ'),
                    'RMILastModifiedDate': datetime.strptime(item['rmaLastModifiedDate'], '%Y-%m-%dT%H:%M:%SZ'),
                    'LastChecked': datetime.now(ZoneInfo('America/New_York'))
                }
                table_rows.append(row)

        return table_rows
