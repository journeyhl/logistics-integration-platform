from datetime import datetime
import logging
from zoneinfo import ZoneInfo

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass
    
    

    def transform_rmas(self, data_extract):
        table_rows = []
        for item in data_extract:
            for line in item['rmaLines']:

                if item['rmaTypeName'] == 'W':
                    prefix = 'rp'
                elif item['rmaTypeName'] == '3':
                    prefix = 'df'
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
