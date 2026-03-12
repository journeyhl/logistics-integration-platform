from datetime import datetime
import logging

class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract):
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