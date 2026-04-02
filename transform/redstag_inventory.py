from datetime import datetime
from zoneinfo import ZoneInfo
class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        pass


    def transform_inventory(self, data_extract):

        item_summary = []
        item_detail = []
        
        self._print_columns_for_naming(data_extract[0])

        bp = 'here'
        self._print_columns_for_naming(data_extract[0]['detailed'][0])
        for item in data_extract:
            item_summary.append({
                'InventoryCD': item['sku'],
                'Expected':     int(float(item['qty_expected'])),
                'Processed':    int(float(item['qty_processed'])),
                'PutAway':      int(float(item['qty_putaway'])),
                'Available':    int(float(item['qty_available'])),
                'Allocated':    int(float(item['qty_allocated'])),
                'Reserved':     int(float(item['qty_reserved'])),
                'Picked':       int(float(item['qty_picked'])),
                'Backordered':  int(float(item['qty_backordered'])),
                'Advertised':   int(float(item['qty_advertised'])),
                'OnHand':       int(float(item['qty_on_hand'])),
                'Timestamp':  datetime.now(ZoneInfo('America/New_York'))
            })
            for detail in item['detailed']:
                item_detail.append({
                    'InventoryCD':  item['sku'],                
                    'Warehouse':    'REDSTAGSLC' if detail['warehouse_id'] == '6' else 'REDSTAGSWT' if detail['warehouse_id'] == '7' else 'Unknown',
                    'Expected':     int(float(detail['qty_expected'])),
                    'Processed':    int(float(detail['qty_processed'])),
                    'PutAway':      int(float(detail['qty_putaway'])),
                    'Available':    int(float(detail['qty_available'])),
                    'Allocated':    int(float(detail['qty_allocated'])),
                    'Reserved':     int(float(detail['qty_reserved'])),
                    'Picked':       int(float(detail['qty_picked'])),
                    'Advertised':   int(float(detail['qty_advertised'])),
                    'OnHand':       int(float(detail['qty_on_hand'])),
                    'Timestamp':  datetime.now(ZoneInfo('America/New_York'))
                })
        data_transformed = {
            'item_summary': item_summary,
            'item_detail': item_detail
        }
        return data_transformed


        
    def _print_columns_for_naming(self, table_row):
        '''`_print_columns_for_naming`(self, table_row: *dict*)
        ---
        <hr>
        
        _summary_
        
        <hr>
        
        Parameters
        ----------
        
        :param table_row: _description_
        :type table_row: _type_
        
        <hr>
        
        Returns
        ----------
        
        '''
        for i, key in enumerate(table_row):
            print(f"            '{key}': item['{key}'],")
        bp = 'here'