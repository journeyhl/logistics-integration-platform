import logging
import polars as pl
class Transform:
    def __init__(self, pipeline):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract: dict[str, pl.DataFrame]):

        central_transformed = data_extract['central_extract']
        acu_transformed = data_extract['acu_extract']
        data_transformed = []
        for order in acu_transformed.iter_rows(named=True):
            match = next((
                rmi_order for rmi_order in central_transformed.iter_rows(named=True)
                    if order['ReturnNbr'] == rmi_order['RMANumber'].replace('-1', '').replace('-2', '').replace('-3', '')
                    ), None)
            if match != None:
                order_formatted = {
                    'OrderType': order['OrderType'],
                    'OrderNbr': order['ReturnNbr'],
                    'AcctCD': order['AcctCD'],
                    'InventoryCD': order['InventoryCD'],
                    'OrderQty': order['Qty'],
                    'InventoryCD_3pl': match['InventoryCD'],
                    'Qty_3pl': match['Qty'] 
                }
                data_transformed.append(order_formatted)
        self.logger.info(f'Matched {len(data_transformed)} rows')
        return data_transformed