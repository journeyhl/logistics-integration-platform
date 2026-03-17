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
        for order in acu_transformed.iter_rows(named=True):
            match = next((
                rmi_order for rmi_order in central_transformed.iter_rows(named=True)
                    if order['ReturnNbr'] == rmi_order['RMANumber']
                    ), None)
            if match != None:
                bp = 'here'
            bp = 'here'

        
        # central_transformed = data_extract['central_extract'].sql('select distinct RMANumber from self').to_series().to_list()
        # acu_transformed = data_extract['acu_extract'].sql(f'select distinct ReturnNbr from self')
        bp = 'here'
        return {}