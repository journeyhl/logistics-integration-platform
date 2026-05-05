import polars as pl
from pipelines import Pipeline

class RMILinkToAcu(Pipeline):

    def __init__(self):
        super().__init__('rmi-link-to-acu')
        
    def extract(self):
        acu_extract = self.acudb.query_to_dataframe(self.acudb.queries.RMI_Link3PL)
        # rmi_extract = self.centralstore.query_db("select distinct RMANumber, concat('https://jhl.returnsmanagement.com/rma/LineItems.asp?rmaid=', RMAID) ValueString from rmi_ClosedShipments")
        rmi_extract = self.centralstore.query_db("select distinct RMANumber, concat('https://jhl.returnsmanagement.com/rma/LineItems.asp?rmaid=', RMAID) ValueString from rmi_RMAStatus")
        

        data_extract = pl.SQLContext(acu = acu_extract, rmi = rmi_extract)
        return data_extract

    def transform(self, data_extract: pl.SQLContext):
        data_transformed = data_extract.execute(
        '''
        select *
        from acu a
        inner join rmi r on a.ShipmentNbr = r.RMANumber                                                
        ''',
        eager =True
        )
        data_transformed = data_transformed.to_dicts()

        return data_transformed
    
    def load(self, data_transformed):
        if len(data_transformed) > 0:
            self.acudb.checked_upsert_paginated('SOShipmentKvExt', data_transformed)
        return data_transformed
    
    def log_results(self, data_loaded):
        pass