import polars as pl
from pipelines import Pipeline

class RMILinkToAcu(Pipeline):
    '''`RMILinkToAcu`(Pipeline)
    ---
    <hr>

    Pipeline to update Aftership data if found to be outdated compared to what is extracted from Acumatica

    # Extraction
     - Two data sources are used in extraction:
        - **acu_extract**: Retrieves any shipments from Acumatica that have RMI as the warehouse and a null Link3PL 
            - **RMI_Link3PL** query
     - Pull all distinct RMAIDs from **rmi_RMAStatus**

    # Transformation
     - Inner join the two extracted DataFrames. This leaves us with just the shipments that don't have a Link3PL attribute value in Acu, but have a record in our RMI status tracking table
     
    # Load
     - Load the Link3PL value from our RMI centralstore query to AcumaticaDB, in the SOShipmentKvExt table

    # Results Logging
     - None needed
    '''
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