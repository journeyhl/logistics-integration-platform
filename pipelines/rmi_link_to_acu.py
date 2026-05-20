import polars as pl
from pipelines import Pipeline

class RMILinkToAcu(Pipeline):
    '''`RMILinkToAcu`(Pipeline)
    ---
    <hr>

    Pipeline to populate Shipment attributes in Acumatica through the database, allowing for updates after the UI has disallowed them. Currently updates the AttributeLINK3PL value for RMI shipments that don't have that attribute populated

    # Extraction
     - Two data sources are used in extraction:
        - **acu_extract**: Retrieves any shipments from Acumatica that have RMI as the warehouse and a null Link3PL 
            - **RMI_Link3PL** query
        - **rmi_extract**: Pulls all distinct RMAIDs from **rmi_RMAStatus**

    # Transformation
     - Inner join the two extracted DataFrames. This leaves us with just the shipments that don't have a Link3PL attribute value in Acu, but have a record in our RMI status tracking table
     
    # Load
     - Load the Link3PL value from our RMI centralstore query to AcumaticaDB, in the SOShipmentKvExt table

    # Results Logging
     - None needed
    '''
    def __init__(self, function: str):
        super().__init__('rmi-link-to-acu', function)
        
    def extract(self):
        acu_extract = self.acudb.query_to_dataframe(self.acudb.queries.RMI_Link3PL)
        rmi_extract = self.centralstore.query_to_dataframe(self.centralstore.queries.RMI_Link3PL_RMAStatus)
        

        data_extract = pl.SQLContext(acu = acu_extract, rmi = rmi_extract)
        return data_extract

    def transform(self, data_extract: pl.SQLContext):
        data_transformed = data_extract.execute(
        '''
        select *
             , case when a.FieldName = 'AttributeLINK3PL' then Link3PL
                    when a.FieldName = 'AttributeRMAID' then RMAID
               else null end ValueString
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
        else:
            self.logger.info(f'No rows to upsert')
        return data_transformed
    
    def log_results(self, data_loaded):
        pass