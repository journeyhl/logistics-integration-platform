from config.settings import DATABASES, TABLES
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from pathlib import Path
import polars as pl
import logging
from dataclasses import dataclass
from typing import Generic, TypeVar

#region Queries
@dataclass(frozen=True)
class Query:
    name: str
    query: str

class Queries:
    def __init__(self, database_name: str):
        queries_dir = Path(__file__).resolve().parent.parent / 'sql' / 'queries' / database_name
        for sql_file in queries_dir.glob('*.sql'):
            setattr(self, sql_file.stem, Query(name=sql_file.stem, query=sql_file.read_text()))

    def __getattr__(self, name: str) -> Query:
        raise AttributeError(f"No query named '{name}'")

class CentralStoreQueries(Queries):
    '''Queries to be executed within db_CentralStore'''

    ReturnsPendingReciept: Query
    '''Checks **rmi_RMAStatus** for any *Closed* **or** *Receipted* **Returns**'''

    StatusCheckRMI: Query
    '''Pulls each distinct RMANumber from **rmi_ClosedShipments**, **rmi_Receipts**, and **_util.rmi_send_log**

    Looks for:
     - *null* **RMAStatuses** or any rows with a **RMAStatus** not equal to '*CLOSED*' or *blank*
    OR
     - **LastChecked** value *within the last two days* and **RMAStatus** not equal to *'OPEN'*'''
    
    AuditFulfillment: Query
    '''No query yet'''

    PackShipmentRedStag: Query
    '''This query originally drove the RedStag Confirmations celigo flow. 

    Pulls shipments from CentralStore using the **acu.rs_** tables to determine which should be shipped'''

    PackShipmentRMI: Query
    '''Pulls closed Shipments from rmi_ClosedShipments'''

    RedStagEvents: Query
    '''More robust version of PackShipment, also a redundancy. 
    
    Uses the **json.RedStagEvents** table to get all rows where **json_value(*jsonData, '$.topic')*** = ***'shipment:packed'***'''

    NotifyFulfillmentOpsTeam: Query
    '''Pulls all Open Return orders from _util.RMI_Send_Log that are stuck with the Item does not exist error from RMI
    '''
    SalesOrderCleaner: Query
    '''Pulls all Sales Orders from acu.SalesOrders that have rows with different statuses.'''
    Kustomer_FilteredOutOrders : Query
    ''''''



class AcumaticaDbQueries(Queries):
    '''Queries to be executed within AcumaticaDb'''
    SendRMIReturns: Query
    '''Pulls all **RC** Sales Orders that are in **Open** status and have a *AttributeRCSHP2WH* value that is **null** or **not equal to 1**

    Where
    ---
    <hr>
    
     **OrderType** = *'RC'* 
        - OrderType is Return

     **Status** = *'N'*
        - Status is Open

     **SiteCD** = *'RMI'*
        - Warehouse is RMI

     **(AttributeRCSHP2WH** != *1* *or* **AttributeRCSHP2WH** *is null***)** 
        - RC Order has not been sent to Warehouse'''
    SendRMIShipments: Query
    '''Pulls Shipments that are ready to be sent to RMI as type Ws

    Where
    ---
    <hr>

     **OrigOrderType** != **'RC'** 
        - Original OrderType != RC

     **Status not in('C', 'L', 'F', 'I')** 
        - Completed, Cancelled, Confirmed, Invoiced

     **AttributeSHP2WH** = **0**
        - Not sent to Warehouse

     **SiteCD** = *'RMI'*
        - Warehouse is RMI'''
    SendRedStagShipments: Query
    '''Pulls Shipments that are ready to be sent to RedStag

    Where
    ---
    <hr>

     **OrigOrderType** != **'RC'** 
        - Original OrderType != RC

     **Status not in('C', 'L', 'F', 'I')** 
        - Completed, Cancelled, Confirmed, Invoiced

     **AttributeSHP2WH** = **0**
        - Not sent to Warehouse

     **left(SiteCD, 7)** = *'RedStag'* (This covers REDSTAGSWT and REDSTAGSLC)
        - Warehouse is RedStag'''
    OpenRCsNoReceipt: Query
    '''Pulls all Open RC Orders that have been sent to RMI and do not have a Shipment(Receipt)

    Where
    ---
    <hr>

     **OrderType** != **'RC'** 
        - OrderType != RC

     **Status in('N')** 
        - Status is Open

     **AttributeSHP2WH** = **1**
        - Order has been sent to the warehouse

     **SiteCD** = *'RMI'*
        - Warehouse is RMI
        
     **ShipmentNbr** *is null*
        - Order does not have a Shipment found when joining on SOLine -> SOShipLine'''
    ShipmentsReadyToConfirm: Query
    '''Pulls all *Open* Shipments that have a Tracking Number and are ready to be confirmed
    
    Where
    ---
    <hr>

     **TrackNumber is not null** 
        - TrackingNumber was able to be joined to Shipment line

     **Status = 'N'** 
        - Status is Open
        
     **left(SiteCD, 7)** = *'REDSTAG'*
        - Warehouse is REDSTAGSWT or REDSTAGSLC'''
    PackShipment: Query
    '''Query from adf that populates acu.rsFulfill

    
    Where
    ---
    <hr>

     **TrackNumber is null** 
        - TrackingNumber was NOT able to be joined to Shipment line

     **Status = 'N'** 
        - Status is Open'''
    ValidateAddresses: Query
    '''Pulls Orders that have unvalidated addresses

    Where
    ---
    <hr>

    **a.IsValidated** *is null* or **a.IsValidated** = **0**
        - Address is not valid
    
    **OrderType** != **'QT'**
         - Order is not a Quote
    
    **Status** not in *('L', 'C', 'S')*'''
    SOOrderDeletions: Query
    '''Pulls records from SOOrder that have been deleted in Acumatica for transfer to db_CentralStore'''
    SOLineDeletions: Query
    '''Pulls records from SOLine that have been deleted in Acumatica for transfer to db_CentralStore'''
    SOShipmentDeletions: Query
    '''Pulls records from SOShipment that have been deleted in Acumatica for transfer to db_CentralStore'''
    SOOrderShipmentDeletions: Query
    '''Pulls records from SOOrderShipment that have been deleted in Acumatica for transfer to db_CentralStore'''
    AcuToDbc_Quotes: Query
    '''Pulls Orders of QT Order Type for upsert to acu.Quotes'''
    Kustomer_OrderIngest : Query
    """Pulls top level Order, Line and Customer data to be sent to Kustomer when **'ingest' *or* no params** are passed to :class:`~pipelines.kustomer.SendOrderDetailsToKustomer`.:meth:`~pipelines.kustomer.SendOrderDetailsToKustomer._re_init`"""
    Kustomer_OrderIngestBackfill : Query
    '''Pulls top level Order, Line and Customer data to be sent to Kustomer when **'backfill'** is passed as a param to :class:`~pipelines.kustomer.SendOrderDetailsToKustomer`.:meth:`~pipelines.kustomer.SendOrderDetailsToKustomer._re_init`'''
    Kustomer_ShipmentData : Query
    '''Pulls Shipments associated with the orders found in the Kustomer_OrderIngest (or Backfill) extract'''
    AcuToDbc_SalesOrders: Query    
    AcuToDbc_Shipments: Query

_QUERY_CLASSES: dict[str, type[Queries]] = {
    'db_CentralStore': CentralStoreQueries,
    'AcumaticaDb': AcumaticaDbQueries,
}
QT = TypeVar('QT', bound=Queries)
#endregion

class SQLConnector(Generic[QT]):
    queries: QT


    def __init__(self, pipeline, database_name: str):
        '''`init`(pipeline, database_name: *str*, )
        ---
        <hr>
        
        Initializes the SQLConnector for either db_CentralStore or AcumaticaDb
        
        <hr>
        
        Parameters
        ---
        :param `pipeline`: Pipeline using the SQLConnector. If just using the Connector without a pipe, send a str
        :param (*str*) `database_name`: Name of database, either 'db_CentralStore' or 'AcumaticaDb'
        
        <hr>
        
        Sets
        ---
        >>> self.pipeline = pipeline
        >>> self.logger = logging.getLogger(f'{database_name}')
        >>> if database_name not in DATABASES:
             raise ValueError(f'Unknown db!')
        >>> self.database_name = database_name
        >>> self.config = DATABASES[database_name]
        >>> self.engine = self._create_engine()
        >>> self.raw_connection = self.engine.raw_connection()
        >>> self.queries = _QUERY_CLASSES.get(database_name, Queries)(database_name)

        **_create_engine** creates connection string using creds from :data:`~config.settings.DATABASES`
        '''
        self.pipeline = pipeline
        if type(pipeline) == str:
            self.logger = logging.getLogger(f'{pipeline}.{database_name}')
        else:
            self.logger = logging.getLogger(f'{pipeline.pipeline_name}.{database_name}')
        if database_name not in DATABASES:
            raise ValueError(f'Unknown db!')

        self.database_name = database_name
        self.config = DATABASES[database_name]
        self.engine = self._create_engine()
        self.raw_connection = self.engine.raw_connection()
        self.queries = _QUERY_CLASSES.get(database_name, Queries)(database_name)  # type: ignore[assignment]
        pass

        
    def _create_engine(self):
        password = quote_plus(str(self.config['password']))
        connection_string = (
            f"mssql+pymssql://{self.config['username']}:{password}"
            f"@{self.config['server']}/{self.config['database']}"
        )
        return create_engine(connection_string, connect_args={"tds_version": "7.3", "login_timeout": 30})
    

    def query_db(self, query: str, params=None, log_str=None):
        '''`query_db`(self, query: *str*)
        ---
        <hr>
            
        Given a string of SQL text, execute and return a polars DataFrame.

        <hr>

        Parameters
        -------------

        :param (*str*) `query`: SQL query to execute
        :param (*dict*) `params`: Parameters to pass to query if neccessary
        :param (*dict*) `log_str`: String to pass to log output if neccessary

        <hr>

        Returns
        -------------

        :return `data_extract` (*pl.DataFrame*): polars DataFrame with results of query
        '''
        execute_options = {'parameters': params} if params else {}
        log_str = log_str if log_str else ''
        data_extract = pl.read_database(query, self.engine, execute_options=execute_options)
        if log_str == '':
            self.logger.info(f'Extracted {data_extract.height} rows')
            
        return data_extract
    

    def insert_df(self, df_data_loaded: pl.DataFrame, table_name: str):
        '''`insert_df`(self, df_data_loaded: *pl.DataFrame*, table_name: *str*)

        Given a polars dataframe and the name of a table, insert the contents of the DataFrame to that table

        :param df_data_loaded: Data to be loaded into SQL db
        :type df_data_loaded: pl.DataFrame
        :param table_name: The name of the table in which the contents of *df_data_loaded* will be inserted
        :type table_name: str
        '''
        df_data_loaded.write_database(table_name=table_name, 
                                      connection=self.engine,
                                      if_table_exists='append',
                                      )
        self.logger.info(f'Wrote {df_data_loaded.height} rows to {table_name}')


    
    def checked_upsert(self, table_name: str, data: list):
        '''
        `checked_upsert`(self, table_name: *str*, data: *list*)
        ---
        <hr>

        Given a table name and a list of rows (dicts) to insert, performs an upsert to database.
    
        ### Downstream Calls 
         #### :meth:`~_dict_to_params`
            - Utility function to format table keys, columns and update_columns with their respective values to parameters

        <hr>
        
        Parameters
        ---
        :param (*str*) `table_name`: The name of the table to update (schema qualified)

         - **'_util.acu_api_log'** or **'AdDetails'**

          - ***AdDetails** doesn't need the schema since it belongs to the **dbo** schema, but you could pass **'dbo.AdDetails'** if you wanted*

        :param (*list*) `data`: A list of dictionaries that correspond to the values in :data:`~config.settings.TABLES`
        
            * Each dictionary should be formatted to contain the values that were mapped in the table configuration in :data:`~config.settings.TABLES`

                * Take **_util.SOOrderDeletions** for example. Its definition looks as follows:
                >>> '_util.SOOrderDeletions':{
                    'keys': ['OrderType', 'OrderNbr'],
                    'columns': [
                        'OrderType',
                        'OrderNbr',
                        'DeletedBy',
                        'DeletedDatetime'
                    ],
                    'update_columns':[
                        'DeletedBy',
                        'DeletedDatetime'
                    ]
                },

                * Using these values, we'll create **upsert_string**. Starting with the check first:

                >>> if not exists(
                select 1 
                from {table_name}
                where {' = %s and '.join(sql_table['keys'])} = %s
                )

                * Alrighty, replace `{table_name}` with `table_name`, or **_util.SOOrderDeletions** here. Then for each **key** in `keys`, we'll format the *where*
                
                >>> if not exists(
                select 1 
                from _util.SOOrderDeletions
                where OrderType = %s and OrderNbr = %s
                )

                * That pattern continues on for the full execution statement.
        '''
        self.tables = TABLES
        sql_table = self.tables[table_name]
        upsert_string = f'''
if not exists(
select 1 
from {table_name}
where {' = %s and '.join(sql_table['keys'])} = %s
)
begin
insert into {table_name}({', '.join(sql_table['columns'])}  )
values({', '.join(['%s'] * len(sql_table['columns']))})
end
else
begin
update {table_name} set {' = %s, '.join(col for col in sql_table['update_columns'])} = %s
where {' = %s and '.join(sql_table['keys'])} = %s
end
'''
        try:
            params = [self._dict_to_params(data_dict, sql_table['keys'] + sql_table['columns'] + sql_table['update_columns'] + sql_table['keys']) for data_dict in data]
            cursor = self.raw_connection.cursor()
            if len(data) > 50: self.logger.info(f'Beginning upsert of {len(data)} rows to {table_name}...')
            cursor.executemany(upsert_string, params)
            self.logger.info(f'{table_name} ╍ Upserted {len(data)} rows')
            self.raw_connection.commit()
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })
            bp = 'here'




    def checked_upsert_paginated(self, table_name: str, data: list, page_size: int = 100):
        '''
        `checked_upsert`(self, table_name: *str*, data: *list*)
        ---
        <hr>

        Given a table name and a list of rows (dicts) to insert, performs a paginated upsert to database.

        #### USE :meth:`~connectors.sql.SQLConnector.checked_upsert` FOR UPSERTS OF LESS THAN 100 ROWS!!
    
        ### Downstream Calls 
         #### :meth:`~_dict_to_params`
            - Utility function to format table keys, columns and update_columns with their respective values to parameters

        <hr>
        
        Parameters
        ---
        :param (*str*) `table_name`: The name of the table to update (schema qualified)

         - **'_util.acu_api_log'** or **'AdDetails'**

          - ***AdDetails** doesn't need the schema since it belongs to the **dbo** schema, but you could pass **'dbo.AdDetails'** if you wanted*

        :param (*list*) `data`: A list of dictionaries that correspond to the values in :data:`~config.settings.TABLES`
        
            * Each dictionary should be formatted to contain the values that were mapped in the table configuration in :data:`~config.settings.TABLES`

                * Take **_util.SOOrderDeletions** for example. Its definition looks as follows:
                >>> '_util.SOOrderDeletions':{
                    'keys': ['OrderType', 'OrderNbr'],
                    'columns': [
                        'OrderType',
                        'OrderNbr',
                        'DeletedBy',
                        'DeletedDatetime'
                    ],
                    'update_columns':[
                        'DeletedBy',
                        'DeletedDatetime'
                    ]
                },

                * Using these values, we'll create **upsert_string**. Starting with the check first:

                >>> if not exists(
                select 1 
                from {table_name}
                where {' = %s and '.join(sql_table['keys'])} = %s
                )

                * Alrighty, replace `{table_name}` with `table_name`, or **_util.SOOrderDeletions** here. Then for each **key** in `keys`, we'll format the *where*
                
                >>> if not exists(
                select 1 
                from _util.SOOrderDeletions
                where OrderType = %s and OrderNbr = %s
                )

                * That pattern continues on for the full execution statement.
        '''
        self.tables = TABLES
        sql_table = self.tables[table_name]
        upsert_string = f'''
if not exists(
select 1 
from {table_name}
where {' = %s and '.join(sql_table['keys'])} = %s
)
begin
insert into {table_name}({', '.join(sql_table['columns'])}  )
values({', '.join(['%s'] * len(sql_table['columns']))})
end
else
begin
update {table_name} set {' = %s, '.join(col for col in sql_table['update_columns'])} = %s
where {' = %s and '.join(sql_table['keys'])} = %s
end
'''
        total = len(data)
        self.logger.info(f'{total} rows to upsert')
        upserts = 0
        total_upserts = int(total/page_size)
        for start in range(0, total, page_size):
            page = data[start:start + page_size]
            params = [self._dict_to_params(data_dict, sql_table['keys'] + sql_table['columns'] + sql_table['update_columns'] + sql_table['keys']) for data_dict in page]
            cursor = self.raw_connection.cursor()
            self.logger.info(f'Beginning upsert of {page_size} rows to {table_name}...')
            cursor.executemany(upsert_string, params)
            self.raw_connection.commit()
            done = min(start + page_size, total)
            self.logger.info(f'{done}/{total} rows upserted, {len(data) - done} remain. {upserts} Upserts complete, {total_upserts - upserts} to go')
            upserts += 1





    
    def _dict_to_params(self, d: dict, keys: list) -> tuple:
        '''_dict_to_params
        ---
        <hr>
        
        Utility function used by **checked_upsert** to format table keys, columns and update_columns with their respective values to parameters

        <hr>

        Parameters
        -------------

            __d__ (*dict*): dict containing the data to be inserted to database
            __keys__ (*list*): list containing the names of keys, columns and update_columns of table receiving insert'''
        return tuple(d[k.replace('[', '').replace(']', '')] for k in keys)
    




    def query_to_dataframe(self, query: Query):
        '''`query_db`(self, query: **_Query_**)
        ---
        <hr>
            
        Given a **_Query_** (see AcumaticaDbQueries and CentralStoreQueries), execute its query and return a polars dataframe

        <hr>

        Parameters
        -------------

        :param (*str*) `query`: An instance of the Query class, the text of which will be executed and the results output to a polars DataFrame.
        :type query: *str*

        <hr>

        Returns
        -------------

        :return `data_extract` (*pl.DataFrame*): polars DataFrame with results of query
        '''
        self.logger.info(f'Running {query.name} query...')
        data = pl.read_database(str(query.query), self.engine, infer_schema_length = None)
        self.logger.info(f'{data.height} rows returned')
        return data
    
    def raw_execute(self, query: str):
        '''`raw_execute`(self, query)
        ===
        Given an Insert, Update or Delete command, execute on db

        Parameters
        ---
        <hr>

        - **query** (*str*): Query to be executed in Database as plain text

        '''
        cursor = self.raw_connection.cursor()
        self.logger.info(f'Executing query with raw_execute')
        db_msg = cursor.execute(query)
        self.raw_connection.commit()
        if cursor.rowcount:
            self.logger.info(f'{cursor.rowcount} rows affected')