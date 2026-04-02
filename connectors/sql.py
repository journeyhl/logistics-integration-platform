from config.settings import DATABASES, TABLES
from urllib.parse import quote_plus
from sqlalchemy import create_engine, text
from pathlib import Path
import polars as pl
import logging
from dataclasses import dataclass
from typing import Generic, TypeVar


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
    ReturnsPendingReciept: Query
    '''Checks **rmi_RMAStatus** for any *Closed* **or** *Receipted* **Returns**
    '''
    StatusCheckRMI: Query
    '''Pulls each distinct RMANumber from **rmi_ClosedShipments**, **rmi_Receipts**, and **_util.rmi_send_log**

    Looks for:
     - *null* **RMAStatuses** or any rows with a **RMAStatus** not equal to '*CLOSED*' or *blank*
    OR
     - **LastChecked** value *within the last two days* and **RMAStatus** not equal to '*OPEN*'
    '''
    AuditFulfillment: Query
    '''No query yet
    '''
    PackShipment: Query
    '''This query originally drove the RedStag Confirmations celigo flow. 

    Pulls shipments from CentralStore using the **acu.rs_** tables to determine which should be shipped
    '''
    RedStagEvents: Query
    '''More robust version of PackShipment, also a redundancy. 
    
    Uses the **json.RedStagEvents** table to get all rows where **json_value(*jsonData, '$.topic')*** = '***shipment:packed***'
    '''


class AcumaticaDbQueries(Queries):
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

     **SiteCD** = *'RedStag'*
        - Warehouse is RMI'''



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



_QUERY_CLASSES: dict[str, type[Queries]] = {
    'db_CentralStore': CentralStoreQueries,
    'AcumaticaDb': AcumaticaDbQueries,
}

QT = TypeVar('QT', bound=Queries)


class SQLConnector(Generic[QT]):
    queries: QT


    def __init__(self, pipeline, database_name: str):
        self.pipeline = pipeline
        # self.logger = logging.getLogger(f'{pipeline.pipeline_name}.{database_name}')
        self.logger = logging.getLogger(f'{database_name}')
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
    

    def query_db(self, query: str):
        data_extract = pl.read_database(query, self.engine)
        self.logger.info(f'Extracted {data_extract.height} rows')
        return data_extract
    

    def insert_df(self, df_data_loaded: pl.DataFrame, table_name: str):
        '''_summary_

        _extended_summary_

        :param df_data_loaded: _description_
        :type df_data_loaded: pl.DataFrame
        :param table_name: _description_
        :type table_name: str
        '''
        df_data_loaded.write_database(table_name=table_name, 
                                      connection=self.engine,
                                      if_table_exists='append',
                                      )
        bp = 'here'


    
    def checked_upsert(self, table_name: str, data: list):        
        self.tables = TABLES
        sql_table = self.tables[table_name]
        upsert_string = f'''
if not exists(
select 1 
from {table_name}
where {' = %s and '.join(sql_table['keys'])} = %s
)
begin
insert into {table_name}({', '.join(sql_table['columns'])})
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
            self.logger.info(f'Beginning upsert of {len(data)} rows to {table_name}...')
            cursor.executemany(upsert_string, params)
            self.logger.info(f'{table_name} ╍ Upserted {len(data)} rows')
            self.raw_connection.commit()
        except Exception as e:
            self.logger.error({
                'Table': table_name,
                'err_msg': e
            })
            bp = 'here'



    
    def _dict_to_params(self, d: dict, keys: list) -> tuple:
        return tuple(d[k.replace('[', '').replace(']', '')] for k in keys)
    

