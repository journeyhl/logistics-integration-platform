from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.kustomer import SendOrderDetailsToKustomer
import logging
import polars as pl
class Transform:
    def __init__(self, pipeline: SendOrderDetailsToKustomer):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass


    def transform(self, data_extract: pl.DataFrame):
        self.order_extract = data_extract
        shipment_where = self.format_data_extract(data_extract)
        detail_query = self.pipeline.acudb.queries.Kustomer_ShipmentData.query + shipment_where
        self.order_detail_extract = self.pipeline.acudb.query_db(query = detail_query)
        
        data_transformed = self.add_shipments_to_orders()

        bp = 'here'
        return data_transformed
    
    def format_data_extract(self, data_extract: pl.DataFrame) -> str:
        '''`format_data_extract`(self, data_Extract: *pl.DataFrame*)
        ---
        <hr>
        
        Transforms data_extract DataFrame to a list of dicts
            
        <hr>
        
        Parameters
        ---
        :param (*pl.DataFrame*) `data_extract`: results of the query executed in :meth:`~pipelines.kustomer.SendOrderDetailsToKustomer.extract`

            - If **'ingest' *or* no params** are passed to :meth:`~pipelines.kustomer.SendOrderDetailsToKustomer._re_init`, use :attr:`~connectors.sql.AcumaticaDbQueries.Kustomer_OrderIngest`
            - if **'backfill'** is passed to :meth:`~pipelines.kustomer.SendOrderDetailsToKustomer._re_init` as a param, use :attr:`~connectors.sql.AcumaticaDbQueries.Kustomer_OrderIngestBackfill`
        
        <hr>
        
        Returns
        ---
        :return shipment_where (str): string to append to the end of :attr:`~connectors.sql.AcumaticaDbQueries.Kustomer_ShipmentData` query
        '''
        self.orders = []
        for order in data_extract.iter_rows(named = True):      
            self.orders.append({
                'OrderNbr':                     order['OrderNbr'],
                'OrderType':                    order['OrderType'],
                'Status':                       order['Status'],
                'OrderStatus':                  order['OrderStatus'],
                'AcctCD':                       order['AcctCD'],
                'AcctName':                     order['AcctName'],
                'Date':                         order['Date'],
                'CustomerClassID':              order['CustomerClassID'],
                'B2BPaymentLink':               order['B2BPaymentLink'],
                'BillingAddressLine1':          order['BillingAddressLine1'],
                'BillingAddressLine2':          order['BillingAddressLine2'],
                'BillingCity':                  order['BillingCity'],
                'BillingState':                 order['BillingState'],
                'BillingStateName':             info_States.get(order['BillingState']),
                'BillingPostalCode':            order['BillingPostalCode'],
                'BillingCountry':               order['BillingCountry'],
                'BillingCountryName':           'United States' if order['BillingCountry'] == 'US' else order['BillingCountry'],
                'BillingPhone':                 order['BillingPhone'],
                'BillingEmail':                 order['BillingEmail'],
                'ShippingAddressLine1':         order['ShippingAddressLine1'],
                'ShippingAddressLine2':         order['ShippingAddressLine2'],
                'ShippingCity':                 order['ShippingCity'],
                'ShippingState':                order['ShippingState'],
                'ShippingPostalCode':           order['ShippingPostalCode'],
                'ShippingCountry':              order['ShippingCountry'],
                'ShippingPhone':                order['ShippingPhone'],
                'ShippingEmail':                order['ShippingEmail'],
                'Salesperson':                  order['Salesperson'],
                'SalespersonCD':                order['SalespersonCD'],
                'AcuCustomerLink':              order['AcuCustomerLink'],
                'AcuOrderLink':                 order['AcuOrderLink'],
                'HubspotLink':                  order['HubspotLink']

            })
        
        shipment_where = f"and s.OrderNbr in('{"', '".join(order['OrderNbr'] for order in self.orders)}')"
        return shipment_where
    
    def add_shipments_to_orders(self) -> list:     
        '''`add_shipments_to_orders`(self)
        ---
        <hr>
        
        Using `self.order_detail_extract`, creates 3 sub DataFrames:
            - df_order_line: **OrderNbr, LineNbr, InventoryCD, Descr, LineQty**
            - df_shipment_line: **OrderNbr, LineNbr, ShipmentNbr, ShipLineNbr, ShipDate, ShipStatus, ShipLineQty, AcuShipLink**
            - df_packages: **OrderNbr, ShipmentNbr, Tracking, TrackingCreated**
            
        <hr>
        
        Returns
        ---
        :return orders (list): full list of formatted orders to be sent to Kustomer
        '''   
        df_order_line = self.order_detail_extract.sql(
            f''' 
            select OrderNbr
                , LineNbr
                , InventoryCD
                , Descr
                , LineQty
                , Warehouse 
            from self
        ''').unique()
        df_shipment_line = self.order_detail_extract.sql(
            f'''
            select OrderNbr
                , LineNbr
                , ShipmentNbr
                , ShipLineNbr
                , ShipDate
                , ShipStatus
                , ShipLineQty
                , AcuShipLink
            from self
        ''').unique()
        df_packages = self.order_detail_extract.sql(
            f'''
            select OrderNbr
                , ShipmentNbr
                , Tracking
                , TrackingCreated
            from self
        ''').unique()
        bp = 'here'
        orders = self.smash_orders(df_order_line, df_shipment_line, df_packages)

        return orders


    def smash_orders(self, df_order_line: pl.DataFrame, df_shipment_line: pl.DataFrame, df_packages: pl.DataFrame) -> list:
        '''`smash_orders`(self, df_order_line: *pl.DataFrame*, df_shipment_line: *pl.DataFrame*, df_packages: *pl.DataFrame*)
        ---
        <hr>
        
        Using the three DataFrames created in :meth:`~add_shipments_to_orders` add LineItems to `orders`
         - Within LineItems, add `Shipments`, and within `Shipments`, add `Packages`

        >>> order[0] = {
            'OrderNbr': '',
            'LineItems': [
                {
                    'Shipments': [
                        {
                            'Packages': []
                        }
                    ]
                }
            ]
        }
        
        ### Downstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
        
        ### Upstream Calls 
         #### :meth:`~folder.file.class.method`
            - Description
            
        <hr>
        
        Parameters
        ---
        :param (*pl.DataFrame*) `df_order_line`: _description_
        :param (*pl.DataFrame*) `df_shipment_line`: _description_
        :param (*pl.DataFrame*) `df_packages`: _description_
        
        <hr>
        
        Returns
        ---
        :return `variablename` (list): _description_
        '''        
        orders = self.orders.copy()
        for order in orders:
            order_nbr = order['OrderNbr']            
            line_items = df_order_line.sql(
                f'''
                select LineNbr
                     , InventoryCD
                     , Descr
                     , LineQty
                     , Warehouse
                from self
                where OrderNbr = '{order_nbr}'
            ''').to_dicts()
            for line in line_items:
                line_nbr = line['LineNbr']                
                shipments = df_shipment_line.sql(
                    f'''
                    select ShipmentNbr
                         , ShipLineNbr
                         , ShipDate
                         , ShipStatus
                         , ShipLineQty
                         , AcuShipLink
                    from self
                    where OrderNbr = '{order_nbr}' and LineNbr = {line_nbr}
                ''').to_dicts()
                
                # For each shipment, attach its packages
                for shipment in shipments:
                    shipment_nbr = shipment['ShipmentNbr']

                    shipment['Packages'] = df_packages.sql(
                        f'''
                        select Tracking, TrackingCreated
                        from self
                        where OrderNbr = '{order_nbr}' and ShipmentNbr = '{shipment_nbr}'
                    ''').to_dicts()

                line['Shipments'] = shipments  

            order['LineItems'] = line_items
            
        return orders




info_States = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AS": "American Samoa",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "DC": "District Of Columbia",
    "FM": "Federated States Of Micronesia",
    "FL": "Florida",
    "GA": "Georgia",
    "GU": "Guam",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MH": "Marshall Islands",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "MP": "Northern Mariana Islands",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PW": "Palau",
    "PA": "Pennsylvania",
    "PR": "Puerto Rico",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VI": "Virgin Islands",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming"
}