from __future__ import annotations
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from pipelines.address_validator import AddressValidator
import logging
import polars as pl

class Transform:    
    def __init__(self, pipeline: AddressValidator):
        self.pipeline = pipeline
        self.logger = logging.getLogger(f'{pipeline.pipeline_name}.transform')
        pass
    
    def transform(self, data_extract: pl.DataFrame):
        data_transformed = []
        for order in data_extract.iter_rows(named=True):
            if order['Match'] == 1:
                self.logger.info(f'{order['OrderNbr']}: Customer has same original Shipping/ Billing address')
                order_avs = self.pipeline.avs.validate(order_data=order, s_or_b='s')
            else:
                self.logger.info(f'{order['OrderNbr']}: Customer has different original Shipping/ Billing addresses')
                order_avs = self.pipeline.avs.validate(order_data=order, s_or_b='s') #validate both
                order_avs = self.pipeline.avs.validate(order_data=order, s_or_b='b') #validate both
            if order_avs.get(f'vsAddressLine1') == None:
                self.logger.error(f'{order_avs['OrderNbr']}: No AddressLine1 returned from AVS')
                bp = 'ERROR'
            else:
                order_avs['update_order_address_payload'] = self.format_order_address_payload(order_avs)
                order_avs['acu_api_log_update_override'] = self.format_acu_api_log_update_override(order_avs)
                order_avs['acu_api_log_validate'] = self.format_acu_api_log_validate(order_avs)
                # order_avs['update_customer_address_payload'] = self.format_customer_address_payload(order_avs)
            data_transformed.append(order_avs)
        return data_transformed

    
    
    def format_order_address_payload(self, order_avs: dict):
        '''
        `format_order_address_payload`(self, order_avs: *dict*)
        ---
        <hr>
        
        Given a dictionary containing a response from AVS, format the payload needed to override and update a Customer's ShipTo Address on a particular **Order**
        
        ### Downstream Function Calls 
         #### :meth:`~_log_differences`
            - Calls out changes that will be made to Order's ShipTo Address in Acumatica
        
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_avs`: Dictionary containing **OrderType**, **OrderNbr**, **CustomerID**, and a **formatted AVS address response**

         - **vAddressLine1**, **vAddressLine2**, **vCity**, **vState**, **vPostalCode**, **vCountryID**
        
        <hr>
        
        Returns
        ---
        :return `payload` (_type_): payload ready to send to Acumatica API (SalesOrder endpoint)
        '''
        payload = {
            "OrderType":   { "value": order_avs['OrderType'] },
            "OrderNbr":    { "value": order_avs['OrderNbr'] },
            "CustomerID": {"value": order_avs['AcctCD']},
            "ShipToAddressOverride": { "value": True },
            "ShipToAddress": {
                "AddressLine1": {"value": order_avs['vsAddressLine1']},
                "AddressLine2": {"value": order_avs['vsAddressLine2']},
                "City":         {"value": order_avs['vsCity']},
                "State":        {"value": order_avs['vsState']},
                "PostalCode":   {"value": order_avs['vsPostalCode']},
                "Country":      {"value": order_avs['vsCountryID']},                
            }
        }
        if order_avs['Match'] == 1:
            payload['BillToAddressOverride'] = { "value": True }            
            payload['BillToAddress'] = {
                "AddressLine1": {"value": order_avs['vsAddressLine1']},
                "AddressLine2": {"value": order_avs['vsAddressLine2']},
                "City":         {"value": order_avs['vsCity']},
                "State":        {"value": order_avs['vsState']},
                "PostalCode":   {"value": order_avs['vsPostalCode']},
                "Country":      {"value": order_avs['vsCountryID']},                
            }
        elif order_avs['Match'] == 0:
            payload['BillToAddressOverride'] = { "value": True }            
            payload['BillToAddress'] = {
                "AddressLine1": {"value": order_avs['vbAddressLine1']},
                "AddressLine2": {"value": order_avs['vbAddressLine2']},
                "City":         {"value": order_avs['vbCity']},
                "State":        {"value": order_avs['vbState']},
                "PostalCode":   {"value": order_avs['vbPostalCode']},
                "Country":      {"value": order_avs['vbCountryID']},                
            }
        self._log_differences(order_avs)
        return payload
    
    def format_acu_api_log_update_override(self, order_avs: dict):
        '''`format_acu_api_log_update_override`(self, order_avs: *dict*)
        ---
        <hr>
        
        Formats the constant part of the dict of data that we'll load to **_util.acu_api_log** when overriding and updating an address
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_avs`: Dictionary containing **OrderNbr**, and **update_order_address_payload** (the return from :meth:`~format_order_address_payload`)
        
        <hr>
        
        Returns
        ---
        :return `data_log_entry` (dict): _description_
        '''
        data_log_entry = {            
            'Entity': 'SalesOrder',
            'KeyValue': f'{order_avs['OrderNbr']}',
            'Operation': f'PUT - Override & Update Address',
            'Payload': order_avs['update_order_address_payload'],
        }
        return data_log_entry
    
    def format_acu_api_log_validate(self, order_avs: dict):
        '''`format_acu_api_log_validate`(self, order_avs: *dict*)
        ---
        <hr>
        
        Formats the constant part of the dict of data that we'll load to **_util.acu_api_log** when
            
        <hr>
        
        Parameters
        ---
        :param (*dict*) `order_avs`: Dictionary containing **OrderNbr**, and **update_order_address_payload** (the return from :meth:`~format_order_address_payload`)
        
        <hr>
        
        Returns
        ---
        :return `data_log_entry` (dict): constant values used for _util.acu_aupi_log
        '''
        data_log_entry = {            
            'Entity': 'SalesOrder',
            'KeyValue': f'{order_avs['OrderNbr']}',
            'Operation': f'POST - Validate Address',
        }
        return data_log_entry





    
    def _log_differences(self, order_avs):
        self.logger.info(f'{order_avs['OrderNbr']}: Comparing AVS address to original...')
        if order_avs['Match'] == 1:
            for (current, new, name) in [
                (order_avs['sAddressLine1'], order_avs['vsAddressLine1'], 'AddressLine1'),
                (order_avs['sAddressLine2'], order_avs['vsAddressLine2'], 'AddressLine2'),
                (order_avs['sCity'], order_avs['vsCity'], 'City'),
                (order_avs['sState'], order_avs['vsState'], 'State'),
                (order_avs['sPostalCode'], order_avs['vsPostalCode'], 'PostalCode'),
                (order_avs['sCountryID'], order_avs['vsCountryID'], 'CountryID'),
            ]:
                if current != new:
                    self.logger.info(f'{order_avs['OrderNbr']}: {name}: {current} -> {new}')
                    bp = 'here'
                bp = 'here'
        elif order_avs['Match'] == 0:
            for (current, new, name) in [
                (order_avs['sAddressLine1'], order_avs['vsAddressLine1'], 'ShipToAddressLine1'),
                (order_avs['sAddressLine2'], order_avs['vsAddressLine2'], 'ShipToAddressLine2'),
                (order_avs['sCity'], order_avs['vsCity'], 'ShipToCity'),
                (order_avs['sState'], order_avs['vsState'], 'ShipToState'),
                (order_avs['sPostalCode'], order_avs['vsPostalCode'], 'ShipToPostalCode'),
                (order_avs['sCountryID'], order_avs['vsCountryID'], 'ShipToCountryID'),
                (order_avs['bAddressLine1'], order_avs['vbAddressLine1'], 'BillToAddressLine1'),
                (order_avs['bAddressLine2'], order_avs['vbAddressLine2'], 'BillToAddressLine2'),
                (order_avs['bCity'], order_avs['vbCity'], 'BillToCity'),
                (order_avs['bState'], order_avs['vbState'], 'BillToState'),
                (order_avs['bPostalCode'], order_avs['vbPostalCode'], 'BillToPostalCode'),
                (order_avs['bCountryID'], order_avs['vbCountryID'], 'BillToCountryID'),
            ]:
                if current != new:
                    self.logger.info(f'{order_avs['OrderNbr']}: {name}: {current} -> {new}')
                    bp = 'here'
                bp = 'here'



#region Delete?
    def format_customer_address_payload(self, order_avs: dict):
        payload = {
            "CustomerID": {"value": order_avs['AcctCD']},
            "MainContact": {
                "Address": {
                    "AddressLine1": {"value": order_avs['vAddressLine1']},
                    "AddressLine2": {"value": order_avs['vAddressLine2']},
                    "City":         {"value": order_avs['vCity']},
                    "State":        {"value": order_avs['vState']},
                    "PostalCode":   {"value": order_avs['vPostalCode']},
                    "Country":    {"value": order_avs['vCountryID']},
                }
            }
        }
        # order_avs['vAddressLine2'] = None if order_avs['AddressLine2'] == None and order_avs['vAddressLine2'] == '' else order_avs['vAddressLine2']
        # order_avs['vAddressLine2'] = '' if order_avs['AddressLine2'] == '' and order_avs['vAddressLine2'] == None else order_avs['vAddressLine2']
        # if order_avs['AddressLine2'] != order_avs['vAddressLine2']:
        #     payload['MainContact']['Address']['AddressLine2'] ={"value": order_avs['vAddressLine2']}
        self._log_differences(order_avs)
        return payload
#endregion Delete? 