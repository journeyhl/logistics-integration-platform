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
        for order in data_extract.iter_rows(named=True):
            order_avs = self.pipeline.avs.validate(order)
            if order_avs.get('vAddressLine1') == None:
                bp = 'ERROR'
            else:
                order_avs['update_address_payload'] = self.format_payload(order_avs)
                validate_address = self.pipeline.acu_api.update_customer_address(order['update_address_payload'])
                if validate_address:
                    self.pipeline.acu_api.validate_customer_address(order_avs)
                    bp = 'here'
        bp = 'here'


    def format_payload(self, order_avs: dict):
        payload = {
            "CustomerID": {"value": order_avs['AcctCD']},
            "MainContact": {
                "Address": {
                    "AddressLine1": {"value": order_avs['vAddressLine1']},
                    "City":         {"value": order_avs['vCity']},
                    "State":        {"value": order_avs['vState']},
                    "PostalCode":   {"value": order_avs['vPostalCode']},
                    "Country":    {"value": order_avs['vCountryID']},
                }
            }
        }
        if order_avs['vAddressLine2'] != '' and order_avs['vAddressLine2'] != None:
            payload['MainContact']['Address']['AddressLine2'] = order_avs['vAddressLine2']
        self._log_differences(order_avs)
        return payload
    
    def _log_differences(self, order_avs):
        for (current, new, name) in [
            (order_avs['AddressLine1'], order_avs['vAddressLine1'], 'AddressLine1'),
            (order_avs['AddressLine2'], order_avs['vAddressLine2'], 'AddressLine2'),
            (order_avs['City'], order_avs['vCity'], 'City'),
            (order_avs['State'], order_avs['vState'], 'State'),
            (order_avs['PostalCode'], order_avs['vPostalCode'], 'PostalCode'),
            (order_avs['CountryID'], order_avs['vCountryID'], 'CountryID'),
        ]:
            if current != new:
                self.logger.info(f'Updated {name}! {current} -> {new}')
                bp = 'here'
            bp = 'here'
