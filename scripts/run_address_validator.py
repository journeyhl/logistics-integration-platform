import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AddressValidator
from connectors import SQLConnector, AcumaticaAPI, AddressVerificationSystem
import polars as pl

addy_validator = AddressValidator()
completed_addy_validator = addy_validator.run()

acudb = SQLConnector(pipeline='pipe', database_name='AcumaticaDb')
acuapi = AcumaticaAPI(pipeline='pipe')
avs = AddressVerificationSystem(pipeline='pipe')

# validate = acudb.query_to_dataframe(acudb.queries.ValidateAddresses)

# for order in validate.iter_rows(named=True):
#     order_avs = avs.validate(order)
#     if order_avs.get('vAddressLine1') == None:
#         bp = 'ERROR'
#     else:
#         response = acuapi.validate_customer_address(order)
#     bp = 'here'





bp = 'here'