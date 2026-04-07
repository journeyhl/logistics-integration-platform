import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, GetReceiptsFromRMI, GetClosedShipmentsFromRMI, GetStatusFromRMI, CreateAcuReceipt
from connectors import SQLConnector, AcumaticaAPI, AddressVerificationSystem
import polars as pl



acudb = SQLConnector(pipeline='pipe', database_name='AcumaticaDb')
acuapi = AcumaticaAPI(pipeline='pipe')
avs = AddressVerificationSystem(pipeline='pipe')

validate = acudb.query_to_dataframe(acudb.queries.ValidateAddresses)

for order in validate.iter_rows(named=True):
    test = avs.validate(order)
    response = acuapi.validate_address(order)
    bp = 'here'





bp = 'here'