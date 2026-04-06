import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, GetReceiptsFromRMI, GetClosedShipmentsFromRMI, GetStatusFromRMI, CreateAcuReceipt
from connectors import SQLConnector, AcumaticaAPI
import polars as pl



acudb = SQLConnector(pipeline='pipe', database_name='AcumaticaDb')
acuapi = AcumaticaAPI(pipeline='pipe')

validate = acudb.query_to_dataframe(acudb.queries.ValidateAddresses)

for order in validate.iter_rows(named=True):
    OrderNbr = order['OrderNbr']
    AcctCD = order['AcctCD']
    ContactID = order['CustomerContactID']
    AddressID = order['CustomerAddressID']
    ContactID2 = order['ContactID']
    cNoteID = order['cNoteID']
    aNoteID = order['aNoteID']
    response = acuapi.validate_address(ContactID)
    bp = 'here'





bp = 'here'