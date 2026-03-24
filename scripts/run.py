import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, SendShipments, SendReturns, GetReceiptsFromRMI, GetClosedShipmentsFromRMI, GetStatusFromRMI, CreateAcuReceipt

 
rmi_shipments = SendShipments()
shipments_result = rmi_shipments.run()


rmi_returns = SendReturns()
returns_result = rmi_returns.run()


rmi_closed_shipments = GetClosedShipmentsFromRMI()
rmi_closed_shipments_result = rmi_closed_shipments.run()

rmi_receipts = GetReceiptsFromRMI()
rmi_receipts_result = rmi_receipts.run()

rmi_statuses = GetStatusFromRMI()
for RMANumber in rmi_statuses.data:
    rmi_statuses.logger.info(RMANumber)
    rmi_statuses._re_init(RMANumber)
    data_extract = rmi_statuses.extract()
    data_transformed = rmi_statuses.transform(data_extract)
    data_loaded = rmi_statuses.load(data_transformed)

bp = 'here'
acu_receipt_creation = CreateAcuReceipt()
acu_receipt_creation_result = acu_receipt_creation.run()