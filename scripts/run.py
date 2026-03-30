import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, SendShipments, SendReturns, GetReceiptsFromRMI, GetClosedShipmentsFromRMI, GetStatusFromRMI, CreateAcuReceipt, StageRMIStatusRetrieval

 
# rmi_shipments = SendShipments()
# shipments_result = rmi_shipments.run()


# rmi_returns = SendReturns()
# returns_result = rmi_returns.run()


# rmi_closed_shipments = GetClosedShipmentsFromRMI()
# rmi_closed_shipments_result = rmi_closed_shipments.run()

# rmi_receipts = GetReceiptsFromRMI()
# rmi_receipts_result = rmi_receipts.run()

# rma_status_staging_pipeline = StageRMIStatusRetrieval()
# completed_rma_status_staging_pipeline = rma_status_staging_pipeline.run()
# rma_numbers = completed_rma_status_staging_pipeline['loaded']
# status_retrieval_pipeline = GetStatusFromRMI()
# test = 1
# for RMANumber in rma_numbers:
#     status_retrieval_pipeline.logger.info(RMANumber)
#     status_retrieval_pipeline._re_init(RMANumber)
#     status_retrieval_pipeline.run()
    

bp = 'here'
acu_receipt_creation = CreateAcuReceipt()
acu_receipt_creation_result = acu_receipt_creation.run()

bp = 'here'
