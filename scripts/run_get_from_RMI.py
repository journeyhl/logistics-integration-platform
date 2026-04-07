import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, GetReceiptsFromRMI, GetClosedShipmentsFromRMI, GetStatusFromRMI, CreateAcuReceipt, StageRMIStatusRetrieval




rmi_closed_shipments = GetClosedShipmentsFromRMI()
rmi_closed_shipments_result = rmi_closed_shipments.run()
bp = 'here'


rmi_receipts = GetReceiptsFromRMI()
rmi_receipts_result = rmi_receipts.run()
bp = 'here'


rma_status_staging_pipeline = StageRMIStatusRetrieval()
rmi_statuses = rma_status_staging_pipeline.run()
rma_numbers = rmi_statuses['loaded']    
status_retrieval_pipeline = GetStatusFromRMI()
for rma_number in rma_numbers:
    status_retrieval_pipeline._re_init(rma_number = rma_number)
    status_retrieval_pipeline.run()

# acu_receipt_creation = CreateAcuReceipt()
# acu_receipt_creation_result = acu_receipt_creation.run()
