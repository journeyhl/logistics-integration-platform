import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import GetStatusFromRMI, StageRMIStatusRetrieval




rma_status_staging_pipeline = StageRMIStatusRetrieval()
rmi_statuses = rma_status_staging_pipeline.run()
rma_numbers = rmi_statuses['loaded']    
status_retrieval_pipeline = GetStatusFromRMI()
for rma_number in rma_numbers:
    status_retrieval_pipeline._re_init(rma_number = rma_number)
    status_retrieval_pipeline.run()

