from pipelines import Pipeline, GetReceiptsFromRMI


rmi_receipts = GetReceiptsFromRMI()
rmi_receipts_result = rmi_receipts.run()
bp = 'here'


