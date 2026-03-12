from pipelines import Pipeline, GetReceiptsFromRMI, GetClosedShipmentsFromRMI


rmi_closed_shipments = GetClosedShipmentsFromRMI()
rmi_closed_shipments_result = rmi_closed_shipments.run()
bp = 'here'


rmi_receipts = GetReceiptsFromRMI()
rmi_receipts_result = rmi_receipts.run()
bp = 'here'
