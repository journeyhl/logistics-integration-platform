from pipelines import Pipeline, SendShipments, FormatData, SendReturns


rmi_shipments = SendShipments()
shipments_result = rmi_shipments.run()
bp = 'here'



rmi_returns = SendReturns()
returns_result = rmi_returns.run()
bp = 'here'