import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, SendRMIShipments, SendRMIReturns




rmi_shipments = SendRMIShipments()
shipments_result = rmi_shipments.run()
bp = 'here'

rmi_returns = SendRMIReturns()
returns_result = rmi_returns.run()
bp = 'here'