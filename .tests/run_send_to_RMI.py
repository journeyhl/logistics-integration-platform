import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import Pipeline, SendShipments, SendReturns




rmi_shipments = SendShipments()
shipments_result = rmi_shipments.run()
bp = 'here'

rmi_returns = SendReturns()
returns_result = rmi_returns.run()
bp = 'here'