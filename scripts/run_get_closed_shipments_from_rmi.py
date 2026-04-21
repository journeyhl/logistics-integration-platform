import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import GetClosedShipmentsFromRMI




rmi_closed_shipments = GetClosedShipmentsFromRMI()
rmi_closed_shipments_result = rmi_closed_shipments.run()
bp = 'here'

