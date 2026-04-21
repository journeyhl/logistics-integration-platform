import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import GetReceiptsFromRMI





rmi_receipts = GetReceiptsFromRMI()
rmi_receipts_result = rmi_receipts.run()
bp = 'here'

