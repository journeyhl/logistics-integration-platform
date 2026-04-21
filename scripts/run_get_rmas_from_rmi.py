import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import GetRMAsFromRMI





rmi_rmas = GetRMAsFromRMI()
rmi_rmas_result = rmi_rmas.run()
bp = 'here'

