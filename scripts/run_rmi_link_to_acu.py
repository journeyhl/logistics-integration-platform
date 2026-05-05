import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import RMILinkToAcu

rmi_link = RMILinkToAcu()

completed_rmi_link = rmi_link.run()

bp = 'here'
