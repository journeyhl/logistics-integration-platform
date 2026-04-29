import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AfterShipRetrieval

aftership = AfterShipRetrieval()
completed_aftership = aftership.run()

bp = 'here'
