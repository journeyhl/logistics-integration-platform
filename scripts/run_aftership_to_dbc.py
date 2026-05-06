import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AfterShipToDbc

aftership = AfterShipToDbc()
completed_aftership = aftership.run()

bp = 'here'
