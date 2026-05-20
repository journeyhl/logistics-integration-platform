import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import SendToAfterShip

aftership = SendToAfterShip('.debug')

completed_aftership = aftership.run()

bp = 'here'
