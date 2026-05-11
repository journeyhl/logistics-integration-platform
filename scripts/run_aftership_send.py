import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import SendToAfterShip, UpdateAfterShip

aftership = UpdateAfterShip()
# aftership = SendToAfterShip()

completed_aftership = aftership.run()

bp = 'here'

# aftership_data = aftership.aftership.get_data(aftership.aftership.tracking_endpoint)
# bp = 'here'