import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import NotifyFulfillmentOps

fot_notification = NotifyFulfillmentOps('.debug')

completed_fot_notification = fot_notification.run()

bp = 'here'
