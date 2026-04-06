import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import SOOrderDeletions

order_deletions = SOOrderDeletions()

completed_order_deletions = order_deletions.run()

bp = 'here'
