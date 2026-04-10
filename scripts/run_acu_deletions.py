import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AcumaticaDeletions

acu_deletions = AcumaticaDeletions()

completed_acu_deletions = acu_deletions.run()

bp = 'here'
