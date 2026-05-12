import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import HubSpotProperties

hs_properties = HubSpotProperties()

completed_hs_properties = hs_properties.run()

bp = 'here'
