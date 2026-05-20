import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import ShipmentsReadyToConfirm

confirms = ShipmentsReadyToConfirm('.debug')

confirm_load = confirms.run()

bp = 'here'