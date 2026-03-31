import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import PackShipments


pack_shipments = PackShipments()
pack_shipments.run()
bp = 'here'

# acu = AcumaticaAPI('acu')