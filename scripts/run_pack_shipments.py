import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import PackShipment


pack_shipment = PackShipment()
pack_shipment.run()
bp = 'here'

# acu = AcumaticaAPI('acu')