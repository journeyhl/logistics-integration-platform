import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import PopulateShipmentDetails


shipment_details = PopulateShipmentDetails()
shipment_details.run()
bp = 'here'

# acu = AcumaticaAPI('acu')