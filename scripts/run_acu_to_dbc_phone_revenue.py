import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AcuToDbcPhoneRevenue




phone_rev_to_dbc = AcuToDbcPhoneRevenue('.debug')
completed_phone_rev_to_dbc = phone_rev_to_dbc.run()

