import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AddressValidator
from connectors import SQLConnector, AcumaticaAPI, AddressVerificationSystem
import polars as pl

addy_validator = AddressValidator()
completed_addy_validator = addy_validator.run()




bp = 'here'