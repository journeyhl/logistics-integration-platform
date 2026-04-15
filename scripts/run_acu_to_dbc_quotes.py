import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import AcuToDbcQuotes



quotes_to_dbc = AcuToDbcQuotes()
completed_quotes_to_dbc = quotes_to_dbc.run()

bp = 'here'