import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import AcumaticaAPI



acu = AcumaticaAPI('acu')
customer = acu.customers(query="MainContact/Phone1 eq '(804) 123-1234' or MainContact/Phone2 eq '8041231234'")
contact = acu.contact(query="Phone1 eq '(804) 123-1234' or Phone2 eq '(804) 123-1234'")
bp = 'here'
