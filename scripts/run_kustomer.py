import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import SendOrderDetailsToKustomer

kustomer = SendOrderDetailsToKustomer()
kustomer._re_init()
bp = 'here'
kustomer._re_init('backfill')
