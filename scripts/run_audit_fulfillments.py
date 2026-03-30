import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import AcumaticaAPI
from pipelines import AuditFulfillment


audit_fulfillment = AuditFulfillment()
audit_fulfillment.run()
bp = 'here'
# acu = AcumaticaAPI('acu')