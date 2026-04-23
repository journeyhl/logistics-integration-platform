import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import CreateAcuReceipt


acu_receipt_creation = CreateAcuReceipt()
acu_receipt_creation_result = acu_receipt_creation.run()

