import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import RedStagInventory



redstag_inventory = RedStagInventory()
redstag_inventory.run()





