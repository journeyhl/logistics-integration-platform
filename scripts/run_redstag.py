import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines import SendRedStagShipments, RedStagInventory




redstag_inventory = RedStagInventory()
redstag_inventory.run()

bp = 'here'

send_redstag = SendRedStagShipments()
send_redstag.run()




bp = 'here'





#region misc
payload_target = [    
    "order.search",
    [
        {
            "unique_id" : {
                "eq" : "078828"
                }
        },
        None,
        [
            "state"
        ]
    ]
]
#endregion


