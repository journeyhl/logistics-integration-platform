import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connectors import RedStagAPI
from pipelines import SendRedStagShipments


redstag = SendRedStagShipments()
redstag.run()

bp = 'here'



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

