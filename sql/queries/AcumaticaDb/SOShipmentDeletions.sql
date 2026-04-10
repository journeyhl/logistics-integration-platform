select ShipmentNbr
     , ShipDate
     , DeletedBy
     , DeletedDatetime
from SOShipmentDeletions
-- where DeletedDatetime >= dateadd(hour, -2, getdate())