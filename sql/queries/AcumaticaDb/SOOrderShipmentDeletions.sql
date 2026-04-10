select OrderType
     , OrderNbr
     , ShipmentNbr
     , ShipDate
     , DeletedBy
     , DeletedDatetime
from SOOrderShipmentDeletions
-- where DeletedDatetime >= dateadd(hour, -2, getdate())