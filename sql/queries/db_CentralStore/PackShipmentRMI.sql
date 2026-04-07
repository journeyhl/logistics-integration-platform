select replace(replace(replace(RMANumber, '-1', ''), '-2', ''), '-3', '') RMANumber
	 , r.InventoryCD
	 , r.QtyShipped
	 , r.QtyToShip
	 , r.ItemCategory
	 , r.Descr
	 , r.Carrier
	 , r.CarrierCode
	 , r.Priority
	 , r.Tracking
	 , count(replace(replace(replace(RMANumber, '-1', ''), '-2', ''), '-3', '')) over(partition by replace(replace(replace(RMANumber, '-1', ''), '-2', ''), '-3', '')) Lines
from rmi_ClosedShipments r
order by RMANumber