
with TopLevel as(

	select json_value(jsonData, '$.topic') Topic
		 , json_value(jsonData, '$.message.order_unique_id')  ShipmentNbr_3pl
		 , json_query(jsonData, '$.message.packages') Packages
		 , json_query(jsonData, '$.message.trackers') Trackers
		 , json_query(jsonData, '$.message') msg
	from json.RedStagEvents
	where json_value(jsonData, '$.topic') = 'shipment:packed'

),
acuShipments as(

	select distinct ShipmentNbr
	from acu.Shipments s
	where s.Status not in('Completed', 'Confirmed', 'Invoiced')
	and OrderDate >= getdate()-365

)
select t.Topic
	 , t.ShipmentNbr_3pl
	 , t.Packages
	 , t.Trackers
	 , json_query(t.Packages, '$[*].items') Items
	 , json_query(t.Packages, '$[*].tracking_numbers') TrackingNumbers
	 , t.msg
from TopLevel t
inner join acuShipments a on t.ShipmentNbr_3pl = a.ShipmentNbr
where t.ShipmentNbr_3pl = '080584'