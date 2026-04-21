
with AllItemsApart as(
select distinct RMANumber
from rmi_ClosedShipments s
union
select distinct RMANumber
from rmi_Receipts
union
select distinct KeyValue
from _util.rmi_send_log
where right(RMI_Response, 19) != 'Item does not exist'
)
, AllItems as(
select distinct RMANumber
from AllItemsApart 
)
select distinct a.RMANumber, s.LastChecked, s.RMAStatus, s.LineStatus
from AllItems a
left join rmi_RMAStatus s on a.RMANumber = s.RMANumber
where (s.RMAStatus not in('CLOSED', '') or s.RMAStatus is null)
or (s.LastChecked >= getdate()-2 and RMAStatus != 'OPEN')
order by s.LastChecked desc
