
with AllItemsApart as(
select distinct RMANumber
from rmi_ClosedShipments s
union
select distinct RMANumber
from rmi_Receipts
union
select distinct KeyValue
from rmi_send_log
)
, AllItems as(
select distinct RMANumber
from AllItemsApart 
)
select distinct a.RMANumber, s.LastChecked, s.RMAStatus, s.DFStatus
from AllItems a
left join rmi_RMAStatus s on a.RMANumber = s.RMANumber
where (s.RMAStatus not in('CLOSED', '') or s.RMAStatus is null)
and s.RMAStatus is null
order by s.LastChecked desc
