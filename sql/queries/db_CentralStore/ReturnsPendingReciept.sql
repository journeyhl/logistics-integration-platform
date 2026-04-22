select *
from rmi_RMAStatus s
where s.RMAType = '3' and (s.RMAStatus = 'CLOSED' or s.LineStatus = 'RECEIVED')
and s.RMILastModifiedDate >= getdate()-21
order by CreateDate desc