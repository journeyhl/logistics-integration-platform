select *
from rmi_RMAStatus s
where s.RMAType = '3' and s.RMAStatus = 'CLOSED' and s.DFStatus = 'RECEIVED'