select *
from rmi_RMAStatus s
where s.RMAType = '3' and (s.RMAStatus = 'CLOSED' or s.LineStatus = 'RECEIVED')
order by CreateDate desc