select *
from SOOrderDeletions
where DeletedDatetime >= dateadd(hour, -2, getdate())