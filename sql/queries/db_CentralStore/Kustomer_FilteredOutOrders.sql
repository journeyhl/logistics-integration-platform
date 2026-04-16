select *,  DATEADD(MINUTE, -60, cast(getdate() at time zone 'UTC' at time zone 'Eastern Standard Time' as datetime)) Cutoff
, concat(OrderNbr, '-', AcuStatus) OrderConcat
from json.K_OrderIngest
where DatetimeSent >= DATEADD(MINUTE, -60, cast(getdate() at time zone 'UTC' at time zone 'Eastern Standard Time' as datetime))