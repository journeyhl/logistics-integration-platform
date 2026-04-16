with RowNums as(
select distinct s.OrderNumber OrderNbr
     , s.Status
     , s2.Status AltStatus
     , case when s.Status = 'Completed' then 1
	   when s.Status = 'Shipping' then 2
	   when s.Status = 'Back Order' then 3
	   when s.Status = 'Open' then 4
	   when s.Status = 'Awaiting Payment' then 5
	   else 6 end StatusRank
     , ROW_NUMBER() over(partition by s.OrderNumber order by case when s.Status = 'Completed' then 1
	   when s.Status = 'Shipping' then 2
	   when s.Status = 'Back Order' then 3
	   when s.Status = 'Open' then 4
	   when s.Status = 'Awaiting Payment' then 5
	   else 6 end) rn
from acu.SalesOrders s
inner join acu.SalesOrders s2 on s.OrderNumber = s2.OrderNumber and s.Status != s2.status
)
select *
from RowNums
where rn = 1
order by StatusRank, OrderNbr