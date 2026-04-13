create trigger TriggerSOOrderDelete
on SOOrder
after delete as
begin
insert into SOOrderDeletions(OrderType, OrderNbr, DeletedBy, DeletedDatetime)
select d.OrderType
	 , d.OrderNbr
	 , (select replace(u.Username, 'journeyhl.com\', '') from Users u where u.PKID = d.LastModifiedByID and u.CompanyID = d.CompanyID) DeletedBy
	 , GETDATE() DeletedDatetime
from deleted d where d.CompanyID = 2 and d.OrderType != 'QT'
end