if not exists(
select *
from sys.schemas s
where s.name = '_util'
)
exec('create schema _util');
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'SOOrderShipmentDeletions' and s.name = '_util'
)
create table _util.SOOrderShipmentDeletions(
OrderType varchar(2),
OrderNbr varchar(15),
ShipmentNbr varchar(15),
ShipDate date,
DeletedBy varchar(200),
DeletedDatetime datetime,
primary key(OrderType, OrderNbr, ShipmentNbr))
