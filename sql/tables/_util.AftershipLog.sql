
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
where t.name = 'AftershipLog' and s.name = '_util'
)
create table _util.AftershipLog(
ShipmentNbr varchar(25) not null,
OrderNbr varchar(25) not null,
Tracking varchar(150) not null,
ResponseCode varchar(15),
ID varchar(65),
Source varchar(35),
Tag varchar(35),
Subtag varchar(35),
SubtagMsg varchar(35),
Created datetime,
Updated datetime,
CourierTrackingLink varchar(355),
CourierRedirectLink varchar(355),
primary key(ShipmentNbr, OrderNbr, Tracking))
