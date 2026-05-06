if not exists(
select *
from sys.schemas s
where s.name = 'acu'
)
exec('create schema acu');
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'AftershipExportv2' and s.name = 'acu'
)
create table acu.AftershipExportv2(
OrderNbr varchar(255) not null,
Tracking varchar(255) not null,
ShipmentNbr varchar(30) not null,
ID varchar(100) not null,
Phone varchar(255),
Product varchar(255),
ShipmentType varchar(50),
ShipmentWeight decimal(18,1),
Link varchar(255),
DestLine1 varchar(255),
DestCity varchar(255),
DestState varchar(255),
DestZip varchar(20),
Tag varchar(255),
Subtag varchar(255),
SubtagMessage varchar(255),
CreatedTime datetime,
LastUpdateTime datetime,
Primary key(OrderNbr, Tracking, ShipmentNbr, ID))