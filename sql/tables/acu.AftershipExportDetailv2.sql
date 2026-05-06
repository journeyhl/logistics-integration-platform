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
where t.name = 'AftershipExportDetailv2' and s.name = 'acu'
)
create table acu.AftershipExportDetailv2(
OrderNbr varchar(255) not null,
Tracking varchar(255) not null,
ShipmentNbr varchar(30) not null,
ID varchar(100) not null,
CheckpointTime datetime not null,
CheckpointCreatedTime datetime not null,
Message varchar(155),
Tag varchar(35),
Subtag varchar(35),
SubtagMessage varchar(55),
City varchar(85),
State varchar(55),
Zip varchar(15),
Slug varchar(15),
RawTag varchar(3),
Source varchar(30),
Primary key(OrderNbr, Tracking, ShipmentNbr, ID, CheckpointTime, CheckpointCreatedTime))