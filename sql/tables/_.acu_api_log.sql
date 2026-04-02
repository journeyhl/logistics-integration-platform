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
where t.name = 'acu_api_log' and s.name = '_util'
) 
create table _util.acu_api_log(
Entity varchar(100),
KeyValue varchar(150),
Operation varchar(100),
Payload varchar(max),
Acu_Response varchar(250),
Timestamp datetime)