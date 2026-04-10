if not exists(
select *
from sys.schemas s
where s.name = 'criteo'
)
exec('create schema criteo');
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'diff_log' and s.name = 'criteo'
) 
create table criteo.diff_log(
report_date         date not null,
campaign_id         int not null default -1,
last_ts             datetime not null,
current_ts          datetime,
impr_current int,
impr_last int,
clicks_current int,
clicks_last int,
cost_current decimal(18,4),
cost_last  decimal(18,4),
convr_current int,
convr_last int,
revenue_current  decimal(18,2),
revenue_last decimal(18,2),
diff bit,
primary key(report_date, campaign_id, last_ts)
)