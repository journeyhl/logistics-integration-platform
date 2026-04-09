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
where t.name = 'campaign_performance_daily' and s.name = 'criteo'
) 
create table criteo.diff_log(
report_date         date not null,
campaign_id         int not null default -1,
last_ts             datetime not null,
current_ts          datetime,
impressions_diff    int,
clicks_diff         int,
cost_diff           decimal (18,4),
revenue_diff        decimal(18,2),
primary key(report_date, campaign_id, last_ts)
)