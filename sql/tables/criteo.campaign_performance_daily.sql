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
create table criteo.campaign_performance_daily(
    report_date      date           not null,
    advertiser_id    int            not null,
    campaign_id      int            not null default -1,
    campaign_name    decimal(255)   null,
    impressions      int            not null default 0,
    clicks           int            not null default 0,
    cost             decimal(10,4)  not null default 0,
    conversions      int            not null default 0,
    revenue          decimal(12,4)  not null default 0,
    load_timestamp   datetime       not null,
    constraint PK_criteo_campaign_perf
    primary key (report_date, campaign_id)
)