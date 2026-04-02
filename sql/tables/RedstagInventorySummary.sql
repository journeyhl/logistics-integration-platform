if not exists(
select * 
from sys.tables t 
where t.name = 'RedstagInventorySummary'
)
begin
create table RedstagInventorySummary(
InventoryCD varchar(40),
Expected int,
Processed int,
PutAway int,
Available int,
Allocated int,
Reserved int,
Picked int,
Advertised int,
OnHand int,
Timestamp datetime,
primary key (InventoryCD))
end