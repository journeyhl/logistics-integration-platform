if not exists(
select * 
from sys.tables t 
where t.name = 'RedstagInventoryDetail'
)
begin
create table RedstagInventoryDetail(
InventoryCD varchar(40),
Warehouse varchar(50),
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
primary key (InventoryCD, Warehouse),
foreign key (InventoryCD) references RedstagInventorySummary(InventoryCD))
end