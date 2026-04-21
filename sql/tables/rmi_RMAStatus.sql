if not exists(
select * 
from sys.tables t 
where t.name = 'rmi_RMAStatus'
)
begin
create table rmi_RMAStatus(
RMANumber varchar(55),
RMAID		int,
RMALineID	int,
RMAType varchar(5),
RMAStatus varchar(35),
CustomerRef varchar(100),
RMALineNbr int,
LineStatus varchar(35),
InventoryCD varchar(35),
Qty int,
Descr varchar(255),
RMATypeName varchar(100),
CreateDate datetime,
RMILastModifiedDate datetime,
LastChecked datetime,
Carrier varchar(100),
Priority varchar(100),
Primary Key(RMANumber, RMAID, RMALineID, RMAType)
)
end