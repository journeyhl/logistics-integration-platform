create table rmi_RMAStatus(
RMANumber varchar(55),
RMAID		int,
RMALineID	int,
RMAType varchar(5),
RMAStatus varchar(35),
CustomerRef varchar(100),
RMALineNbr int,
DFStatus varchar(35),
InventoryCD varchar(35),
Qty int,
Descr varchar(255),
RMATypeName varchar(100),
CreateDate datetime,
RMILastModifiedDate datetime,
LastChecked datetime,
Primary Key(RMANumber, RMAID, RMALineID, RMAType)
)
