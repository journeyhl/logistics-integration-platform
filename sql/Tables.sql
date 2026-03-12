create table rmi_ClosedShipments(
RMANumber varchar(55),
RMAID		int,
RMALineID int,
RMAType varchar(5),
CreateDate datetime,
ShipDate datetime,
InventoryCD varchar(35),
QtyShipped int,
QtyToShip int,
Location varchar(50),
ItemCategory varchar(100),
Descr varchar(255),
Carrier varchar(50),
CarrierCode varchar(15),
Priority varchar(50),
Tracking varchar(50),
FreightCost decimal(18,2),
OutboundShipMethod varchar(50),
Primary Key(RMANumber, RMAID, RMALineID, RMAType)
)

create table rmi_Receipts(
RMANumber varchar(55),
ReceiptDate datetime,
ReceiptID int,
RMAID int,
RMALineID int,
Qty int,
InventoryCD varchar(35),
Location varchar(50),
ItemType varchar(50),
ItemCategory varchar(100),
Descr varchar(255),
Price decimal(18,2),
Cost decimal(18,2),
Primary Key(RMANumber, ReceiptID, RMAID, RMALineID)
)

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
