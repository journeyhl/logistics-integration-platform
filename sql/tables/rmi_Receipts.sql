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