if not exists(
select * 
from sys.tables t 
where t.name = 'rmi_ClosedShipments'
)
begin
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
end