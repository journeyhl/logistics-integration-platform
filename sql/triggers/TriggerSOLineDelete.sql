create trigger TriggerSOLineDelete
on SOLine
after delete as
begin
insert into SOLineDeletions(
CompanyID, 
OrderType, 
OrderNbr, 
LineNbr,  
OrderDate, 
DeletedDatetime, 
DeletedBy, 
InventoryID, 
SubItemID, 
SiteID, 
LocationID, 
OrderQty, 
ShippedQty, 
UnitPrice, 
UnitCost, 
ExtPrice, 
ExtCost, 
DiscPct, 
DiscAmt, 
ManualDisc, 
ReasonCode, 
OpenQty, 
OpenAmt, 
LineAmt, 
Completed, 
OrigOrderType, 
OrigOrderNbr,
OrigLineNbr, 
OrigShipmentType, 
OpenLine, 
BilledQty, 
UnbilledQty, 
UnbilledAmt, 
ShipDate, 
CancelDate, 
BilledAmt, 
DiscountID, 
DiscountSequenceID, 
InvoiceType, 
InvoiceNbr, 
InvoiceLineNbr, 
InvoiceDate, 
IsFree, 
POCreate, 
POCreated, 
POSource, 
POSiteID, 
QtyOnOrders,
CustomerOrderNbr, 
POCreateDate, 
IsStockItem, 
NetSales, 
MarginAmt, 
MarginPct, 
CreatedDatetime, 
CreatedBy)
select CompanyID
     , OrderType
     , OrderNbr
     , LineNbr
     , cast(OrderDate as date) OrderDate
     , getdate() DeletedDatetime
     , (select replace(u.Username, 'journeyhl.com\', '') from Users u where u.PKID = d.LastModifiedByID and u.CompanyID = d.CompanyID) DeletedBy     
     , InventoryID
     , SubItemID
     , SiteID
     , LocationID
     , cast(OrderQty as int) OrderQty
     , cast(ShippedQty as int) ShippedQty
     , cast(UnitPrice as decimal(18,2)) UnitPrice
     , cast(UnitCost as decimal(18,2)) UnitCost
     , cast(ExtPrice as decimal(18,2)) ExtPrice
     , cast(ExtCost as decimal(18,2)) ExtCost
     , cast(DiscPct as decimal(18,2)) DiscPct
     , cast(DiscAmt as decimal(18,2)) DiscAmt
     , ManualDisc
     , ReasonCode
     , cast(OpenQty as int) OpenQty
     , cast(OpenAmt as decimal(18,2)) OpenAmt
     , cast(LineAmt as decimal(18,2)) LineAmt
     , Completed
     , OrigOrderType
     , OrigOrderNbr
     , OrigLineNbr
     , OrigShipmentType
     , OpenLine
     , cast(BilledQty as int) BilledQty
     , cast(UnbilledQty as int) UnbilledQty
     , cast(UnbilledAmt as decimal(18,2)) UnbilledAmt
     , cast(ShipDate as date) ShipDate
     , cast(CancelDate as date) CancelDate
     , cast(BilledAmt as decimal(18,2)) BilledAmt
     , DiscountID
     , DiscountSequenceID
     , InvoiceType
     , InvoiceNbr
     , InvoiceLineNbr
     , InvoiceDate
     , IsFree
     , POCreate
     , POCreated
     , POSource
     , POSiteID
     , cast(QtyOnOrders as int) QtyOnOrders
     , CustomerOrderNbr
     , cast(POCreateDate as date) POCreateDate
     , IsStockItem
     , cast(NetSales as decimal(18,2)) NetSales
     , cast(MarginAmt as decimal(18,2)) MarginAmt
     , MarginPct
     , CreatedDatetime
     , (select replace(u.Username, 'journeyhl.com\', '') from Users u where u.PKID = d.CreatedByID and u.CompanyID = d.CompanyID) CreatedBy
from deleted d 
where d.CompanyID = 2 and d.OrderType != 'QT'
end

