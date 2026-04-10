if not exists(
select *
from sys.schemas s
where s.name = '_util'
)
exec('create schema _util');
if not exists(
select * 
from sys.tables t 
inner join sys.schemas s on t.schema_id = s.schema_id
where t.name = 'SOLineDeletions' and s.name = '_util'
) 
CREATE TABLE _util.SOLineDeletions(
	OrderType char(2) not null,
	OrderNbr nvarchar(15) not null,
	LineNbr int not null,
	OrderDate date null,
	DeletedDatetime datetime not null,
	DeletedBy nvarchar(4000) null,
	InventoryID int null,
	SubItemID int null,
	SiteID int null,
	LocationID int null,
	OrderQty int null,
	ShippedQty int null,
	UnitPrice decimal(18, 2) null,
	UnitCost decimal(18, 2) null,
	ExtPrice decimal(18, 2) null,
	ExtCost decimal(18, 2) null,
	DiscPct decimal(18, 2) null,
	DiscAmt decimal(18, 2) null,
	ManualDisc bit null,
	ReasonCode nvarchar(20) null,
	OpenQty int null,
	OpenAmt decimal(18, 2) null,
	LineAmt decimal(18, 2) null,
	Completed bit null,
	OrigOrderType char(2) null,
	OrigOrderNbr nvarchar(15) null,
	OrigLineNbr int null,
	OrigShipmentType char(1) null,
	OpenLine bit null,
	BilledQty int null,
	UnbilledQty int null,
	UnbilledAmt decimal(18, 2) null,
	ShipDate date null,
	CancelDate date null,
	BilledAmt decimal(18, 2) null,
	DiscountID nvarchar(10) null,
	DiscountSequenceID nvarchar(10) null,
	InvoiceType char(3) null,
	InvoiceNbr nvarchar(15) null,
	InvoiceLineNbr int null,
	InvoiceDate datetime2(0) null,
	IsFree bit null,
	POCreate bit ,
	POCreated bit ,
	POSource char(1) null,
	POSiteID int null,
	QtyOnOrders int null,
	CustomerOrderNbr nvarchar(40) null,
	POCreateDate date null,
	IsStockItem bit null,
	NetSales decimal(18, 2) null,
	MarginAmt decimal(18, 2) null,
	MarginPct decimal(19, 2) null,
	CreatedDatetime datetime not null,
	CreatedBy nvarchar(4000) null,
	primary key (OrderType, OrderNbr, LineNbr))
