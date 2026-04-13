create trigger TriggerSOOrderShipmentDelete
on SOOrderShipment
after delete as
begin
insert into SOOrderShipmentDeletions(
CompanyID,
OrderType,
OrderNbr,
ShippingRefNoteID,
ShipmentType,
ShipmentNbr,
CustomerID,
CustomerLocationID,
ShipAddressID,
LineCntr,
ShipDate,
ProjectID,
Hold,
ShipComplete,
ShipmentQty,
CreatedByID,
CreatedByScreenID,
CreatedDateTime,
LastModifiedByID,
LastModifiedByScreenID,
LastModifiedDateTime,
SiteID,
Confirmed,
Operation,
ShipContactID,
ShipmentWeight,
ShipmentVolume,
LineTotal,
InvoiceType,
InvoiceNbr,
InvoiceReleased,
InvtDocType,
CreateINDoc,
CreateARDoc,
InvtRefNbr,
OrderFreightAllocated,
OrderTaxAllocated,
OrderNoteID,
ShipmentNoteID,
InvtNoteID,
UsrIGCMShipContNbr,
DeletedBy,
DeletedDateTime
)
select d.CompanyID
	 , d.OrderType
	 , d.OrderNbr
	 , d.ShippingRefNoteID
	 , d.ShipmentType
	 , d.ShipmentNbr
	 , d.CustomerID
	 , d.CustomerLocationID
	 , d.ShipAddressID
	 , d.LineCntr
	 , d.ShipDate
	 , d.ProjectID
	 , d.Hold
	 , d.ShipComplete
	 , d.ShipmentQty
	 , d.CreatedByID
	 , d.CreatedByScreenID
	 , d.CreatedDateTime
	 , d.LastModifiedByID
	 , d.LastModifiedByScreenID
	 , d.LastModifiedDateTime
	 , d.SiteID
	 , d.Confirmed
	 , d.Operation
	 , d.ShipContactID
	 , d.ShipmentWeight
	 , d.ShipmentVolume
	 , d.LineTotal
	 , d.InvoiceType
	 , d.InvoiceNbr
	 , d.InvoiceReleased
	 , d.InvtDocType
	 , d.CreateINDoc
	 , d.CreateARDoc
	 , d.InvtRefNbr
	 , d.OrderFreightAllocated
	 , d.OrderTaxAllocated
	 , d.OrderNoteID
	 , d.ShipmentNoteID
	 , d.InvtNoteID
	 , d.UsrIGCMShipContNbr
	 , (select replace(u.Username, 'journeyhl.com\', '') from Users u where u.PKID = d.LastModifiedByID and u.CompanyID = d.CompanyID) DeletedBy
	 , GETDATE() DeletedDateTime
from deleted d where d.CompanyID = 2 and d.OrderType != 'QT'
end

