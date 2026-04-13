create trigger TriggerSOShipmentDelete
on SOShipment
after delete as
begin
insert into SOShipmentDeletions(
CompanyID,
DatabaseRecordStatus,
ShipmentNbr,
CustomerID,
CustomerLocationID,
ShipAddressID,
Hold,
ControlQty,
ShipmentQty,
ShipmentDesc,
CreatedByID,
CreatedByScreenID,
CreatedDateTime,
LastModifiedByID,
LastModifiedByScreenID,
LastModifiedDateTime,
GotReadyForArchiveAt,
SiteID,
DestinationSiteID,
ShipDate,
LineCntr,
OrderCntr,
BilledOrderCntr,
UnbilledOrderCntr,
ReleasedOrderCntr,
Confirmed,
ShipmentType,
Operation,
Released,
Picked,
PickedViaWorksheet,
PickedQty,
PackedQty,
ShipContactID,
ShipVia,
UseCustomerAccount,
Resedential,
SaturdayDelivery,
Insurance,
GroundCollect,
LabelsPrinted,
CommercialInvoicesPrinted,
PickListPrinted,
ConfirmationPrinted,
ShippedViaCarrier,
FOBPoint,
ShipTermsID,
ShipZoneID,
CuryInfoID,
CuryID,
LineTotal,
PackageLineCntr,
PackageWeight,
IsPackageValid,
IsManualPackage,
FreightCost,
CuryFreightCost,
OverrideFreightAmount,
FreightAmountSource,
CuryFreightAmt,
FreightAmt,
CuryPremiumFreightAmt,
PremiumFreightAmt,
CuryTotalFreightAmt,
TotalFreightAmt,
TaxCategoryID,
OwnerID,
WorkGroupID,
Status,
ShipmentWeight,
ShipmentVolume,
FreeItemQtyTot,
FreightClass,
SkipAddressVerification,
CustomerOrderNbr,
NoteID,
ConfirmedByID,
ConfirmedDateTime,
CurrentWorksheetNbr,
ManifestNbr,
TermsOfSale,
DHLBillingRef,
IsIntercompany,
ExcludeFromIntercompanyProc,
DeliveryConfirmation,
EndorsementService,
ExternalShipmentUpdated,
Installed,
BrokerContactID,
PackageCount,
BillSeparatelyCntr,
UsrIGCMIncludeInContainer,
DeletedBy,
DeletedDateTime)

select CompanyID
	 , d.DatabaseRecordStatus
	 , d.ShipmentNbr
	 , d.CustomerID
	 , d.CustomerLocationID
	 , d.ShipAddressID
	 , d.Hold
	 , d.ControlQty
	 , d.ShipmentQty
	 , d.ShipmentDesc
	 , d.CreatedByID
	 , d.CreatedByScreenID
	 , d.CreatedDateTime
	 , d.LastModifiedByID
	 , d.LastModifiedByScreenID
	 , d.LastModifiedDateTime
	 , d.GotReadyForArchiveAt
	 , d.SiteID
	 , d.DestinationSiteID
	 , d.ShipDate
	 , d.LineCntr
	 , d.OrderCntr
	 , d.BilledOrderCntr
	 , d.UnbilledOrderCntr
	 , d.ReleasedOrderCntr
	 , d.Confirmed
	 , d.ShipmentType
	 , d.Operation
	 , d.Released
	 , d.Picked
	 , d.PickedViaWorksheet
	 , d.PickedQty
	 , d.PackedQty
	 , d.ShipContactID
	 , d.ShipVia
	 , d.UseCustomerAccount
	 , d.Resedential
	 , d.SaturdayDelivery
	 , d.Insurance
	 , d.GroundCollect
	 , d.LabelsPrinted
	 , d.CommercialInvoicesPrinted
	 , d.PickListPrinted
	 , d.ConfirmationPrinted
	 , d.ShippedViaCarrier
	 , d.FOBPoint
	 , d.ShipTermsID
	 , d.ShipZoneID
	 , d.CuryInfoID
	 , d.CuryID
	 , d.LineTotal
	 , d.PackageLineCntr
	 , d.PackageWeight
	 , d.IsPackageValid
	 , d.IsManualPackage
	 , d.FreightCost
	 , d.CuryFreightCost
	 , d.OverrideFreightAmount
	 , d.FreightAmountSource
	 , d.CuryFreightAmt
	 , d.FreightAmt
	 , d.CuryPremiumFreightAmt
	 , d.PremiumFreightAmt
	 , d.CuryTotalFreightAmt
	 , d.TotalFreightAmt
	 , d.TaxCategoryID
	 , d.OwnerID
	 , d.WorkGroupID
	 , d.Status
	 , d.ShipmentWeight
	 , d.ShipmentVolume
	 , d.FreeItemQtyTot
	 , d.FreightClass
	 , d.SkipAddressVerification
	 , d.CustomerOrderNbr
	 , d.NoteID
	 , d.ConfirmedByID
	 , d.ConfirmedDateTime
	 , d.CurrentWorksheetNbr
	 , d.ManifestNbr
	 , d.TermsOfSale
	 , d.DHLBillingRef
	 , d.IsIntercompany
	 , d.ExcludeFromIntercompanyProc
	 , d.DeliveryConfirmation
	 , d.EndorsementService
	 , d.ExternalShipmentUpdated
	 , d.Installed
	 , d.BrokerContactID
	 , d.PackageCount
	 , d.BillSeparatelyCntr
	 , d.UsrIGCMIncludeInContainer
	 , (select replace(u.Username, 'journeyhl.com\', '') from Users u where u.PKID = d.LastModifiedByID and u.CompanyID = d.CompanyID) DeletedBy
	 , GETDATE() DeletedDateTime
from deleted d where d.CompanyID = 2
end


