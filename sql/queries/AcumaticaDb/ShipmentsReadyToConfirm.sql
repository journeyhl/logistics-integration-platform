
with TopLevel as(
select s.ShipmentNbr
	 , s.Status
	 , count(sl.LineNbr) OrderLines
	 , count(p.LineNbr) PackageLines
	 , cast(sum(sl.ShippedQty) as int) ShipmentQty
	 , cast(sum(spl.PackedQty) as int) PackageQty
	 , count(distinct p.TrackNumber) Packages
	 , cast(sum(sl.ShippedQty) as int) /count(distinct p.TrackNumber) ShipmentQty2
	 
from SOShipment s 
inner join SOShipLine sl on s.CompanyID = sl.CompanyID and s.ShipmentNbr = sl.ShipmentNbr
left join SOShipLineSplitPackage spl on s.CompanyID = spl.CompanyID and s.ShipmentNbr = spl.ShipmentNbr and sl.LineNbr = spl.ShipmentLineNbr and sl.InventoryID = spl.InventoryID
left join SOPackageDetail p on s.CompanyID = p.CompanyID and s.ShipmentNbr = p.ShipmentNbr and spl.PackageLineNbr = p.LineNbr
where s.companyid = 2 
and s.Status = 'N'
and p.TrackNumber is not null
group by s.ShipmentNbr, s.Status
)
select *
from TopLevel
where ShipmentQty = PackageQty or ShipmentQty2 = PackageQty