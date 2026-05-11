
with TopLevel as(
select s.CompanyID
	 , s.ShipmentNbr
	 , s.Status
	 , count(sl.LineNbr) OrderLines
	 , count(p.LineNbr) PackageLines
	 , cast(sum(sl.ShippedQty) as int) ShipmentQty
	 , cast(sum(spl.PackedQty) as int) PackageQty
	 , count(distinct p.TrackNumber) Packages
	--  , cast(sum(sl.ShippedQty) as int) /count(distinct p.TrackNumber) ShipmentQty2
	 
from SOShipment s 
inner join SOShipLine sl on s.CompanyID = sl.CompanyID and s.ShipmentNbr = sl.ShipmentNbr
left join SOShipLineSplitPackage spl on s.CompanyID = spl.CompanyID and s.ShipmentNbr = spl.ShipmentNbr and sl.LineNbr = spl.ShipmentLineNbr and sl.InventoryID = spl.InventoryID
left join SOPackageDetail p on s.CompanyID = p.CompanyID and s.ShipmentNbr = p.ShipmentNbr and spl.PackageLineNbr = p.LineNbr
where s.companyid = 2 
and s.Status = 'N'
and p.TrackNumber is not null
group by s.ShipmentNbr, s.Status, s.CompanyID
)
, SecondLevel as(
select *
	 , (select cast(sum(l.ShippedQty) as int) from SOShipLine l where t.CompanyID = l.CompanyID and t.ShipmentNbr = l.ShipmentNbr) ShippedQty
	 , (select cast(sum(p.PackedQty) as int) from SOShipLineSplitPackage p where t.CompanyID = p.CompanyID and t.ShipmentNbr = p.ShipmentNbr) PackedQty
from TopLevel t
where ShipmentQty = PackageQty
)
select *
from SecondLevel s
where ShippedQty = PackedQty