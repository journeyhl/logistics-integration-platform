
declare @DateCutoff datetime = dateadd(day, -12, getdate());

select s.ShipmentNbr
	 , sl.LineNbr ShipmentLineNbr
	 , 'DEFAULT BOX' BoxID
	 , spl.SplitLineNbr
	 , i.InventoryID
	 , RTRIM(i.InventoryCD) InventoryCD
     , rtrim(b.AcctCD) AcctCD
	 , cast(sl.ShippedQty as int) ShipQty
	 , cast(sl.OrigOrderQty as int) OrderQty
	 , p.TrackNumber Tracking
	 , si.SiteCD
	, concat(s.ShipmentNbr, '-', RTRIM(i.InventoryCD)) ShipItem,
case when s.Status = 'C' then 'Completed'
	 when s.Status = 'N' then 'Open'
	 when s.Status = 'I' then 'Invoiced'
	 when s.Status = 'F' then 'Confirmed'
else null end ShipStatus
     , splp.PackageLineNbr
     , case when splp.PackageLineNbr is not null then 'Has Package' else 'No Package' end Package
from SOShipment s
inner join SOShipLine sl on s.CompanyID = sl.CompanyID and s.ShipmentNbr = sl.ShipmentNbr  
inner join SOShipLineSplit spl on s.CompanyID = spl.CompanyID and s.ShipmentNbr = spl.ShipmentNbr  
inner join InventoryItem i on s.CompanyID = i.CompanyID and sl.InventoryID = i.InventoryID and spl.InventoryID = i.InventoryID 
inner join INSite si on s.CompanyID = si.CompanyID and s.SiteID = si.SiteID
left join SOShipLineSplitPackage splp on s.CompanyID = splp.CompanyID and s.ShipmentNbr = splp.ShipmentNbr
        and spl.SplitLineNbr = splp.ShipmentSplitLineNbr and splp.ShipmentLineNbr = sl.LineNbr
left join SOPackageDetail p on s.CompanyID = p.CompanyID and s.ShipmentNbr = p.ShipmentNbr
inner join BAccount b on s.CompanyID = b.CompanyID and s.CustomerID = b.BAccountID
where s.CompanyID = 2 
and left(SiteCD, 7) = 'REDSTAG'
and (
s.LastModifiedDateTime >= @DateCutoff or
sl.LastModifiedDateTime >= @DateCutoff or
splp.LastModifiedDateTime >= @DateCutoff)
and s.Status in('N')
and p.TrackNumber is null