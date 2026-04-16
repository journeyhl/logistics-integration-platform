
select distinct cast(s.OrderDate as date) OrderDate
	 , s.OrderType
	 , s.OrderNbr
	 , jo.Status
	 , l.LineNbr
	 , cast(l.OrderQty as int) LineQty
	 , rtrim(i.InventoryCD) InventoryCD
	 , i.Descr
	 , rtrim(insO.SiteCD) Warehouse
	 , sh.ShipmentNbr
	 , sl.LineNbr ShipLineNbr
	 , cast(sh.ShipDate as date) ShipDate
	 , cast(sl.ShippedQty as int) ShipLineQty
	 , js.Status ShipStatus
	 , d.TrackNumber Tracking
	 , slp.CreatedDateTime TrackingCreated
     , concat('https://erp.journeyhl.com/Main?CompanyID=JHL&ScreenId=SO302000&ShipmentNbr=', sh.ShipmentNbr) AcuShipLink

from SOOrder s 
inner join SOLine l on s.CompanyID = l.CompanyID and s.OrderType = l.OrderType and s.OrderNbr = l.OrderNbr and s.CustomerID = l.CustomerID
inner join SOLineSplit lp on s.CompanyID = lp.CompanyID and s.OrderType = lp.OrderType and s.OrderNbr = lp.OrderNbr and l.LineNbr = lp.LineNbr 
inner join InventoryItem i on l.CompanyID = i.CompanyID and l.InventoryID = i.InventoryID
left join SOShipLine sl on s.CompanyID = l.CompanyID and l.LineNbr = sl.OrigLineNbr and l.OrderNbr = sl.OrigOrderNbr and lp.SplitLineNbr = sl.OrigSplitLineNbr
left join SOShipment sh on s.CompanyID = sh.CompanyID and sl.ShipmentType = sh.ShipmentType and sl.ShipmentNbr = sh.ShipmentNbr
left join SOShipLineSplit sp on s.CompanyID = sp.CompanyID and sh.ShipmentNbr = sp.ShipmentNbr and sl.LineNbr = sp.LineNbr and i.InventoryID = sp.InventoryID
left join SOShipLineSplitPackage slp on s.CompanyID = slp.CompanyID and sh.ShipmentNbr = slp.ShipmentNbr and sl.LineNbr = slp.ShipmentLineNbr and sp.SplitLineNbr = slp.ShipmentSplitLineNbr
left join SOPackageDetail d on s.CompanyID = d.CompanyID and sh.ShipmentNbr = d.ShipmentNbr and slp.PackageLineNbr = d.LineNbr
left join INsite insO on s.CompanyID = insO.CompanyID and l.SiteID = insO.SiteID 
inner join JJStatusLookup jo on s.Status = jo.CStatus and jo.Tbl = 'SOOrder'
inner join JJStatusLookup js on sh.Status = js.CStatus and js.Tbl = 'SOShipment'
where s.CompanyID = 2 
and s.OrderType not in('QT', 'CM', 'ZA')