select s.OrderNbr ReturnNbr
	 , sl.LineNbr LineNumber
	 , s.OrderType
	 , rtrim(i.InventoryCD) InventoryCD
	 , cast(sl.OrderQty as int) Qty
	 , rtrim(c.acctcd) AcctCD
	 , sl.OrigOrderType
	 , i.Descr Item
	 , ic.Descr ItemClass
	 , cast(k.ValueNumeric as int) SentToWH
	 , rtrim(c.AcctCD) CustomerID
	 , s.OrderDate
	 , j.Status AcuStatus
	 , coalesce(sl.OrigOrderNbr, s.CustomerOrderNbr) OriginalOrderNbr
	 , s.ShipVia
	 , case when s.ShipVia is null or s.ShipVia = 'GROUND' then 'FDXG'
		when s.ShipVia = '2DAY' then 'FED2' else null end ShipCode
	 , 'Fedex' ShipPriority
	 , left(sc.FullName, 25) ShipToName
	 , left(c.AcctName, 25) CompanyName
	 , coalesce(sc.email, 'cs@journeyhl.com') ShipToEmailContact
	 , coalesce(sc.Phone1, sc.Phone2) ShipToPhone
	 , sa.AddressLine1 ShipToAddress1
	 , sa.AddressLine2 ShipToAddress2
	 , sa.City ShipToCity
	 , sa.State ShipToState
	 , sa.PostalCode ShipToZip
	 , sa.CountryID ShipToCountry
	 , shl.ShipmentNbr
from SOOrder s
inner join SOLine sl on s.CompanyID = sl.CompanyID and s.OrderType = sl.OrderType and s.OrderNbr = sl.OrderNbr
inner join SOContact sc on s.CompanyID = sc.CompanyID and s.ShipContactID = sc.ContactID and s.CustomerID = sc.CustomerID
inner join SOAddress sa on s.CompanyID = sa.CompanyID and s.ShipAddressID = sa.AddressID and s.CustomerID = sa.CustomerID
inner join BAccount c on s.CompanyID = c.CompanyID and s.CustomerID = c.BAccountID
inner join InventoryItem i on s.CompanyID = i.CompanyID and sl.InventoryID = i.InventoryID
inner join INItemClass ic on s.CompanyID = ic.CompanyID and i.ItemClassID = ic.ItemClassID
inner join INSite isi on s.CompanyID = isi.CompanyID and sl.SiteID = isi.SiteID
left join SOOrderKvExt k on s.CompanyID = k.CompanyID and s.NoteID = k.RecordID and k.FieldName = 'AttributeRCSHP2WH'
left join JJStatusLookup j on s.Status = j.CStatus and j.Tbl = 'SOOrder'
left join SOShipLine shl on shl.OrigOrderNbr = s.OrderNbr and shl.OrigLineNbr = sl.LineNbr and sl.InventoryID = shl.InventoryID
where s.CompanyID = 2
and isi.SiteCD = 'RMI'
and s.Status in('N')
and s.OrderType = 'RC'
--and s.OrderDate > '20260301'
and k.ValueNumeric = 1
and shl.ShipmentNbr is null
order by s.OrderDate desc, ReturnNbr, LineNumber