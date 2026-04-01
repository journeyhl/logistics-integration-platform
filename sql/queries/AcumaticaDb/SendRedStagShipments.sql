select s.ShipmentNbr
	 , j.Status AcuStatus
	 , cast(k.ValueNumeric as int) SentToWH
	 , kr.ValueString rsOrderID
	 , sl.OrigOrderNbr 
	 , sl.OrigOrderType
	 , s.ShipVia
	 , sl.LineNbr LineNumber
	 , cast(sl.ShippedQty as int) ShippedQty
	 , rtrim(i.InventoryCD) InventoryCD
	 , i.Descr ItemDescr
	 , ic.Descr ItemClassDescr
	 , case when s.ShipVia is null or s.ShipVia = 'GROUND' then 'FDXG'
		when s.ShipVia = '2DAY' then 'FED2' else null end ShipCode
	 , 'Fedex' ShipPriority
	 , rtrim(c.AcctCD) CustomerID
	 , left(sc.FullName, 35) ShipToName
	 , left(c.AcctName, 35) CompanyName
	 , coalesce(sc.email, 'cs@journeyhl.com') ShipToEmailContact
	 , coalesce(sc.Phone1, sc.Phone2) ShipToPhone
	 , sa.AddressLine1 ShipToAddress1
	 , sa.AddressLine2 ShipToAddress2
	 , sa.City ShipToCity
	 , sa.State ShipToState
	 , sa.PostalCode ShipToZip
	 , sa.CountryID ShipToCountry
	, case when s.ShipVia in ('GROUND', 'PSC150THRES', 'WHITEGLOVE399', 'WHITEGLOVE450', 'WHITEGLOVE499') and i.Descr like '%battery%' 
	  			then 'cheapest_UPS'
			when s.ShipVia in ('GROUND', 'PSC150THRES', 'WHITEGLOVE399', 'WHITEGLOVE450', 'WHITEGLOVE499')
				then 'cheapest_ALL'
			when s.ShipVia = '2DAY'
				then 'cheapest_TWO_DAY'
			when s.ShipVia = 'LTL'
				then 'external_ltl'
	  else 'cheapest_ALL' end rsShipVia
	, s.CustomerOrderNbr

from SOShipment s
inner join SOShipLine sl on s.CompanyID = sl.CompanyID and s.ShipmentType = sl.ShipmentType and s.ShipmentNbr = sl.ShipmentNbr
inner join SOContact sc on s.CompanyID = sc.CompanyID and s.ShipContactID = sc.ContactID and s.CustomerID = sc.CustomerID
inner join SOAddress sa on s.CompanyID = sa.CompanyID and s.ShipAddressID = sa.AddressID and s.CustomerID = sa.CustomerID
inner join BAccount c on s.CompanyID = c.CompanyID and s.CustomerID = c.BAccountID
inner join InventoryItem i on s.CompanyID = i.CompanyID and sl.InventoryID = i.InventoryID
inner join INItemClass ic on s.CompanyID = ic.CompanyID and i.ItemClassID = ic.ItemClassID
inner join INSite isi on s.CompanyID = isi.CompanyID and s.SiteID = isi.SiteID
left join SOShipmentKvExt k on s.CompanyID = k.CompanyID and s.NoteID = k.RecordID and k.FieldName = 'AttributeSHP2WH'
left join SOShipmentKvExt kr on s.CompanyID = kr.CompanyID and s.NoteID = kr.RecordID and kr.FieldName = 'AttributeRSORDERID'
left join JJStatusLookup j on s.Status = j.CStatus and j.Tbl = 'SOShipment'
where s.CompanyID = 2 and 
left(isi.SiteCD, 7) = 'RedStag' and sl.OrigOrderType != 'RC'
and s.Status not in('C', 'L', 'F', 'I')
and k.ValueNumeric = 0
-- s.ShipmentNbr = '077252' and sl.LineNbr = 1		--This line is to send one offs
-- and s.ShipmentNbr = '078974'
order by ShipmentNbr, LineNumber