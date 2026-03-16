select s.ShipmentNbr IFReference
	 , 'Import' IFCompany
	 , j.Status AcuStatus
	 , s.ShipmentNbr RMANumber
	 , sl.OrigOrderNbr OrderNbr
	 , case when sl.OrigOrderType = 'RC' then '3' else 'W' end RMAType
	 , '' RMASubType
	 , s.ShipVia
	 , case when s.ShipVia is null or s.ShipVia = 'GROUND' then 'FDXG'
		when s.ShipVia = '2DAY' then 'FED2' else null end ShipCode
	 , 'Fedex' ShipPriority
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
	 , sl.LineNbr LineNumber
	 , rtrim(i.InventoryCD) DFPart
	 , cast(sl.ShippedQty as int) DFQuantity
	 , rtrim(i.InventoryCD) RPPart
	 , cast(sl.ShippedQty as int) RPQuantity
	 , '' Serialnumber, '' DFCategory, '' DFComments
	 , rtrim(c.acctcd) AcctCD

	 , sl.OrigOrderType
	 , i.Descr SOItemDescr
	 , i.Descr RPItemDescr
	 , case when i.descr like '%cpo%' then 'CPO'
			when i.descr like '%pre own%' then 'CPO'
			when i.descr like '%pre-own%' then 'CPO'
			when ic.Descr like '%part%' or ic.Descr = 'Misc. Other' then 'Parts'
			when ic.Descr like '%Product%' or i.inventorycd = '08568' then 'NEW-RESTCK'
		else null end RPLocation
	 , ic.Descr
	 , cast(k.ValueNumeric as int) SentToWH
	 , '' SerialNumber
	 , rtrim(c.AcctCD) CustomerID
from SOShipment s
inner join SOShipLine sl on s.CompanyID = sl.CompanyID and s.ShipmentType = sl.ShipmentType and s.ShipmentNbr = sl.ShipmentNbr
inner join SOContact sc on s.CompanyID = sc.CompanyID and s.ShipContactID = sc.ContactID and s.CustomerID = sc.CustomerID
inner join SOAddress sa on s.CompanyID = sa.CompanyID and s.ShipAddressID = sa.AddressID and s.CustomerID = sa.CustomerID
inner join BAccount c on s.CompanyID = c.CompanyID and s.CustomerID = c.BAccountID
inner join InventoryItem i on s.CompanyID = i.CompanyID and sl.InventoryID = i.InventoryID
inner join INItemClass ic on s.CompanyID = ic.CompanyID and i.ItemClassID = ic.ItemClassID
inner join INSite isi on s.CompanyID = isi.CompanyID and s.SiteID = isi.SiteID
left join SOShipmentKvExt k on s.CompanyID = k.CompanyID and s.NoteID = k.RecordID and k.FieldName = 'AttributeSHP2WH'
left join JJStatusLookup j on s.Status = j.CStatus and j.Tbl = 'SOShipment'
where s.CompanyID = 2 and 
isi.SiteCD = 'RMI' and sl.OrigOrderType != 'RC'
and s.Status not in('C', 'L', 'F', 'I')
and k.ValueNumeric = 0
-- s.ShipmentNbr = '077252' and sl.LineNbr = 1		--This line is to send one offs
order by IFReference, LineNumber