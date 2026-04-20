select top 200 cast(s.OrderDate as date) Date
	   , s.OrderType
	   , s.OrderNbr
       , (
           select sl2.LineNbr
                  , rtrim(i2.InventoryCD) InventoryCD
                  , rtrim(i2.Descr) Descr
                  , cast(sl2.OrderQty as int) as Qty
           from SOLine sl2
           inner join InventoryItem i2 on sl2.InventoryID = i2.InventoryID and sl2.CompanyID = i2.CompanyID
           where sl2.OrderNbr = s.OrderNbr 
             and sl2.CompanyID = s.CompanyID
           for json path
       ) as LineItems
	   , j.Status OrderStatus
	   , s.Status
	   , rtrim(b.AcctCD) AcctCD
	   , rtrim(b.AcctName) AcctName
	   , c.CustomerClassID
	   , cc.Url B2BPaymentLink
	   --/*
	   , ba.AddressLine1 BillingAddressLine1
	   , ba.AddressLine2 BillingAddressLine2
	   , ba.City		 BillingCity
	   , ba.State		 BillingState
	   , ba.PostalCode	 BillingPostalCode
	   , ba.CountryID	 BillingCountry
	   , concat('+1', bc.Phone1)		 BillingPhone
	   , bc.Email		 BillingEmail
	   , sa.AddressLine1 ShippingAddressLine1
	   , sa.AddressLine2 ShippingAddressLine2
	   , sa.City		 ShippingCity
	   , sa.State		 ShippingState
	   , sa.PostalCode	 ShippingPostalCode
	   , sa.CountryID	 ShippingCountry
	   , concat('+1', sc.Phone1)	 ShippingPhone
	   , sc.Email		 ShippingEmail
	   --*/
	   , case when s.OrderType = 'RT' or sp.SalespersonCD is not null then rtrim(sp.SalespersonCD) 
			  when sp.SalespersonCD is null then rtrim(o.AcctCD)	   
			  else null	   
	     end SalespersonCD
	   , case when s.OrderType = 'RT' or sp.SalespersonCD is not null then rtrim(sp.Descr) 
			  when sp.SalespersonCD is null then o.AcctName	   
			  else null	   
	     end Salesperson
	   , concat('https://erp.journeyhl.com/Main?CompanyID=JHL&ScreenId=AR303000&AcctCD=', rtrim(b.AcctCD)) AcuCustomerLink
	   , concat('https://erp.journeyhl.com/Main?CompanyID=JHL&ScreenId=SO301000&OrderType=', s.OrderType, '&OrderNbr=', rtrim(s.OrderNbr)) AcuOrderLink
	   , hs.Value HubspotLink
	   , cast(b.CreatedDateTime as date) CustomerCreatedOn
     , k.LastChecked
from SOOrder s 
inner join BAccount b on s.CustomerID = b.BAccountID and s.CompanyID = b.CompanyID 
inner join Customer c on b.BAccountID = c.BAccountID and s.CompanyID = c.CompanyID
inner join SOAddress ba on s.CompanyID = ba.CompanyID and s.BillAddressID = ba.AddressID and s.CustomerID = ba.CustomerID
inner join SOAddress sa on s.CompanyID = sa.CompanyID and s.ShipAddressID = sa.AddressID and s.CustomerID = sa.CustomerID
inner join SOContact bc on s.CompanyID = bc.CompanyID and s.CustomerID = bc.CustomerID and s.BillContactID = bc.ContactID and s.CustomerID = bc.CustomerID
inner join SOContact sc on s.CompanyID = sc.CompanyID and s.CustomerID = sc.CustomerID and s.ShipContactID = sc.ContactID and s.CustomerID = sc.CustomerID
left join BAccount o on s.CompanyID = o.CompanyID and s.OwnerID = o.DefContactID
left join SalesPerson sp on s.CompanyID = sp.CompanyID and s.SalesPersonID = sp.SalespersonID
left join K_OrderIngest k on s.OrderNbr = k.OrderNbr and s.Status = k.Status 
left join CSAnswers hs on b.CompanyID = hs.CompanyID and b.NoteID = hs.RefNoteID and hs.AttributeID = 'HUBSPOTID'
left join SOInvoice si on s.CompanyID = si.CompanyID and s.OrderNbr = si.SOOrderNbr and s.OrderType = si.SOOrderType
left join CCPayLink cc on s.CompanyID = cc.CompanyID and si.RefNbr = cc.RefNbr
inner join JJStatusLookup j on s.Status = j.CStatus and j.tbl = 'SOOrder'
where s.CompanyID = 2 
and s.OrderType not in('QT', 'CM', 'ZA')
-- and (k.LastChecked <= dateadd(hour, -1, getdate()) or k.LastChecked is null)
and (k.LastChecked is null
or k.LastChecked <= getdate() - 14)
order by k.LastChecked, s.LastModifiedDatetime