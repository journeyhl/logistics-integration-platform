

select s.OrderType
	 , s.OrderNbr QuoteNbr
	 , l.LineNbr LineNbr 
	 , cast(s.OrderDate as date) DatePlaced
	 , rtrim(b.AcctCD) CustomerID
	 , b.AcctName CustomerName
	 , c.CustomerClassID CustomerClass
	 , rtrim(i.InventoryCD) InventoryCD
     , rtrim(i.Descr) Description
     , cast(l.OrderQty as int) as Quantity
	 , cast(l.LineAmt  as decimal(18,2))	LinePrice
	 , cast(s.OrderTotal as decimal(18,2)) OrderTotal
	 , js.Status Status
	 , s.CustomerOrderNbr CustOrderNumber
	 , rtrim(o.AcctCD) D2CSalesperson
	 , o.AcctName SalespersonName
	 , null SalespersonEmail
	 , rtrim(sp.SalespersonCD) B2BSalesperson
	 , l.DiscountID DiscountCode
	 , cast(l.DiscAmt  as decimal(18,2))	DiscountAmt
	 , cast(l.ExtCost as decimal(18,2)) LineCost
	 , cast(s.PaidAmt as decimal(18,2)) TotalPaid
	 , sc.Phone1		 Phone
	 , sc.Email		 Email
	 , sa.AddressLine1 AddressLine1
	 , case when sa.AddressLine2 = '' then null else sa.AddressLine2 end AddressLine2
	 , sa.City		 City
	 , sa.State		 State
	 , sa.PostalCode	 Zip
	 , sa.CountryID	 Country
	 , cast(l.LineAmt  as decimal(18,2)) LineAmount
	 , r.ReplenishmentClassID ReplenishmentClass
	 , i.StkItem StockItem
	 , rtrim(si.SiteCD) OrderLineWH
	 , s.LastModifiedDatetime LastModifiedDT
	 , s.CreatedDatetime CreatedDT
	 , ph.OrderType AttachedOrderType
	 , ph.OrderNbr AttachedOrderNbr
from SOOrder s 
inner join BAccount b on s.CustomerID = b.BAccountID and s.CompanyID = b.CompanyID 
inner join Customer c on b.BAccountID = c.BAccountID and s.CompanyID = c.CompanyID
left join SOAddress ba on s.CompanyID = ba.CompanyID and s.BillAddressID = ba.AddressID and s.CustomerID = ba.CustomerID
left join SOAddress sa on s.CompanyID = sa.CompanyID and s.ShipAddressID = sa.AddressID and s.CustomerID = sa.CustomerID
left join SOContact bc on s.CompanyID = bc.CompanyID and s.CustomerID = bc.CustomerID and s.BillContactID = bc.ContactID
left join SOContact sc on s.CompanyID = sc.CompanyID and s.CustomerID = sc.CustomerID and s.ShipContactID = sc.ContactID
left join SOLine l on s.CompanyID = l.CompanyID and s.OrderNbr = l.OrderNbr and s.OrderType = l.OrderType and s.CustomerID = l.CustomerID
left join InventoryItem i on s.CompanyID = i.CompanyID and l.InventoryID = i.InventoryID
left join INItemRep r on s.CompanyID = r.CompanyID and i.InventoryID = r.InventoryID
left join INSite si on s.CompanyID = si.CompanyID and l.SiteID = si.SiteID


left join CustSalesPeople csp on s.CompanyID = csp.CompanyID and s.CustomerID = csp.BAccountID
left join SalesPerson sp on s.CompanyID = sp.CompanyID and csp.SalesPersonID = sp.SalespersonID
left join BAccount o on s.CompanyID = o.CompanyID and s.OwnerID = o.DefContactID 
inner join JJStatusLookup js on s.Status = js.CStatus and js.Tbl = 'SOOrder'
left join SOOrder ph on s.CompanyID = ph.CompanyID and s.OrderType = ph.OrigOrderType and s.OrderNbr = ph.OrigOrderNbr

left join Users uc on s.CompanyID = uc.CompanyID and s.CreatedByID = uc.PKID
left join Users um on s.CompanyID = um.CompanyID and s.LastModifiedByID = um.PKID
where s.CompanyID = 2 
and s.OrderType = 'QT'
--and s.OrderNbr in('')
--and s.Status in('')
--and o.AcctName in('')
--and sh.ShipmentNbr in('')
--and sh.Status in('')
--and InventoryCD in('')
--and i.Descr like '%%'
--and b.AcctCD in('')
--and b.AcctName like '%%'
--and c.CustomerClassID in('')
order by DatePlaced desc


