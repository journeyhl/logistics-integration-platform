select s.OrderNbr
     , s.OrderType
     , rtrim(b.AcctCD) AcctCD
     , cast(s.OrderDate as date) OrderDate
     , sa.IsValidated
     , j.Status
     , sa.AddressLine1 sAddressLine1
     , case when sa.AddressLine2 is null then '' else sa.AddressLine2 end sAddressLine2
     , sa.City sCity
     , sa.State sState
     , sa.PostalCode sPostalCode
     , sa.CountryID sCountryID
     
     , ba.AddressLine1 bAddressLine1
     , case when ba.AddressLine2 is null then '' else ba.AddressLine2 end bAddressLine2
     , ba.City bCity
     , ba.State bState
     , ba.PostalCode bPostalCode
     , ba.CountryID bCountryID
     , case when sa.AddressLine1 = ba.AddressLine1 and sa.AddressLine2 = ba.AddressLine2 and sa.City = ba.City and sa.State = ba.State and sa.PostalCode = ba.PostalCode and sa.CountryID = ba.CountryID
     then 1 else 0 end Match
     
from SOOrder s
inner join SOAddress sa on s.CompanyID = sa.CompanyID and s.CustomerID = sa.CustomerID and s.ShipAddressID = sa.AddressID
inner join SOContact sc on s.CompanyID = sc.CompanyID and s.ShipContactID = sc.ContactID
inner join SOAddress ba on s.CompanyID = ba.CompanyID and s.CustomerID = ba.CustomerID and s.BillAddressID = ba.AddressID
inner join SOContact bc on s.CompanyID = bc.CompanyID and s.BillContactID = bc.ContactID
inner join BAccount b on s.CompanyID = b.CompanyID and s.CustomerID = b.BAccountID
inner join JJStatusLookup j on s.Status = j.CStatus and j.Tbl = 'SOOrder'

left join Contact c on s.CompanyID = c.CompanyID and b.BAccountID = c.BAccountID and sc.CustomerContactID = c.ContactID
left join Address a on s.CompanyID = a.CompanyID and b.BAccountID = a.BAccountID and sa.CustomerAddressID = a.AddressID

where s.CompanyID = 2 and
(sa.IsValidated is null or sa.IsValidated = 0)
and s.[Status] not in('L', 'C', 'S')
and s.[Status] = 'H'
-- and b.AcctCD = 'C0006719'
and s.OrderDate >= cast(getdate() as date)
and s.OrderType != 'QT'
-- and s.OrderNbr = 'QT051599'
order by OrderDate desc
