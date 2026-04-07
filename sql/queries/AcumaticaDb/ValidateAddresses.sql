select s.OrderNbr
     , s.OrderType
     , rtrim(b.AcctCD) AcctCD
     , cast(s.OrderDate as date) OrderDate
     , sa.IsValidated
     , j.Status
     , sa.AddressLine1
     , sa.AddressLine2
     , sa.City
     , sa.State
     , sa.PostalCode
     , sa.CountryID
     , sc.ContactID
     , sc.CustomerContactID
     , a.AddressID
     , sa.CustomerAddressID
     , a.NoteID aNoteID
     , c.NoteID cNoteID
from SOOrder s
inner join SOAddress sa on s.CompanyID = sa.CompanyID and s.CustomerID = sa.CustomerID and s.ShipAddressID = sa.AddressID
inner join SOContact sc on s.CompanyID = sc.CompanyID and s.ShipContactID = sc.ContactID
inner join BAccount b on s.CompanyID = b.CompanyID and s.CustomerID = b.BAccountID
inner join JJStatusLookup j on s.Status = j.CStatus and j.Tbl = 'SOOrder'

left join Contact c on s.CompanyID = c.CompanyID and b.BAccountID = c.BAccountID and sc.CustomerContactID = c.ContactID
left join Address a on s.CompanyID = a.CompanyID and b.BAccountID = a.BAccountID and sa.CustomerAddressID = a.AddressID

where s.CompanyID = 2 and
(a.IsValidated is null or a.IsValidated = 0)
and s.[Status] not in('L', 'C', 'S')
-- and s.OrderType != 'QT'
and s.OrderNbr = 'QT051599'
order by OrderDate desc
