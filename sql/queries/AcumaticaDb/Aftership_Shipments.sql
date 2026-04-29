with Toplevel as(
select sh.ShipmentNbr
     , sl.LineNbr ShipLineNbr
     , j.Status
     , sl.OrigOrderNbr OrderNbr
     , sl.OrigLineNbr OrderLineNbr
     , sl.OrigOrderType OrderType
     , rtrim(ii.InventoryCD) InventoryCD
     , ii.descr product_title
     , cast(ll.OrderQty as int) OrderQty
     , cast(ll.ShippedQty as int) ShipQty
     , trim(replace(replace(replace(pd.TrackNumber, 'PRO 	', ''), 'BOL 	', ''), 'FedEx Ground Economy 	', '')) Tracking
     , case when left(trim(replace(replace(replace(pd.TrackNumber, 'PRO 	', ''), 'BOL 	', ''), 'FedEx Ground Economy 	', '')), 1)
     not like '%[0-9]%' then 1 else null end BadTracking
     , cast(pd.CreatedDateTime as date) TrackingCreatedDate
     , cast(sh.ShipDate as date) ShipDate
     , rtrim(ba.AcctCD) CustomerID
     , case when sc.Attention != '' and sc.Attention is not null then sc.Attention else sc.FullName end Customer
     , sc.FullName FullName
     , sc.Attention
     , case when sa.CountryID in('US', 'CA') and sc.Phone1 not like '%+%' then concat('+1', sc.phone1)
	   else sc.Phone1 end phone_number
     , sc.Email
     , sa.AddressLine1 DestinationLine1
     , sa.AddressLine2 DestinationLine2
     , sa.City DestinationCity
     , sa.State DestinationState
     , sa.CountryID DestinationCountry
     , sa.PostalCode DestinationZip
     , rtrim(si.SiteCD) WarehouseID
     , wa.AddressLine1  OriginLine1
     , wa.AddressLine2  OriginLine2
     , wa.City  OriginCity
     , wa.State  OriginState
     , wa.CountryID  OriginCountry
     , wa.PostalCode  OriginZip
     , sh.PackageCount
     , cast(ll.UnitPrice * sl.ShippedQty as decimal(18,2)) LinePrice     
     , cast(ll.LineAmt as decimal(18,2)) LineAmt
     , cast(so.FreightAmt as decimal(18,2)) FreightAmt
     , cast(kw.ValueNumeric as int) SentToWH
     , cast(so.OrderTotal as decimal(18,2)) OrderTotal
     , kc.ValueString CourierCode
     , kn.ValueString CourierName
     , kr.ValueString CarrierName
     , cast(so.OrderDate as date) OrderDate
     , rtrim(cu.CustomerClassID) CustomerClass
from SOShipment sh 
inner join SOShipLine sl            on sh.CompanyID = sl.CompanyID and sh.ShipmentNbr = sl.ShipmentNbr and sh.ShipmentType = sl.ShipmentType
inner join SOLine ll                on sh.CompanyID = ll.CompanyID and sl.OrigOrderNbr = ll.OrderNbr and sl.OrigOrderNbr = ll.OrderNbr and sl.InventoryID = ll.InventoryID and sl.OrigOrderType = ll.OrderType
inner join SOOrder so               on sh.CompanyID = so.CompanyID and ll.OrderType = so.OrderType and ll.OrderNbr = so.OrderNbr
left join SOShipLineSplitPackage sp on sh.CompanyID = sp.CompanyID and sh.ShipmentNbr = sp.ShipmentNbr and sl.LineNbr = sp.ShipmentLineNbr and sl.InventoryID = sp.InventoryID
left join SOPackageDetail        pd on sh.CompanyID = pd.CompanyID and sh.ShipmentNbr = pd.ShipmentNbr and sp.PackageLineNbr = pd.LineNbr
inner join InventoryItem         ii on sh.CompanyID = ii.CompanyID and sl.InventoryID = ii.InventoryID
inner join BAccount              ba on sh.CompanyID = ba.CompanyID and sh.CustomerID = ba.BAccountID
inner join Customer              cu on sh.CompanyID = cu.CompanyID and sh.CustomerID = cu.BAccountID
inner join SOContact             sc on sh.CompanyID = sc.CompanyID and sh.ShipContactID = sc.ContactID and sh.CustomerID = sc.CustomerID
inner join SOAddress             sa on sh.CompanyID = sa.CompanyID and sh.ShipAddressID = sa.AddressID and sh.CustomerID = sa.CustomerID
inner join INsite                si on sh.CompanyID = si.CompanyID and sl.SiteID = si.SiteID
inner join Address               wa on sh.CompanyID = wa.CompanyID and si.AddressID = wa.AddressID and si.BAccountID = wa.BAccountID
left join SOShipmentKvExt        kc on sh.CompanyID = kc.CompanyID and sh.NoteID = kc.RecordID and kc.FieldName = 'AttributeCOURCODE'
left join SOShipmentKvExt        kn on sh.CompanyID = kn.CompanyID and sh.NoteID = kn.RecordID and kn.FieldName = 'AttributeCOURNAME'
left join SOShipmentKvExt        kr on sh.CompanyID = kr.CompanyID and sh.NoteID = kr.RecordID and kr.FieldName = 'AttributeCARRIER'
left join SOShipmentKvExt        kw on sh.CompanyID = kw.CompanyID and sh.NoteID = kw.RecordID and kw.FieldName = 'AttributeSHP2WH'
inner join jjStatusLookup         j on sh.Status = j.CStatus and j.tbl = 'SOShipment'

where sh.CompanyID = 2
and sh.ShipDate >= getdate()-60
and pd.TrackNumber is NOT NULL
and si.SiteCD != 'RLM NEJ HB'
)
, SecondLevel as(
select case when BadTracking = 1 and len(Tracking) > 0 then right(Tracking, (len(Tracking)-1)) else Tracking end Tracking2ndPass
     , * 
from TopLevel t
)
select s.ShipmentNbr
     , s.ShipLineNbr
     , s.Status
     , s.OrderNbr
     , s.OrderLineNbr
     , s.OrderType
     , s.InventoryCD
     , s.product_title
     , s.OrderQty
     , s.ShipQty
     , s.Tracking2ndPass Tracking
     , s.TrackingCreatedDate
     , s.ShipDate
     , s.CustomerID
     , s.Customer
     , case when charindex(' ', s.Customer) > 0
            then stuff(s.Customer, charindex(' ', s.Customer), len(s.Customer), '')
            else s.Customer end first_name
     , case when charindex(' ', s.Customer) > 0
            then stuff(s.Customer, 1, charindex(' ', s.Customer), '')
            else null end last_name
     , s.phone_number
     , s.Email
     , s.DestinationLine1
     , s.DestinationLine2
     , s.DestinationCity
     , s.DestinationState
     , s.DestinationCountry
     , s.DestinationZip
     , s.WarehouseID
     , s.OriginLine1
     , s.OriginLine2
     , s.OriginCity
     , s.OriginState
     , s.OriginCountry
     , s.OriginZip
     , s.PackageCount
     , s.LinePrice     
     , s.LineAmt
     , s.FreightAmt
     , s.SentToWH
     , s.OrderTotal
     , case when left(Tracking2ndPass, 1) = '9' 
                then 'usps'
            when left(Tracking2ndPass, 1) = '8' or left(Tracking2ndPass, 2) in('38', '39', '87')
                then 'fedex'
            when left(Tracking2ndPass, 2) = '1Z'
                then 'ups'
        else null end Slug
     , s.CourierCode
     , s.CourierName
     , s.CarrierName
     , s.OrderDate
     , s.CustomerClass
from SecondLevel s
order by ShipmentNbr desc, ShipLineNbr, OrderNbr, OrderLineNbr