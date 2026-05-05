
select distinct sh.ShipmentNbr
     , 2 CompanyID
     , sh.NoteID RecordID
     , null ValueNumeric
     , null ValueDate
     , null ValueText
     , 'AttributeLINK3PL' FieldName

from SOShipment sh
inner join SOShipLine shl on sh.CompanyID = shl.CompanyID and sh.ShipmentNbr = shl.ShipmentNbr and sh.ShipmentType = shl.ShipmentType
inner join INSite si on sh.CompanyID = si.CompanyID and shl.SiteID = si.SiteID
inner join JJStatusLookup j on sh.Status = j.CStatus and j.Tbl = 'SOShipment'
left join SOShipmentKvExt k_wh on sh.CompanyID = k_wh.CompanyID and sh.NoteID = k_wh.RecordID and k_wh.FieldName = 'AttributeSHP2WH'
left join SOShipmentKvExt k_lk on sh.CompanyID = k_lk.CompanyID and sh.NoteID = k_lk.RecordID and k_lk.FieldName = 'AttributeLINK3PL'
where sh.CompanyID = 2 and si.SiteCD = 'RMI'
and sh.CreatedDateTime >= '20260301'
and k_lk.ValueString is null

