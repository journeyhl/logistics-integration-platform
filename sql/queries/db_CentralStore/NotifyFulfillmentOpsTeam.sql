
select distinct r.KeyValue
     , r.RMI_Response
     , s.ReturnStatus
     , rtrim(s.ReturnItem) InventoryCD
     , i.[Description]
     , s.ReturnType
     , concat('https://erp.journeyhl.com/(W(11))/Main?CompanyID=JHL&ScreenId=SO301000&OrderType=', s.ReturnType, '&OrderNbr=', s.ReturnNbr) ReturnLink
from _util.rmi_send_log r
left join acu.[Returns] s on r.KeyValue = s.ReturnNbr
left join acu.InventorySummary i on s.ReturnItem = i.InventoryCD
left join _util.SOOrderDeletions d on r.KeyValue = d.OrderNbr
where left(r.RMI_Response, 9) = '200 ERROR'
and right(r.RMI_Response, 19) = 'Item does not exist'
and (s.ReturnNbr is null or s.ReturnStatus = 'Open')
and d.OrderNbr is null