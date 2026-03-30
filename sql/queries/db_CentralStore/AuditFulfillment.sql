SELECT top 20 Id
      ,CompanyID
      ,PostedDate
      ,LastDate
      ,f.OrderType
      ,f.ShipmentNbr
      ,ShipmentStatus
      ,FF_ShipmentNbr
      ,FF_Status
      ,WarehouseID
      ,TPL_Processor
      , d.ShipmentNbr DeletedShipNbr
FROM [acu].[Fulfillment] f
left join acu.ShipmentDeletions d on f.ShipmentNbr = d.ShipmentNbr and f.OrderType = d.OrderType
WHERE 
 ([FF_Status] NOT IN ('DELETED', 'COMPLETED', 'ERROR', 'TRANSMITTED'))
AND ([ShipmentStatus] NOT IN ('COMPLETED', 'Confirmed', 'Invoiced', ''))
and d.ShipmentNbr is null
order by PostedDate desc