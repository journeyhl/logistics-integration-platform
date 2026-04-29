/****** Object:  View [acu].[AftershipShipments]    Script Date: 4/28/2026 10:07:02 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create view [acu].[AftershipShipments]
as
SELECT s.[ShipmentNbr]
      , s.[ShipLineNbr]
      , s.[Status]
      , s.[OrderNbr]
      , s.[OrderLineNbr]
      , s.[OrderDate]
      , s.[InventoryCD]
	  , i.Description product_title
      , s.[OrderQTY]
      , s.[ShipQTY]
	  , s.Tracking
      , s.TrackingCreatedDate
      , s.[ShipDate]
      , s.[CustomerID]
      ,case when s.Attention != '' and s.Attention is not null then s.Attention else s.[AccountName] end Customer
	  , case when c.CustomerClass = 'B2B' and ContactName is not null and ContactName != '' and s.AccountName = c.AccountName
then 
SUBSTRING(LTRIM(c.ContactName), 0, charindex(' ',LTRIM(c.ContactName)))
else 
SUBSTRING(LTRIM(case when s.Attention != '' and s.Attention is not null then s.Attention else s.[AccountName] end), 0, charindex(' ',LTRIM(case when s.Attention != '' and s.Attention is not null then s.Attention else s.[AccountName] end))) end recipient_first_name,

case when c.CustomerClass = 'B2B' and ContactName is not null and ContactName != '' and ltrim(ContactName) like '% %' and s.AccountName = c.AccountName
then 
STUFF(LTRIM(c.ContactName), 1, charindex(' ',LTRIM(c.ContactName)),'')
else 
STUFF(LTRIM(case when s.Attention != '' and s.Attention is not null then s.Attention else s.[AccountName] end), 1, charindex(' ',LTRIM(case when s.Attention != '' and s.Attention is not null then s.Attention else s.[AccountName] end)),'') end recipient_last_name
	  ,case when s.Country in('US', 'CA') and s.CustomerPhone not like '%+%' then concat('+1', s.CustomerPhone)
	  else s.CustomerPhone end phone_number
      , s.[Email] 
      , s.[AddressLine1] DestinationLine1
      , s.[AddressLine2] DestinationLine2
      , s.[City] DestinationCity
      , s.[State] DestinationState
      , s.[Country] DestinationCountry
      , s.[Zip] DestinationZip
      , s.[WarehouseID]
	  , w.AddressLine1 OriginLine1
	  , w.AddressLine2 OriginLine2
	  , w.City OriginCity
	  , w.State OriginState
	  , w.Country OriginCountry
	  , w.PostalCode OriginZip
      , s.[PackageCount]
      , s.[SOLinePrice]
      , s.[FreightCost]
      , s.[SenttoWH]
	  , o.OrderTotal
	  , case 
	  when Tracking like '%,9%' then 'usps'	  
	  when sa.Slug is not null then sa.Slug
		 when s.WarehouseID in('Seko-WC', 'Seko-EC') and s.Carrier is null and s.ShipVia in(
			'PSC150THRES',
			'WHITEGLOVE399',
			'WHITEGLOVE450',
			'WHITEGLOVE499',
			'WHITEGLOVESHP') 
			then 'sekologistics' else '' end slug
	 , s.Carrier
	 , s.CourierCode
	 , s.CourierName
  FROM acu.Shipments s inner join
		acu.WarehouseAddresses w on s.WarehouseID = w.Warehouse inner join
		acu.InventorySummary i  on s.InventoryCD = i.InventoryCD inner join
		acu.Customers c on s.CustomerID = c.CustomerID inner join
		acu.SalesOrders o on s.OrderNbr = o.OrderNumber and s.OrderLineNbr = o.LineNbr left join
		SlugsAftership sa on case when s.Carrier = 'SmartPost' then 'fedex' else s.Carrier end = sa.Carrier	
where s.ShipDate >= GETDATE()-100 
and Tracking is not null 
and Tracking != 'Undefined' 
and TrackingCreatedDate is not null
and (s.ShipLineNbr = 1 
	 or (s.ShipLineNbr != 1 and s.Tracking != (select Tracking from acu.Shipments h where h.ShipmentNbr = s.ShipmentNbr and h.ShipLineNbr = 1))
	 )
and s.WarehouseID != 'RLM NEJ HB'

GO


