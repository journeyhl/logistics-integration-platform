with RowOrder as(
    select *, ROW_NUMBER() over(partition by ShipmentNbr, TrackingNbr order by ShipLineNbr) Num
    , ROW_NUMBER() over(partition by ShipmentNbr order by ShipLineNbr) PackageNum
    from acu.rsFulfill
    where ffStatus = 'Ready'
), 
PackageLines as(
    Select ShipmentNbr, TrackingNbr, max(num) ItemsOnPackage
    from RowOrder
    group by ShipmentNbr, TrackingNbr

), 
acuRsShipments as(
    select s.UniqueID ShipmentID, s.OrderUniqueID ShipmentNbr, pi.InventoryCD, p.TrackingNumber
    , s.Carrier CourierCode
    , s.Carrier Carrier
    , p.Courier CourierName

    from acu.rsShipment s
    inner join acu.rsPackage p on s.PackageID = p.PackageID and s.TrackerID = p.TrackerID 
    inner join acu.rsPackageItems pi on p.PackageID = pi.PackageID and p.TrackerID = pi.TrackerID
)

select *, 
case when rsQty = (select sum(ShipQty) from acu.rsFulfill ff where r.ShipmentNbr = ff.ShipmentNbr and r.InventoryCD = ff.InventoryCD) 
 and 1 = (select distinct 1 from acu.rsFulfill f where r.ShipmentNbr = f.ShipmentNbr and (f.rsStatus is null or r.rsStatus != f.rsStatus) and ffStatus is null)
		then 'Partially Complete' 
	 when rsQty = (select sum(ShipQty) from acu.rsFulfill ff where r.ShipmentNbr = ff.ShipmentNbr and r.InventoryCD = ff.InventoryCD) 
		then 'Complete' 
	 else 'Not' 
end Complete,
(select ItemsOnPackage from PackageLines l where l.ShipmentNbr= r.ShipmentNbr and l.TrackingNbr = r.TrackingNbr) ItemsOnPackage
, r.AcuPackageStatus
, (select max(PackageNum) from RowOrder r1 where r.ShipmentNbr = r1.ShipmentNbr) MaxPackageNum
, s.CourierCode
, s.CourierName
, s.Carrier

from RowOrder r
inner join acuRsShipments s on r.ShipmentNbr = s.ShipmentNbr and r.ShipmentID = s.ShipmentID and r.InventoryCD = s.InventoryCD and r.TrackingNbr = s.TrackingNumber

where r.ShipmentNbr in(
    Select ShipmentNbr
    from RowOrder
    group by ShipmentNbr
    having max(num) <= 4
    )
and ffStatus is not null and ffStatus != 'Complete' --and ShipmentNbr != '069861'
order by r.ShipmentNbr, ShipLineNbr