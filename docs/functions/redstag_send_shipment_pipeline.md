# redstag_send_shipment_pipeline
**summary**

## Schedule
- ### :05, :15, :35

## Execution Behavior
Executes single pipeline, **SendRedStagShipments**

---

## Pipelines

### SendRedStagShipments
#### `SendRedStagShipments` Pipeline Documentation — [pipelines/redstag_send_shipments.py](../../pipelines/redstag_send_shipments.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([redstag_send_shipments]) --> B[SendRedStagShipments.__init__]
    B --> B1[init Transform]
    B --> B2[init RedStagAPI]
    B --> B3[init AcumaticaAPI]
    B --> B4[init Load]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(AcuDB: SOShipment + SOShipLine<br/>SiteCD=RedStag%, AttributeSHP2WH=0,<br/>Status not in C/L/F/I, OrigOrderType != RC)]

    RUN --> TR[transform]
    TR --> TR1{rsOrderID<br/>already set?}
    TR1 -->|no| TR2[build order.search payload<br/>unique_id = ShipmentNbr]
    TR2 --> RS1[(RedStagAPI: order.search)]
    RS1 --> TR3{found at<br/>RedStag?}
    TR3 -->|yes| TR4[note = Already at RedStag<br/>execution = order.search]
    TR3 -->|no| TR5[build order.create payload<br/>items + shipping address + shipVia]
    TR5 --> TR6[_determine_shipvia<br/>resolve ShipVia across multi-line shipments]
    TR6 --> TR7[execution = order.create]
    TR1 -->|yes| TR8[skip lookup]

    RUN --> LD[load]
    LD --> LD1[send_shipments: per shipment]
    LD1 --> RS2[(RedStagAPI: execute order.search or order.create)]
    RS2 --> LD2[transform_acu_attribute_payload<br/>rsOrderID, rsOrderNum,<br/>courier code/name, SHP2WHDT]
    LD2 --> ACU1[(AcuAPI: send_to_wh_v2<br/>set AttributeSHP2WH = true)]

    RUN --> LR[log_results]
    LR --> LO[AcumaticaAPI._logout]
    LR --> UPS[(CentralStore: upsert _util.acu_api_log)]
```

## Queries
### AcumaticaDb
 - #### [SendRedStagShipments.sql](../../sql/queries/AcumaticaDb/SendRedStagShipments.sql)