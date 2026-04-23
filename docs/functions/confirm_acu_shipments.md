# confirm_acu_shipments
 *1. Pulls all Open RedStag Shipments that have a Tracking Number and are ready to be confirmed*

 *2. Formats payload for Shipment Confirmation via Acumatica API*

 *3. Sends payload to confirm each Shipment***

## Schedule
- ### :00, :20, :40

## Execution Behavior
Executes single pipeline, **ShipmentsReadyToConfirm**

## Pipelines

### ShipmentsReadyToConfirm
#### `ShipmentsReadyToConfirm` Pipeline Documentation — [pipelines/confirm_open_shipments.py](../../pipelines/confirm_open_shipments.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([confirm_open_shipments]) --> B[ShipmentsReadyToConfirm.__init__]
    B --> B1[init SQLConnector: CentralStore]
    B --> B2[init SQLConnector: AcumaticaDb]
    B --> B3[init Logger]
    B --> B4[init AcumaticaAPI]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(AcuDB: ShipmentsReadyToConfirm<br/>open shipments with tracking<br/>where shipped qty = packed qty)]

    RUN --> TR[transform]
    TR --> T1[convert to list of ShipmentNbr dicts]

    RUN --> LD[load]
    LD --> LS[for each shipment — sleep 3s]
    LS --> L1[(AcuAPI: POST /Shipment/ConfirmShipment)]

    RUN --> LR[log_results]
    LR --> LO[acu_api._logout]
    LR --> UPS[(CentralStore: upsert _util.acu_api_log)]
```

## Queries
### AcumaticaDb
 - #### [ShipmentsReadyToConfirm.sql](../../sql/queries/AcumaticaDb/ShipmentsReadyToConfirm.sql)