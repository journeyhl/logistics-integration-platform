# pack_shipments
1. Queries CentralStore RedStag tables and json.RedStagEvents to find RedStag shipments ready to be packed and have tracking added.*

```python 
central_extract = self.centralstore.query_db(self.centralstore.queries.PackShipment.query)
```
```python 
redstag_event_extract = self.centralstore.query_db(self.centralstore.queries.RedStagEvents.query)
```
2. Queries AcumaticaDb for Open Shipments that have been sent to Warehouse but don't have a Tracking Nbr*
   
```python 
acu_extract = self.acudb.query_db(self.acudb.queries.PackShipment.query)
```
3. Matches results from Acumatica to one or both of the CentralStore extracts, then formats the Package payload to be sent to Acumatica's API*

4. Sends each Shipments Package Payload to Acumatica API*

## Schedule
- ### :0, :15, :30, :45

## Execution Behavior
Executes single pipeline, **PackShipments**

## Pipelines

### PackShipments
#### `PackShipments` Pipeline Documentation — [pipelines/pack_shipments.py](../../pipelines/pack_shipments.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([pack_shipments]) --> B[PackShipments.__init__]
    B --> B1[init AcumaticaAPI]
    B --> B2[init Transform]
    B --> B3[init Load]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(CentralStore: PackShipmentRedStag)]
    EX --> D2[(CentralStore: RedStagEvents)]
    EX --> D3[(CentralStore: PackShipmentRMI)]
    EX --> D4[(AcuDB: PackShipment)]

    RUN --> TR[transform]
    TR --> T1[transform_redstag_events]
    TR --> T2[match each acu_shipment vs central / redstag / rmi]
    T2 --> MATCH{match found?}
    MATCH -->|central| M1[format central payload]
    MATCH -->|redstag| M2[format redstag payload]
    MATCH -->|rmi| M3[format rmi payload]
    MATCH -->|none| SKIP[skip]
    M1 & M2 & M3 --> GT[group_tracking]
    GT --> FP[_format_package]
    GT --> FFP[_format_friendly_package_payload]

    RUN --> LD[load]
    LD --> LS[load_shipments]
    LS --> SD[acu_api.shipment_details]
    SD --> PKG{Packages contents = Lines contents?}
    PKG -->|Yes| AP[acu_api.add_package_v2]
    PKG -->|No| GP[acu_api.get_package_details]

    RUN --> LR[log_results]
    LR --> LO[acu_api._logout]
    LR --> UPS[(CentralStore: upsert _util.acu_api_log)]
```

## Queries
### AcumaticaDb
 - #### [PackShipment.sql](../../sql/queries/AcumaticaDb/PackShipment.sql)
### db_CentralStore
 - #### [PackShipmentRedStag.sql](../../sql/queries/db_CentralStore/PackShipmentRedStag.sql)
 - #### [RedStagEvents.sql](../../sql/queries/db_CentralStore/RedStagEvents.sql)
 - #### [PackShipmentRMI.sql](../../sql/queries/db_CentralStore/PackShipmentRMI.sql)