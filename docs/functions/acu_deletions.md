# acu_deletions
Pipeline to extract deleted records from **SOOrder**, **SOLine**, **SOOrderShipment** and **SOShipment** in *Acumatica* and load them to ***db_CentralStore***


## Schedule
- ### :40

## Execution Behavior
Executes single pipeline, **AcumaticaDeletions**

## Pipelines

### AcumaticaDeletions
#### `AcumaticaDeletions` Pipeline Documentation — [pipelines/acu_deletions.py](../../pipelines/acu_deletions.py)
```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([acu_deletions]) --> B[AcumaticaDeletions.__init__]
    B --> B1[inherits Pipeline]
    
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(
        <b><i>AcuDb</i></b>
        SOOrderDeletions: Query
    )]
    EX --> D2[(
        <b><i>AcuDb</i></b>
        SOLineDeletions: Query
    )]
    EX --> D3[(
        <b><i>AcuDb</i></b>
        SOShipmentDeletions: Query
    )]
    EX --> D4[(
        <b><i>AcuDb</i></b>
        SOOrderShipmentDeletions: Query
    )]

    RUN --> TR[transform]
    TR --> T1[df.to_dicts for each deletion type]

    RUN --> LD[load]
    LD --> LS[checked_upsert each deletion type]
    LS --> CS1[(
        <b><i>CentralStore</i></b>
        upsert _util.SOOrderDeletions
    )]
    LS --> CS2[(
        <b><i>CentralStore</i></b>
        upsert _util.SOLineDeletions
    )]
    LS --> CS3[(
        <b><i>CentralStore</i></b>
        upsert _util.SOShipmentDeletions
    )]
    LS --> CS4[(
        <b><i>CentralStore</i></b>
        upsert _util.SOOrderShipmentDeletions
    )]
    LS --> CL[clean]
    CL --> CL1[DELETE acu.SalesOrders joined on SOOrderDeletions]
    CL --> CL2[DELETE acu.SalesOrders lines joined on SOLineDeletions]
    CL --> CL3[DELETE acu.Shipments joined on SOShipmentDeletions]

    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```


## Queries
### AcumaticaDb
 - #### [SOOrderDeletions.sql](../../sql/queries/AcumaticaDb/SOOrderDeletions.sql)
 - #### [SOLineDeletions.sql](../../sql/queries/AcumaticaDb/SOLineDeletions.sql)
 - #### [SOShipmentDeletions.sql](../../sql/queries/AcumaticaDb/SOShipmentDeletions.sql)
 - #### [SOOrderShipmentDeletions.sql](../../sql/queries/AcumaticaDb/SOOrderShipmentDeletions.sql)