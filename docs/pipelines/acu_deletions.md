```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([acu_deletions]) --> B[AcumaticaDeletions.__init__]
    B --> B1[init SQLConnector: CentralStore]
    B --> B2[init SQLConnector: AcumaticaDb]
    B --> B3[init Logger]
    
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(AcuDB: SOOrderDeletions)]
    EX --> D2[(AcuDB: SOLineDeletions)]
    EX --> D3[(AcuDB: SOShipmentDeletions)]
    EX --> D4[(AcuDB: SOOrderShipmentDeletions)]

    RUN --> TR[transform]
    TR --> T1[df.to_dicts for each deletion type]

    RUN --> LD[load]
    LD --> LS[checked_upsert each deletion type]
    LS --> CS1[(CentralStore: _util.SOOrderDeletions)]
    LS --> CS2[(CentralStore: _util.SOLineDeletions)]
    LS --> CS3[(CentralStore: _util.SOShipmentDeletions)]
    LS --> CS4[(CentralStore: _util.SOOrderShipmentDeletions)]
    LS --> CL[clean]
    CL --> CL1[DELETE acu.SalesOrders joined on SOOrderDeletions]
    CL --> CL2[DELETE acu.SalesOrders lines joined on SOLineDeletions]
    CL --> CL3[DELETE acu.Shipments joined on SOShipmentDeletions]

    RUN --> LR[log_results<br/>*Do nothing]
```
