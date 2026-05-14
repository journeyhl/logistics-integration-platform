# rmi_link_to_acumatica
Gets all QT type Sales Orders from AcumaticaDb that were modified within the last day and loads them to **acu.Quotes**

## Schedule
- ### :00, :30

## Execution Behavior
Executes single pipeline, **RMILinkToAcu**

## Pipelines

### RMILinkToAcu
#### `RMILinkToAcu` Pipeline Documentation — [pipelines/rmi_link_to_acu.py](../../pipelines/rmi_link_to_acu.py)
```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([run_rmi_link_to_acu]) --> B[RMILinkToAcu.__init__]
    B --> B1[inherits Pipeline]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(
        <b><i>AcuDB</i></b>
        RMI_Link3PL: Query
    )]
    EX --> D2[(
        <b><i>CentralStore</i></b>
        RMI_Link3PL_RMAStatus: Query
    )]
    EX --> CTX[
        pl.SQLContext<br/>acu = acu_extract<br/>rmi = rmi_extract
    ]

    RUN --> TR[transform]
    TR --> T1[inner join acu vs rmi<br/>on a.ShipmentNbr = r.RMANumber]
    T1 --> T2[derive ValueString:<br/>FieldName='AttributeLINK3PL' → Link3PL URL<br/>FieldName='AttributeRMAID' → RMAID<br/>else null]
    T2 --> T3[to_dicts → list of rows]

    RUN --> LD[load]
    LD --> LD1[(
        <b><i>AcuDB</i></b>
        checked_upsert_paginated<br/>SOShipmentKvExt<br/>writes ValueString for<br/>AttributeLINK3PL + AttributeRMAID
    )]

    RUN --> LR[log_results<br/>*Do nothing]
    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```



## Queries
### AcumaticaDb
 - #### [RMI_Link3PL.sql](../../sql/queries/AcumaticaDb/RMI_Link3PL.sql)
### db_CentralStore
 - #### [RMI_Link3PL_RMAStatus.sql](../../sql/queries/db_CentralStore/RMI_Link3PL_RMAStatus.sql)