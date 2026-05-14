# sales_order_cleaner
Gets all QT type Sales Orders from AcumaticaDb that were modified within the last day and loads them to **acu.Quotes**

## Schedule
- ### :00, :30

## Execution Behavior
Executes single pipeline, **SalesOrderCleaner**

## Pipelines

### SalesOrderCleaner
#### `SalesOrderCleaner` Pipeline Documentation — [pipelines/sales_order_cleaner.py](../../pipelines/sales_order_cleaner.py)
```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([run_sales_order_cleaner]) --> B[SalesOrderCleaner.__init__]
    B --> B1[inherits Pipeline]
    B --> B2[
        self.transformer = Transform
    ]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(
        <b><i>CentralStore</i></b>
        SalesOrderCleaner: Query
        )]

    RUN --> TR[transform]
    TR --> T1[collect distinct OrderNbrs<br/>from duplicates]
    T1 --> D2[(
        <b><i>AcuDb</i></b>
        SOOrder + jjStatusLookup<br/>get current authoritative status<br/>for each OrderNbr)]
    D2 --> T2[join CentralStore results<br/>with AcuDB statuses]
    T2 --> T3[group orders by<br/>current AcuDB status]
    T3 --> T4[build DELETE command per status group<br/>DELETE FROM acu.SalesOrders<br/>WHERE Status != current AND OrderNumber IN list]

    RUN --> LD[load]
    LD --> LD1{orders out<br/>of sync?}
    LD1 -->|yes, per status group| LD2[(
        <b><i>CentralStore</i></b>
        raw DELETE<br/>acu.SalesOrders rows<br/>not matching AcuDB status)]
    LD1 -->|no| SKIP[skip]

    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```

## Queries
### db_CentralStore
 - #### [SalesOrderCleaner.sql](../../sql/queries/db_CentralStore/SalesOrderCleaner.sql)