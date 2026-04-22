```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([run_sales_order_cleaner]) --> B[SalesOrderCleaner.__init__]
    B --> B1[init Transform]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(CentralStore: acu.SalesOrders<br/>find OrderNumbers with duplicate statuses<br/>via SalesOrderCleaner.sql)]

    RUN --> TR[transform]
    TR --> T1[collect distinct OrderNbrs<br/>from duplicates]
    T1 --> D2[(AcuDB: SOOrder + jjStatusLookup<br/>get current authoritative status<br/>for each OrderNbr)]
    D2 --> T2[join CentralStore results<br/>with AcuDB statuses]
    T2 --> T3[group orders by<br/>current AcuDB status]
    T3 --> T4[build DELETE command per status group<br/>DELETE FROM acu.SalesOrders<br/>WHERE Status != current AND OrderNumber IN list]

    RUN --> LD[load]
    LD --> LD1{orders out<br/>of sync?}
    LD1 -->|yes, per status group| LD2[(CentralStore: raw DELETE<br/>acu.SalesOrders rows<br/>not matching AcuDB status)]
    LD1 -->|no| SKIP[skip]

    RUN --> LR[log_results<br/>*Do nothing]
```
