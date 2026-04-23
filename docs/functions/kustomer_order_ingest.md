# kustomer_order_ingest
Gets all recent orders from AcumaticaDb/orders that haven't been sent to Kustomer

Pulls shipment details & tracking, formats, then sends to Kustomer

## Schedule
- ### :00, :12, :24, :36, :48

## Execution Behavior
Executes single pipeline, **SendOrderDetailsToKustomer** with **'ingest'** passed to *_re_init* 
- We don't need to pass any value to _re_init. 'ingest' = default value

## Pipelines

### SendOrderDetailsToKustomer
#### `SendOrderDetailsToKustomer` Pipeline Documentation — [pipelines/kustomer.py](../../pipelines/kustomer.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([kustomer_order_ingest]) --> B[SendOrderDetailsToKustomer.__init__]
    B --> B1[init Transform]
    B --> B2[init Kustomer API connector]
    B --> B3[init Load]
    A -->|"_re_init()"| Q1[set query: Kustomer_OrderIngest<br/>top 120, new orders only]
    Q1 --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(AcuDB: SOOrder + customer,<br/>address, contact, status joins)]
    EX --> D2[(AcuDB: K_OrderIngest<br/>filter already-sent orders)]

    RUN --> TR[transform]
    TR --> T1[format_data_extract: rows to dicts,<br/>format state and country names]
    T1 --> D3[(AcuDB: Kustomer_ShipmentData<br/>SOOrder + SOShipment + SOPackageDetail)]
    TR --> T2[add_shipments_to_orders: build order-line,<br/>shipment-line, and package DataFrames]
    TR --> T3[smash_orders: nest packages into<br/>shipments into lines into orders]

    RUN --> LD[load]
    LD --> LS1[filter: exclude orders whose<br/>JSON payload is unchanged]
    LS1 --> D4[(AcuDB: K_OrderIngest<br/>read existing json for comparison)]
    LS1 --> LS2[send_payloads: POST each order<br/>to Kustomer webhook]
    LS2 --> D5[(KustomerAPI: webhook POST)]
    LS2 --> LS3[format_db_row + batch upsert<br/>every 25 orders]
    LS3 --> D6[(AcuDB: K_OrderIngest<br/>upsert on OrderNbr + Status)]

    RUN --> LR[log_results<br/>*Do nothing]
```

## Queries
### AcumaticaDb
 - #### [Kustomer_OrderIngest.sql](../../sql/queries/AcumaticaDb/Kustomer_OrderIngest.sql)
### db_CentralStore
 - #### [placeholder.sql](../../sql/queries/db_CentralStore/placeholder.sql)
