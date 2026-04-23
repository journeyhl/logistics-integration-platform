# rmi_data_retrieval_pipeline
**Hits RMI's *ClosedShipmentsV1* endpoint, transforms, then upserts to *rmi_ClosedShipments***

**Hits RMI's *Receipts* endpoint , transforms, then upserts to *rmi_Receipts***

**Hits RMI's *RMAs* endpoint , transforms, then upserts to *rmi_RMAStatus***

## Schedule
- ### :25

## Execution Behavior
```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    TRIGGER([rmi_data_retrieval_pipeline<br/>timer: :25 every hour])

    TRIGGER --> GCS[GetClosedShipmentsFromRMI.run<br/>ClosedShipmentsV1 — last 21 days]
    TRIGGER --> GR[GetReceiptsFromRMI.run<br/>Receipts — last 21 days]
    TRIGGER --> GRMA[GetRMAsFromRMI.run<br/>RMAs — last 120 days]

    GCS --> GCS_DB[(CentralStore: rmi_ClosedShipments)]
    GR --> GR_DB[(CentralStore: rmi_Receipts)]
    GRMA --> GRMA_DB[(CentralStore: rmi_RMAStatus)]
```

## Pipelines

### GetClosedShipmentsFromRMI
#### `GetClosedShipmentsFromRMI` Pipeline Documentation — [pipelines/get_closed_shipments_from_RMI.py](../../pipelines/get_closed_shipments_from_RMI.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([get_closed_shipments_from_rmi]) --> B[GetClosedShipmentsFromRMI.__init__]
    B --> B1[init RMIAPI: token auth on init]
    B --> B2[init Transform]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract: RMIAPI.closed_shipments]
    EX --> D1[(RMIAPI: ClosedShipmentsV1 — last 21 days)]

    RUN --> TR[transform: flatten shipLines per RMA]

    RUN --> LD[load: checked_upsert]
    LD --> CS1[(CentralStore: rmi_ClosedShipments)]

    RUN --> LR[log_results<br/>*Do nothing]
```

<hr>

### GetReceiptsFromRMI
#### `GetReceiptsFromRMI` Pipeline Documentation — [pipelines/get_receipts_from_rmi.py](../../pipelines/get_receipts_from_rmi.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([get_receipts_from_rmi]) --> B[GetReceiptsFromRMI.__init__]
    B --> B1[init RMIAPI: token auth on init]
    B --> B2[init Transform]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract: RMIAPI.get_receipts]
    EX --> D1[(RMIAPI: Receipts — last 21 days)]

    RUN --> TR[transform: map receipt fields]

    RUN --> LD[load: checked_upsert]
    LD --> CS1[(CentralStore: rmi_Receipts)]

    RUN --> LR[log_results<br/>*Do nothing]
```


<hr>

### GetRMAsFromRMI
#### `GetRMAsFromRMI` Pipeline Documentation — [pipelines/get_rmas_from_rmi.py](../../pipelines/get_rmas_from_rmi.py)

```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([get_rmas_from_rmi]) --> B[GetRMAsFromRMI.__init__]
    B --> B1[init RMIAPI: token auth on init]
    B --> B2[init Transform]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract: RMIAPI.target_api 'RMAs']
    EX --> D1[(RMIAPI: RMAs — last 21 days)]

    RUN --> TR[transform: transform_status_records<br/>flatten rmaLines per RMA]

    RUN --> LD[load: checked_upsert]
    LD --> CS1[(CentralStore: rmi_RMAStatus)]

    RUN --> LR[log_results<br/>*Do nothing]
```

## Queries
None