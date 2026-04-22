```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([get_rmas_from_rmi]) --> B[GetRMAsFromRMI.__init__]
    B --> B1[init RMIAPI: token auth on init]
    B --> B2[init Transform]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract: RMIAPI.target_api 'RMAs']
    EX --> D1[(RMIAPI: RMAs — last 120 days)]

    RUN --> TR[transform: transform_status_records<br/>flatten rmaLines per RMA]

    RUN --> LD[load: checked_upsert]
    LD --> CS1[(CentralStore: rmi_RMAStatus)]

    RUN --> LR[log_results<br/>*Do nothing]
```
