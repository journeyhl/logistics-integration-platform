```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([criteo]) --> B[Criteo.__init__]
    B --> B1[init SQLConnector: CentralStore]
    B --> B2[init SQLConnector: AcumaticaDb]
    B --> B3[init Logger]
    B --> B4[init CriteoAPI: OAuth2 auth on init]
    B --> B5[init Transform]
    A --> RI[_re_init: set start_date, end_date, mode]
    RI --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(CentralStore: criteo.campaign_performance_daily)]
    EX --> API[CriteoAPI.fetch_campaign_data]
    API --> D2[(CriteoAPI: statistics/report)]

    RUN --> TR[transform]
    TR --> T1[transform_criteo: drop rollups, rename cols, cast types]
    TR --> T2[find_differences: full join DB vs API on date + campaign_id]
    T2 --> T3{row status?}
    T3 -->|only in API| T4[queue for insert]
    T3 -->|in both, changed| T5[queue for update, log diff=1]
    T3 -->|in both, unchanged| T6[log diff=0 only]

    RUN --> LD[load]
    LD --> LS1[upsert diff entries]
    LS1 --> CS1[(CentralStore: criteo.diff_log)]
    LD --> LS2[upsert campaign performance]
    LS2 --> CS2[(CentralStore: criteo.campaign_performance_daily)]

    RUN --> LR[log_results<br/>*Do nothing]
```
