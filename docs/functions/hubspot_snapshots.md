# hubspot_snapshots
Gets all QT type Sales Orders from AcumaticaDb that were modified within the last day and loads them to **acu.Quotes**

## Schedule
- ### 11:30pm

## Execution Behavior
Executes single pipeline, **HubSpotSnapshot**

## Pipelines

### HubSpotSnapshot
#### `HubSpotSnapshot` Pipeline Documentation — [pipelines/hubspot_snapshot.py](../../pipelines/hubspot_snapshot.py)
```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([hubspot_snapshot]) --> B[HubSpotSnapshot.__init__]
    B --> B1[inherits Pipeline]
    B --> B2[ 
        self.hubapi = HubSpotAPI
        self.transformer = Transform
        self.hubapi._set_snapshot_windows
    ]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> HS[(HubSpotAPI: owners already loaded
    search_deals: all B2B deals
    search_activities: calls, emails, meetings, tasks)]
    HS --> EX_OUT[data_extract:
    owners, deals, calls,<br/>emails, meetings, tasks, timestamp]

    RUN --> TR[transform]
    TR --> T1[activity_counts: 
    sum activity per window ]
    T1 --> T2[smash_activity_counts: 
    flatten to sql format]
    TR --> T3[deals: 
    format each deal for sql]
    T2 & T3 --> TR_OUT[db_activities + db_deals]

    RUN --> LD[load]
    LD --> CS1[(
        <b><i>CentralStore</i></b>
        upsert hs.deal_tracking
    )]
    LD --> CS2[(
        <b><i>CentralStore</i></b>
        insert hs.deal_snapshots
    )]
    LD --> CS3[(
        <b><i>CentralStore</i></b>
        insert hs.activity_snapshots
    )]

    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```


## Queries
None
