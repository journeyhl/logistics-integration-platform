```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([aftership_to_dbc]) --> B[AfterShipToDbc.__init__]
    B --> B1[
        self.aftership = AfterShip
        self.transformer = Transform
    ]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> AS[(AfterShip: GET /tracking/2026-01/trackings
    created_at_min = now - 7 days
    paginated via cursor until has_next_page = false)]
    AS --> EX_OUT[list of raw tracking dicts]

    RUN --> TR[transform]
    TR --> T1[skip rows with no customer phone]
    T1 --> T2[map each tracking to AftershipExport row<br/>convert updated_at + created_at to ET]
    T2 --> T3[for each checkpoint in tracking<br/>map to AftershipExportDetail row<br/>convert checkpoint_time to ET]
    T3 --> TR_OUT[aftership_export + aftership_export_detail]

    RUN --> LD[load]
    LD --> CS1[(
        <b><i>CentralStore</i></b>
        upsert acu.AftershipExportv2
    )]
    LD --> CS2[(
        <b><i>CentralStore</i></b>
        upsert acu.AftershipExportDetailv2
    )]

    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```
