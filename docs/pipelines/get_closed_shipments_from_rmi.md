```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([get_closed_shipments_from_rmi]) --> B[GetClosedShipmentsFromRMI.__init__]
    B --> B1[inherits Pipeline]
    B --> B2[
        self.rmi = RMIAPI
        self.transformer = Transform
        self.payload_template
    ]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract: RMIAPI.closed_shipments]
    EX --> D1[(RMIAPI: ClosedShipmentsV1 — last 21 days)]

    RUN --> TR[transform: flatten shipLines per RMA]

    RUN --> LD[load: checked_upsert]
    LD --> CS1[(
        <b><i>CentralStore</i></b>
        upsert rmi_ClosedShipments
    )]

    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```
