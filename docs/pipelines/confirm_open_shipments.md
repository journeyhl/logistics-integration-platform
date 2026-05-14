```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([confirm_open_shipments]) --> B[ShipmentsReadyToConfirm.__init__]
    B --> B1[
        self.acu_api = AcumaticaAPI
    ]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(
        <b><i>AcuDb</i></b>
        ShipmentsReadyToConfirm<br/>open shipments with tracking<br/>where shipped qty = packed qty)]

    RUN --> TR[transform]
    TR --> T1[convert to list of ShipmentNbr dicts]

    RUN --> LD[load]
    LD --> LS[for each shipment — sleep 3s]
    LS --> L1[(AcuAPI: POST /Shipment/ConfirmShipment)]

    RUN --> LR[log_results]
    LR --> LO[acu_api._logout]
    LR --> UPS[(
        <b><i>CentralStore</i></b>
        upsert _util.acu_api_log
    )]
    RUN --> LOGS[(
        <b><i>CentralStore</i></b>
        _util.Logs<br/>insert run logs
    )]
```
