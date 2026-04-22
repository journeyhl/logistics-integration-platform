```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([create_acu_receipt]) --> B[CreateAcuReceipt.__init__]
    B --> B1[init SQLConnector: CentralStore]
    B --> B2[init SQLConnector: AcumaticaDb]
    B --> B3[init Logger]
    B --> B4[init AcumaticaAPI]
    B --> B5[init Transform]
    B --> B6[init Load]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(CentralStore: rmi_RMAStatus<br/>RMAType=3, status CLOSED or RECEIVED)]
    EX --> D2[(AcuDB: OpenRCsNoReceipt<br/>RC orders at RMI warehouse, open, sent to WH, no shipment yet)]

    RUN --> TR[transform]
    TR --> T1[match AcuDB RC orders to CentralStore RMA records<br/>on ReturnNbr = RMANumber stripped of -1/-2/-3 suffix]

    RUN --> LD[load]
    LD --> LS[for each matched order]
    LS --> L1[(AcuAPI: GET SalesOrder — check for existing receipt/shipment)]
    L1 --> L2{receipt exists?}

    L2 -->|yes| L3[(AcuAPI: GET Shipment — get receipt details)]
    L3 --> L4[for each line: ensure ReasonCode = RETURN]
    L4 --> L5[(AcuAPI: PUT Shipment — update_reason_code if needed)]
    L5 --> L6{package_count = 0<br/>or != line_count?}
    L6 -->|yes| L7[(AcuAPI: PUT Shipment — add_package)]
    L6 -->|no| L8[(AcuAPI: PUT Shipment — get_package_details)]

    L2 -->|no| L9[(AcuAPI: POST SalesOrder — order_create_receipt)]
    L9 --> L10[sleep 5s]
    L10 --> L11[(AcuAPI: GET SalesOrder — get newly created shipment)]
    L11 --> L12[(AcuAPI: GET Shipment — get receipt details)]
    L12 --> L13[for each line: ensure ReasonCode = RETURN]
    L13 --> L14[(AcuAPI: PUT Shipment — update_reason_code if needed)]
    L14 --> L15[(AcuAPI: PUT Shipment — add_package)]

    L7 --> CONF{ready to confirm?<br/>status=Open, pkg count=line count,<br/>items and qtys match}
    L8 --> CONF
    L15 --> CONF
    CONF -->|yes| L16[(AcuAPI: POST Shipment/ConfirmShipment)]
    CONF -->|no| L17[skip — log mismatch]

    RUN --> LR[log_results]
    LR --> LO[acu_api._logout]
    LR --> UPS[(CentralStore: upsert _util.acu_api_log)]
```
