```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    A([run_send_shipments_to_RMI]) --> B[SendRMIShipments.__init__]
    B --> B1[init Transform]
    B --> B2[init AcuOData]
    B --> B3[init RMIXML]
    B --> B4[init AcumaticaAPI]
    A --> RUN[Pipeline.run]

    RUN --> EX[extract]
    EX --> D1[(AcuDB: SOShipment + SOShipLine + SOAddress<br/>via SendRMIShipments.sql<br/>SiteCD=RMI, OrigOrderType != RC,<br/>Status not in C/L/F/I, AttributeSHP2WH=0)]

    RUN --> TR[transform]
    TR --> T1[group rows by ShipmentNbr<br/>ShipmentNbr → list of line dicts]

    RUN --> LD[load]
    LD --> LD1[initiate_send: per shipment]
    LD1 --> LD2{ShipmentType?}
    LD2 -->|Type W — all RMI shipments| LD3[post_W: format SOAP XML payload<br/>ShipmentNbr, lines, shipping address, ShipMethod]
    LD2 -->|Type 3| LD4[post_3]
    LD3 --> RS1[(RMIXML: POST CreateNew<br/>SOAP to RMI API)]
    RS1 --> LD5{RMI response<br/>has error?}
    LD5 -->|no error or<br/>already exists| LD6[(AcuAPI: PUT Shipment<br/>AttributeSHP2WH = True)]
    LD5 -->|error| SKIP[skip Acumatica update<br/>log RMI error]

    RUN --> LR[log_results]
    LR --> LR1[(CentralStore: insert _util.rmi_send_log<br/>Type=Shipment, KeyValue, Lines,<br/>RMI_Response, RMI_Payload,<br/>ACU_Response, Timestamp)]
    LR --> LO[AcumaticaAPI._logout]
    LR --> UPS[(CentralStore: upsert _util.acu_api_log)]
```
