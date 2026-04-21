```mermaid
%%{init: {"flowchart": {"wrappingWidth": 400}}}%%
flowchart TD
    TRIGGER([rmi_send_shipment_return_pipeline<br/>timer: :10 and :40 every hour])

    TRIGGER --> SRS[SendRMIShipments.run<br/>Open RMI shipments — Type W]
    TRIGGER --> SRR[SendRMIReturns.run<br/>Open RC return orders — Type 3]

    SRS --> SRS_LOG[(CentralStore: _util.rmi_send_log<br/>Type=Shipment)]
    SRR --> SRR_LOG[(CentralStore: _util.rmi_send_log<br/>Type=Return)]

    SRS --> SRS_ACU[(AcuAPI: PUT Shipment<br/>AttributeSHP2WH = True)]
    SRR --> SRR_ACU[(AcuAPI: PUT SalesOrder<br/>AttributeRCSHP2WH = True)]
```

---

**Pipeline docs:**
- `SendRMIShipments` — see [pipelines/rmi_send_shipments.py](../../pipelines/rmi_send_shipments.py)
- `SendRMIReturns` — see [docs/pipelines/rmi_send_returns.md](../pipelines/rmi_send_returns.md)
