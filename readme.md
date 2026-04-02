# **Logistics Integration Platform**

## Table of Contents

- [Installation](#installation)
  - [Prerequisites](#prerequisites)
  - [Setup](#setup)
- [Configuration](#configuration)
- [Usage](#usage)
  - [Azure Functions](#azure-functions)
  - [Local](#local)
- [Project Structure](#project-structure)
- [Pipelines](#pipelines)
  - [SendRMIShipments](#pipeline---rmi_send_shipmentspy)
  - [SendRMIReturns](#pipeline---rmi_send_returnspy)
  - [GetClosedShipmentsFromRMI](#pipeline---get_closed_shipments_from_rmipy)
  - [GetReceiptsFromRMI](#pipeline---get_receipts_from_rmipy)
  - [StageRMIStatusRetrieval](#pipeline---stage_rmi_status_retrievalpy)
  - [GetStatusFromRMI](#pipeline---get_status_from_rmipy)
  - [CreateAcuReceipt](#pipeline---create_acu_receiptpy)
  - [PackShipments](#pipeline---pack_shipmentspy)
  - [ShipmentsReadyToConfirm](#pipeline---confirm_open_shipmentspy)
  - [RedStagInventory](#pipeline---redstag_inventorypy)
  - [SendRedStagShipments](#pipeline---redstag_send_shipmentspy)
  - [AuditFulfillment](#pipeline---audit_fulfillmentpy)

---

# Installation

## Prerequisites

- **Python 3.13**
- **pip**
- Network access and credentials for the following systems:
  - **CentralStore** — Azure SQL data warehouse (`jhl-dbcentral.database.windows.net`)
  - **AcumaticaDb** — VM-hosted SQL Server (`10.101.20.5`)
  - **Acumatica ERP API** (`erp.journeyhl.com`)
  - **RMI/BackTrackSRL** — REST API (`api.backtracksrl.com`) and SOAP service (`jhl.returnsmanagement.com`)
  - **RedStag** — JSON-RPC API (`wms.redstagfulfillment.com`)
- A `.env` file with valid credentials (see [Configuration](#configuration))
- **For Azure deployment only:** an Azure Function App named `logistics-integration-platform` and an `AZURE_CREDENTIALS` secret configured in the GitHub repository

## Setup

1. Clone the repository:

   ```bash
   git clone <repository-url>
   cd logistics-integration-platform
   ```

2. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

3. Place the `.env` file at the root of the project (see [Configuration](#configuration)).

---

# Configuration

1. Download the `.env` file from Journey's repo and place it at the root level of the project. This file is not committed to source control and must be obtained separately.

2. [`config/settings.py`](config/settings.py) loads all credentials from `.env` via `python-dotenv` and exposes them as module-level dictionaries imported throughout the connectors:

   | Dictionary | Keys | Used By |
   |---|---|---|
   | `DATABASES['db_CentralStore']` | `server`, `database`, `username`, `password` | [`connectors/sql.py`](connectors/sql.py) |
   | `DATABASES['AcumaticaDb']` | `server`, `database`, `username`, `password` | [`connectors/sql.py`](connectors/sql.py) |
   | `RMI` | `username`, `password` | [`connectors/rmi_api.py`](connectors/rmi_api.py), [`connectors/rmi_xml.py`](connectors/rmi_xml.py) |
   | `REDSTAG` | `username`, `password` | [`connectors/redstag_api.py`](connectors/redstag_api.py) |
   | `ACUMATICA_API` | `username`, `password` | [`connectors/acu_api.py`](connectors/acu_api.py) |



3. For **local development**, [`local.settings.json`](local.settings.json) is read by the Azure Functions Core Tools runtime and sets `FUNCTIONS_WORKER_RUNTIME` to `python`. Credentials are not stored here — they come exclusively from `.env`.

4. For **Azure deployment**, all environment variables listed above must be set as Application Settings on the `logistics-integration-platform` Function App in Azure. The GitHub Actions workflow ([`.github/workflows/deploy.yml`](.github/workflows/deploy.yml)) deploys only the code package — it does not inject secrets at runtime.

5. The `TABLES` dictionary in [`config/settings.py`](config/settings.py) defines the upsert schema (primary keys, column lists, and update columns) for each CentralStore table that pipelines write to: `rmi_Receipts`, `rmi_ClosedShipments`, `rmi_RMAStatus`, `_util.acu_api_log`, `RedstagInventorySummary`, and `RedstagInventoryDetail`.

---

# Usage

## Azure Functions

The application runs as six timer-triggered Azure Functions defined in [`function_app.py`](function_app.py). All cron schedules are evaluated in UTC. Active hours are 4am–11pm daily (some functions have a narrower window).

| Function | Pipeline(s) Invoked | Schedule (cron) | Runs At |
|---|---|---|---|
| `rmi_send_shipment_return_pipeline` | `SendRMIShipments`, `SendRMIReturns` | `10/30 4-23/1 * * *` | :10 and :40 every hour, 4am–11pm |
| `rmi_data_retrieval_pipeline` | `GetClosedShipmentsFromRMI`, `GetReceiptsFromRMI`, `StageRMIStatusRetrieval`, `GetStatusFromRMI` | `25 4-23/1 * * *` | :25 every hour, 4am–11pm |
| `create_acu_receipts` | `CreateAcuReceipt` | `50 8-20 * * *` | :50 every hour, 8am–8pm |
| `confirm_acu_shipments` | `ShipmentsReadyToConfirm` | `*/20 4-23 * * *` | Every 20 min, 4am–11pm |
| `pack_shipments` | `PackShipments` | `*/15 4-23 * * *` | Every 15 min, 4am–11pm |
| `redstag_inventory_retrieval` | `RedStagInventory` | `10 4-23/2 * * *` | :10 every 2 hours, 4am–11pm |

Deployment is handled automatically by the GitHub Actions workflow in [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml). Every push to the repository triggers a build and deploy:

1. Checks out the code on an `ubuntu-latest` runner
2. Sets up Python 3.13
3. Installs `requirements.txt` into `.python_packages/lib/site-packages` (the Azure Functions remote-build convention)
4. Authenticates to Azure using the `AZURE_CREDENTIALS` repository secret
5. Deploys the package to the `logistics-integration-platform` Function App

## Local

Each script in [`scripts/`](scripts/) targets one or more pipelines directly. Run from the project root after completing setup and configuration.

```bash
# Send open RMI shipments and return orders to the RMI warehouse
python scripts/run_send_to_RMI.py

# Pull closed shipments, receipts, and RMA statuses from RMI into CentralStore
python scripts/run_get_from_RMI.py

# Match RedStag tracking events to open Acumatica shipments and add package info
python scripts/run_pack_shipments.py

# Confirm packed RedStag shipments in Acumatica
python scripts/run_confirm_open_shipments.py

# Sync RedStag inventory and send outbound shipments to RedStag
python scripts/run_redstag.py

# Run the fulfillment audit pipeline (WIP)
python scripts/run_audit_fulfillments.py
```

Each pipeline follows the ETL pattern defined in [`pipelines/base.py`](pipelines/base.py): `extract()` → `transform()` → `load()` → `log_results()`. Structured log output goes to stdout via `colorlog` with millisecond timestamps (America/New_York timezone).

---

# Project Structure

```
logistics-integration-platform/
│
├── .github/
│   └── workflows/
│       └── deploy.yml                    # CI/CD: build and deploy to Azure Functions on every push
│
├── .vscode/                              # VS Code workspace settings and launch configurations
│
├── .degraded/                            # Archived/deprecated code; not active in any pipeline
│
├── config/
│   └── settings.py                       # Loads .env; exports DATABASES, RMI, REDSTAG, ACUMATICA_API, TABLES
│
├── connectors/
│   ├── __init__.py
│   ├── sql.py                            # SQLConnector: SQLAlchemy engine, dynamic query loading, upsert logic
│   ├── acu_api.py                        # AcumaticaAPI: cookie-authenticated REST client (erp.journeyhl.com)
│   │                                     #   Operations: shipments, packages, receipts, reason codes
│   ├── acu_odata.py                      # AcuOData: OData protocol client for Acumatica
│   ├── rmi_api.py                        # RMIAPI: token-based REST client (api.backtracksrl.com)
│   │                                     #   Endpoints: ClosedShipmentsV1, Receipts, RMA status
│   ├── rmi_xml.py                        # RMIXML: SOAP/XML client (jhl.returnsmanagement.com)
│   │                                     #   Sends shipment and return order payloads to RMI
│   ├── redstag_api.py                    # RedStagAPI: JSON-RPC client (wms.redstagfulfillment.com)
│   │                                     #   Operations: inventory, orders, shipments
│   └── transunion.py                     # TransUnion connector (present; not used in active pipelines)
│
├── dags/                                 # Apache Airflow DAGs (alternate orchestration; not the primary runtime)
│   ├── create_acu_receipts.py
│   ├── get_from_rmi.py
│   └── send_to_rmi.py
│
├── load/                                 # Standalone data loading utilities
│   ├── load_redstag_send.py
│   └── shipment_api.py
│
├── pipelines/
│   ├── __init__.py                       # Exports all pipeline classes
│   ├── base.py                           # Abstract Pipeline base class: extract/transform/load/log_results
│   │                                     #   Initializes SQLConnector instances; configures colorlog
│   ├── rmi_send_shipments.py             # SendRMIShipments: open RMI-bound shipments → RMI SOAP API
│   ├── rmi_send_returns.py               # SendRMIReturns: open RC orders → RMI SOAP API
│   ├── get_closed_shipments_from_RMI.py  # GetClosedShipmentsFromRMI: ClosedShipmentsV1 → rmi_ClosedShipments
│   ├── get_receipts_from_RMI.py          # GetReceiptsFromRMI: Receipts endpoint → rmi_Receipts
│   ├── stage_rmi_status_retrieval.py     # StageRMIStatusRetrieval: determines which RMAs need status checks
│   ├── get_status_from_RMI.py            # GetStatusFromRMI: RMI RMA endpoint (per order) → rmi_RMAStatus
│   ├── create_acu_receipt.py             # CreateAcuReceipt: matches RMI closed orders to Acumatica RC orders,
│   │                                     #   creates/verifies receipts, sets reason codes, confirms shipments
│   ├── pack_shipments.py                 # PackShipments: matches RedStag tracking to open Acumatica shipments,
│   │                                     #   adds packages and tracking numbers via Acumatica API
│   ├── confirm_open_shipments.py         # ShipmentsReadyToConfirm: confirms packed shipments in Acumatica
│   ├── redstag_inventory.py              # RedStagInventory: RedStag → RedstagInventorySummary/Detail
│   ├── redstag_send_shipments.py         # SendRedStagShipments: open RedStag-bound shipments → RedStag API
│   └── audit_fulfillment.py              # AuditFulfillment: fulfillment audit pipeline (WIP)
│
├── scripts/                              # Manual execution entry points; run from project root
│   ├── run.py
│   ├── run_send_to_RMI.py                # Runs: SendRMIShipments, SendRMIReturns
│   ├── run_get_from_RMI.py               # Runs: GetClosedShipmentsFromRMI, GetReceiptsFromRMI, GetStatusFromRMI
│   ├── run_pack_shipments.py             # Runs: PackShipments
│   ├── run_confirm_open_shipments.py     # Runs: ShipmentsReadyToConfirm
│   ├── run_redstag.py                    # Runs: RedStagInventory, SendRedStagShipments
│   ├── run_audit_fulfillments.py         # Runs: AuditFulfillment
│   └── query_acu_api.py                  # Ad-hoc Acumatica API query utility
│
├── sql/
│   ├── queries/
│   │   ├── AcumaticaDb/                  # SQL files: SendRMIShipments, SendRMIReturns, SendRedStagShipments,
│   │   │                                 #   OpenRCsNoReceipt, ShipmentsReadyToConfirm, PackShipment
│   │   └── db_CentralStore/              # SQL files: ReturnsPendingReciept, StatusCheckRMI, PackShipment,
│   │                                     #   RedStagEvents, AuditFulfillment
│   └── tables/                           # DDL for 6 CentralStore tables:
│                                         #   rmi_ClosedShipments, rmi_Receipts, rmi_RMAStatus,
│                                         #   _.acu_api_log, RedstagInventorySummary, RedstagInventoryDetail
│
├── transform/                            # Transformation logic imported by pipeline classes
│   ├── rmi_send.py
│   ├── rmi_receipt_pull.py
│   ├── create_acu_receipt.py
│   ├── pack_shipment.py
│   ├── redstag_inventory.py
│   ├── redstag_send.py
│   └── audit_fulfillment.py
│
├── function_app.py                       # Azure Functions entry point; all 6 timer-triggered functions
├── host.json                             # Azure Functions host config
├── local.settings.json                   # Local dev runtime settings (FUNCTIONS_WORKER_RUNTIME: python)
├── requirements.txt                      # Python dependencies
└── readme.md
```

**Python dependencies** (`requirements.txt`):

| Package | Version | Purpose |
|---|---|---|
| `colorlog` | 6.10.1 | Colored, structured console logging |
| `pandas` | 3.0.1 | Data manipulation |
| `polars` | 1.38.0 | Primary DataFrame library for extract/transform steps |
| `pyarrow` | >=8.0.0 | Polars database I/O backend |
| `pymssql` | 2.3.4 | SQL Server driver (used via SQLAlchemy) |
| `python-dotenv` | 1.2.2 | `.env` file loading |
| `Requests` | 2.32.5 | HTTP client for all external API calls |
| `SQLAlchemy` | 2.0.48 | Database engine and connection management |
| `xmltodict` | 1.0.4 | SOAP XML parsing for RMI responses |

---

# Pipelines 

## Pipeline - [`rmi_send_shipments.py`](pipelines/rmi_send_shipments.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`rmi_send_returns.py`](pipelines/rmi_send_returns.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`get_closed_shipments_from_RMI.py`](pipelines/get_closed_shipments_from_RMI.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`get_receipts_from_RMI.py`](pipelines/get_receipts_from_RMI.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`stage_rmi_status_retrieval.py`](pipelines/stage_rmi_status_retrieval.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`get_status_from_RMI.py`](pipelines/get_status_from_RMI.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`create_acu_receipt.py`](pipelines/create_acu_receipt.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`pack_shipments.py`](pipelines/pack_shipments.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`confirm_open_shipments.py`](pipelines/confirm_open_shipments.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`redstag_inventory.py`](pipelines/redstag_inventory.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`redstag_send_shipments.py`](pipelines/redstag_send_shipments.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---

## Pipeline - [`audit_fulfillment.py`](pipelines/audit_fulfillment.py)

### Execution Behavior

### Functions

#### ```def __init__```
   - bullets


---
---
---
