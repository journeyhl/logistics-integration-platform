# **Integration Platform**

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
  - [GetRMAsFromRMI](#pipeline---get_rmas_from_rmipy)
  - [CreateAcuReceipt](#pipeline---create_acu_receiptpy)
  - [PackShipments](#pipeline---pack_shipmentspy)
  - [ShipmentsReadyToConfirm](#pipeline---confirm_open_shipmentspy)
  - [RedStagInventory](#pipeline---redstag_inventorypy)
  - [SendRedStagShipments](#pipeline---redstag_send_shipmentspy)
  - [AcumaticaDeletions](#pipeline---acu_deletionspy)
  - [AddressValidator](#pipeline---address_validatorpy)
  - [Criteo](#pipeline---criteopy)
  - [AcuToDbcQuotes](#pipeline---acu_to_dbc_quotespy)
  - [AcuToDbcSalesOrders](#pipeline---acu_to_dbc_sales_orderspy)
  - [AcuToDbcPhoneRevenue](#pipeline---acu_to_dbc_phone_revenuepy)
  - [SalesOrderCleaner](#pipeline---sales_order_cleanerpy)
  - [SendOrderDetailsToKustomer](#pipeline---kustomerpy)
  - [HubSpotSnapshot](#pipeline---hubspot_snapshotpy)
  - [HubSpotProperties](#pipeline---hubspot_propertiespy)
  - [SendToAfterShip](#pipeline---aftership_sendpy)
  - [UpdateAfterShip](#pipeline---aftership_updatepy)
  - [AfterShipToDbc](#pipeline---aftership_to_dbcpy)
  - [RMILinkToAcu](#pipeline---rmi_link_to_acupy)
  - [Dev / Scaffolded Pipelines](#dev--scaffolded-pipelines)
- [Connectors](#connectors)
  - [SQLConnector](#sqlconnector)
  - [AcumaticaAPI](#acumaticaapi)
  - [AcuOData](#acuodata)
  - [RMIAPI](#rmiapi)
  - [RMIXML](#rmixml)
  - [RedStagAPI](#redstagapi)
  - [CriteoAPI](#criteoapi)
  - [AddressVerificationSystem (AVS)](#addressverificationsystem-avs)
  - [HubSpotAPI](#hubspotapi)
  - [AfterShip](#aftership)
  - [Kustomer](#kustomer)
  - [ShopifyAPI](#shopifyapi)
  - [Teams](#teams)
  - [Sharepoint](#sharepoint)
  - [TransunionAPI](#transunionapi)
- [Transforms](#transforms)
- [Load Helpers](#load-helpers)
- [Pipeline Base Class](#pipeline-base-class)
- [Documentation](#documentation)

---

# Installation

## Prerequisites

- **Python 3.13**
- **pip**
- Network access and credentials provided internally

## Setup

### Option A: Install as a package

```bash
py -3.13 -m pip install git+https://github.com/journeyhl/integration-platform.git
```

Then import directly in any Python project:

```python
from connectors import sql
from pipelines import kustomer
```

### Option B: Clone and run locally

1. Clone the repository:

   ```bash
   git clone https://github.com/journeyhl/logistics-integration-platform.git
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


3. For **Azure deployment**, all environment variables listed above must be set as Application Settings on the `logistics-integration-platform` Function App in Azure. The GitHub Actions workflow ([`.github/workflows/deploy.yml`](.github/workflows/deploy.yml)) deploys only the code package: it does not inject secrets at runtime.

4. The `TABLES` dictionary in [`config/settings.py`](config/settings.py) defines the upsert schema (primary keys, column lists, and update columns) consumed by [`SQLConnector.checked_upsert`](connectors/sql.py). It currently covers the following tables:

   **RMI / 3PL inbound:**
   - `rmi_Receipts`, `rmi_ClosedShipments`, `rmi_RMAStatus`
   - `RedstagInventorySummary`, `RedstagInventoryDetail`

   **Acumatica mirror in CentralStore (`acu` schema):**
   - `acu.SalesOrders`, `acu.Quotes`, `acu.Shipments`
   - `acu.AftershipExportv2`, `acu.AftershipExportDetailv2`

   **Acumatica deletion-tracking (`_util` schema):**
   - `_util.SOOrderDeletions`, `_util.SOLineDeletions`, `_util.SOShipmentDeletions`, `_util.SOOrderShipmentDeletions`
   - `_util.acu_api_log`, `_util.AfterShipLog`

   **Acumatica writeback (AcumaticaDb):**
   - `SOShipmentKvExt`, written by [`RMILinkToAcu`](#pipeline---rmi_link_to_acupy)

   **Ad / marketing data:**
   - `AdDetails`
   - `criteo.campaign_performance_daily`, `criteo.diff_log`

   **HubSpot snapshots:**
   - `hs.deal_tracking`, `hs.deal_snapshots`, `hs.activity_snapshots`, `hs.Properties`

   **Kustomer ingest:**
   - `K_OrderIngest`

   ### Structure
   ```python
   'table_name': {                            # name of the target table
       'keys':            ['pk_col_1', ...],  # primary key columns
       'columns':         [...],              # full column list
       'update_columns':  [...]               # columns to update on key match
   }
   ```

---

# Usage

## Azure Functions

The application runs as twenty timer-triggered Azure Functions defined in [`function_app.py`](function_app.py). All cron schedules are evaluated in UTC.

| Function | Pipeline(s) Invoked | Schedule (cron) | Runs At |
|---|---|---|---|
| `rmi_send_shipment_return_pipeline` | [`SendRMIShipments`](#pipeline---rmi_send_shipmentspy), [`SendRMIReturns`](#pipeline---rmi_send_returnspy) | `20,10/30 * * * *` | :10, :20, :40 every hour |
| `rmi_data_retrieval_pipeline` | [`GetClosedShipmentsFromRMI`](#pipeline---get_closed_shipments_from_rmipy), [`GetReceiptsFromRMI`](#pipeline---get_receipts_from_rmipy), [`GetRMAsFromRMI`](#pipeline---get_rmas_from_rmipy) | `25 * * * *` | :25 every hour |
| `create_acu_receipts` | [`CreateAcuReceipt`](#pipeline---create_acu_receiptpy) | `50 * * * *` | :50 every hour |
| `confirm_acu_shipments` | [`ShipmentsReadyToConfirm`](#pipeline---confirm_open_shipmentspy) | `*/20 * * * *` | Every 20 minutes (:00, :20, :40) |
| `pack_shipments` | [`PackShipments`](#pipeline---pack_shipmentspy) | `*/15 * * * *` | Every 15 minutes (:00, :15, :30, :45) |
| `redstag_send_shipment_pipeline` | [`SendRedStagShipments`](#pipeline---redstag_send_shipmentspy) | `15,5/30 * * * *` | :05, :15, :35 every hour |
| `redstag_inventory_retrieval` | [`RedStagInventory`](#pipeline---redstag_inventorypy) | `10 */2 * * *` | :10 every 2 hours |
| `acu_deletions` | [`AcumaticaDeletions`](#pipeline---acu_deletionspy) | `40 * * * *` | :40 every hour |
| `address_validator` | [`AddressValidator`](#pipeline---address_validatorpy) | `55 * * * *` | :55 every hour |
| `criteo_ads` | [`Criteo`](#pipeline---criteopy) | `1 * * * *` | :01 every hour |
| `acu_to_dbc_sales_orders` | [`AcuToDbcSalesOrders`](#pipeline---acu_to_dbc_sales_orderspy) | `*/10 * * * *` | Every 10 minutes |
| `acu_to_dbc_quotes` | [`AcuToDbcQuotes`](#pipeline---acu_to_dbc_quotespy) | `*/30 * * * *` | :00, :30 every hour |
| `acu_to_dbc_phone_revenue` | [`AcuToDbcPhoneRevenue`](#pipeline---acu_to_dbc_phone_revenuepy) | `40 1/8 * * *` | 1:40, 9:40, 17:40 (UTC) |
| `kustomer_order_ingest` | [`SendOrderDetailsToKustomer`](#pipeline---kustomerpy) (ingest mode) | `*/12 * * * *` | :00, :12, :24, :36, :48 every hour |
| `kustomer_order_backfill` | [`SendOrderDetailsToKustomer`](#pipeline---kustomerpy) (backfill mode) | `43 * * * *` | :43 every hour |
| `aftership_send` | [`SendToAfterShip`](#pipeline---aftership_sendpy) | `2/15 * * * *` | :02, :17, :32, :47 every hour |
| `aftership_update` | [`UpdateAfterShip`](#pipeline---aftership_updatepy) | `7/20 * * * *` | :07, :27, :47 every hour |
| `aftership_to_dbc` | [`AfterShipToDbc`](#pipeline---aftership_to_dbcpy) | `38 7/12 * * *` | 7:38 AM and 7:38 PM (UTC) |
| `hubspot_snapshots` | [`HubSpotSnapshot`](#pipeline---hubspot_snapshotpy) | `0 23 * * *` | 11:00 PM (UTC) daily |
| `rmi_link_to_acumatica` | [`RMILinkToAcu`](#pipeline---rmi_link_to_acupy) | `30 4/6 * * *` | 4:30, 10:30 AM/PM (UTC) |
| `sales_order_cleaner` | [`SalesOrderCleaner`](#pipeline---sales_order_cleanerpy) | `5 1 * * *` | 1:05 AM (UTC) daily |

A visual calendar of these cron schedules is generated by [`scripts/cron_calendar.py`](scripts/cron_calendar.py).

Deployment is handled automatically by the GitHub Actions workflow in [`.github/workflows/deploy.yml`](.github/workflows/deploy.yml). Every push to the repository triggers a build and deploy:

1. Checks out the code on an `ubuntu-latest` runner
2. Sets up Python 3.13
3. Installs `requirements.txt` into `.python_packages/lib/site-packages` (the Azure Functions remote-build convention)
4. Authenticates to Azure using the `AZURE_CREDENTIALS` repository secret
5. Deploys the package to the `logistics-integration-platform` Function App (environment `prod`)

## Local

Each script in [`scripts/`](scripts/) targets one or more pipelines directly. Run from the project root after completing setup and configuration.

```bash
# RMI - Send shipments / returns to the RMI warehouse
python scripts/run_send_shipments_to_RMI.py
python scripts/run_send_returns_to_RMI.py

# RMI - Pull closed shipments, receipts, and RMA statuses into CentralStore
python scripts/run_get_closed_shipments_from_rmi.py
python scripts/run_get_receipts_from_rmi.py
python scripts/run_get_rmas_from_rmi.py

# Match RedStag/RMI tracking events to open Acumatica shipments and add packages
python scripts/run_pack_shipments.py

# Confirm packed shipments in Acumatica
python scripts/run_confirm_open_shipments.py

# RedStag - Send outbound shipments
python scripts/run_redstag.py

# RedStag - Sync inventory
python scripts/run_redstag_inventory.py

# Create receipts in Acumatica for Closed RMA returns
python scripts/run_create_acu_receipt.py

# Mirror Acumatica row deletions into _util tables in CentralStore
python scripts/run_acu_deletions.py

# Validate WB on-hold order addresses with Avalara AVS
python scripts/run_address_validator.py

# Pull Criteo ad data and load to criteo.campaign_performance_daily
python scripts/run_criteo.py

# Sync Acumatica Sales Orders to acu.SalesOrders in CentralStore
python scripts/run_acu_to_dbc_sales_orders.py

# Sync Acumatica Quotes to acu.Quotes in CentralStore
python scripts/run_acu_to_dbc_quotes.py

# Sync Acumatica PH/BF order phone revenue to acu.PhoneRevByMonth in CentralStore
python scripts/run_acu_to_dbc_phone_revenue.py

# Remove out-of-sync rows from acu.SalesOrders in CentralStore
python scripts/run_sales_order_cleaner.py

# Send order details to Kustomer (ingest or backfill)
python scripts/run_kustomer.py

# AfterShip - send new trackings, update existing, sync to CentralStore
python scripts/run_aftership_send.py
python scripts/run_aftership_update.py
python scripts/run_aftership_to_dbc.py

# HubSpot - pull property metadata to hs.Properties
python scripts/run_hubspot_properties.py

# Write RMI Link3PL + RMAID values back to Acumatica SOShipmentKvExt
python scripts/run_rmi_link_to_acu.py

# Generate a visual calendar of all timer-trigger cron schedules
python scripts/cron_calendar.py
```

Each pipeline follows the ETL pattern defined in [`pipelines/base.py`](pipelines/base.py): `extract()` → `transform()` → `load()` → `log_results()`. Structured log output goes to stdout via `colorlog` with millisecond timestamps (America/New_York timezone), and the in-memory log buffer is persisted to **`_util.Logs`** at the end of every run.

---

# Project Structure

```
logistics-integration-platform/
│
├── .github/
│   └── workflows/
│       └── deploy.yml                    # CI/CD: build and deploy to Azure Functions on every push
│
├── .vscode/                              # VS Code workspace settings, launch configs, docstring template
│
├── config/
│   └── settings.py                       # Loads .env; exports DATABASES, RMI, REDSTAG, ACUMATICA_API,
│                                         #   AVS, CRITEO, KUSTOMER, HUBSPOT, SHOPIFY, TEAMS, AFTERSHIP,
│                                         #   SHAREPOINT, TABLES
│
├── connectors/
│   ├── __init__.py
│   ├── sql.py                            # SQLConnector: SQLAlchemy engine, dynamic query loading,
│   │                                     #   upsert logic. Defines CentralStoreQueries, AcumaticaDbQueries
│   ├── acu_api.py                        # AcumaticaAPI: cookie-authenticated REST client (erp.journeyhl.com)
│   │                                     #   Operations: shipments, packages, receipts, reason codes,
│   │                                     #   send-to-warehouse, address validation
│   ├── acu_odata.py                      # AcuOData: OData protocol client for Acumatica
│   ├── rmi_api.py                        # RMIAPI: token-based REST client (api.backtracksrl.com)
│   │                                     #   Endpoints: ClosedShipmentsV1, Receipts, RMAs
│   ├── rmi_xml.py                        # RMIXML: SOAP/XML client (jhl.returnsmanagement.com)
│   │                                     #   Sends shipment (Type W) and return order (Type 3) payloads
│   ├── redstag_api.py                    # RedStagAPI: JSON-RPC client (wms.redstagfulfillment.com)
│   │                                     #   Operations: inventory, orders, shipments
│   ├── avs.py                            # AddressVerificationSystem: Avalara AVS REST client
│   ├── criteo_api.py                     # CriteoAPI: OAuth2-authenticated stats client (api.criteo.com)
│   ├── hubspot_api.py                    # HubSpotAPI: HubSpot CRM client (deals, activities, properties)
│   ├── aftership.py                      # AfterShip: tracking API (api.aftership.com/tracking/2026-01)
│   ├── kustomer.py                       # Kustomer: webhook-based customer-data push
│   ├── shopify.py                        # ShopifyAPI: GraphQL/ShopifyQL client (sales reporting)
│   ├── teams.py                          # Teams: Microsoft Teams webhook poster
│   ├── sharepoint.py                     # Sharepoint: Microsoft Graph client (file retrieval)
│   └── transunion.py                     # TransUnion connector (present; not used in active pipelines)
│
├── docs/                                 # Mermaid flowchart documentation per pipeline + function
│   ├── pipeline_docstring_analysis.md
│   ├── functions/                        # Per-Azure-Function docs (schedule, pipelines, queries)
│   └── pipelines/                        # Per-pipeline mermaid diagrams
│
├── load/                                 # Reusable load helpers used by some pipelines
│   ├── address_validator.py
│   ├── kustomer.py                       # Load.send_payloads: POST each order to Kustomer + upsert log
│   ├── load_redstag_send.py              # Load.send_shipments → marks Acumatica ShipToWH after RedStag accepts
│   └── shipment_api.py                   # Load.load_shipments / load_receipts: shared package + receipt logic
│
├── pipelines/
│   ├── __init__.py                       # Exports all pipeline classes
│   ├── base.py                           # Abstract Pipeline base class: extract/transform/load/log_results.
│   │                                     #   Initializes SQLConnectors; configures colorlog;
│   │                                     #   appends every run's log buffer to _util.Logs
│   ├── rmi_send_shipments.py             # SendRMIShipments: open RMI-bound shipments → RMI SOAP API (Type W)
│   ├── rmi_send_returns.py               # SendRMIReturns: open RC orders → RMI SOAP API (Type 3)
│   ├── get_closed_shipments_from_RMI.py  # GetClosedShipmentsFromRMI: ClosedShipmentsV1 → rmi_ClosedShipments
│   ├── get_receipts_from_RMI.py          # GetReceiptsFromRMI: Receipts endpoint → rmi_Receipts
│   ├── get_rmas_from_RMI.py              # GetRMAsFromRMI: RMAs endpoint (last 120 days) → rmi_RMAStatus
│   ├── create_acu_receipt.py             # CreateAcuReceipt: matches RMI closed orders to Acumatica RC orders,
│   │                                     #   creates/verifies receipts, sets reason codes, confirms shipments
│   ├── pack_shipments.py                 # PackShipments: matches RedStag/RMI tracking to open Acumatica
│   │                                     #   shipments, adds packages and tracking via Acumatica API
│   ├── confirm_open_shipments.py         # ShipmentsReadyToConfirm: confirms packed shipments in Acumatica
│   ├── redstag_inventory.py              # RedStagInventory: RedStag → RedstagInventorySummary/Detail
│   ├── redstag_send_shipments.py         # SendRedStagShipments: open RedStag-bound shipments → RedStag API
│   ├── acu_deletions.py                  # AcumaticaDeletions: SOOrder/SOLine/SOShipment/SOOrderShipment
│   │                                     #   row-deletions captured by trigger → _util.* tables; then
│   │                                     #   reconciles acu.SalesOrders and acu.Shipments in CentralStore
│   ├── address_validator.py              # AddressValidator: WB on-hold orders → Avalara AVS → update +
│   │                                     #   validate + remove-hold + create shipment
│   ├── criteo.py                         # Criteo: incremental Criteo ads → criteo.campaign_performance_daily
│   │                                     #   and criteo.diff_log
│   ├── acu_to_dbc_quotes.py              # AcuToDbcQuotes: Acumatica Quotes → acu.Quotes in CentralStore
│   ├── acu_to_dbc_sales_orders.py        # AcuToDbcSalesOrders: Acumatica Sales Orders → acu.SalesOrders
│   ├── acu_to_dbc_phone_revenue.py       # AcuToDbcPhoneRevenue: PH/BF orders → acu.PhoneRevByMonth
│   ├── sales_order_cleaner.py            # SalesOrderCleaner: removes out-of-sync rows from acu.SalesOrders
│   ├── kustomer.py                       # SendOrderDetailsToKustomer: ingest/backfill order data to Kustomer
│   ├── hubspot_snapshot.py               # HubSpotSnapshot: deals + activity counts → hs.deal_snapshots,
│   │                                     #   hs.deal_tracking, hs.activity_snapshots
│   ├── hubspot_properties.py             # HubSpotProperties: HubSpot property metadata → hs.Properties
│   ├── aftership_send.py                 # SendToAfterShip: new shipments → AfterShip /trackings (POST)
│   ├── aftership_update.py               # UpdateAfterShip: diff & PATCH existing AfterShip trackings
│   ├── aftership_to_dbc.py               # AfterShipToDbc: AfterShip trackings + checkpoints → CentralStore
│   ├── rmi_link_to_acu.py                # RMILinkToAcu: writes Link3PL URL + RMAID back to Acumatica
│   │                                     #   SOShipmentKvExt
│   └── dev/                              # Scaffolded / in-progress pipelines
│       ├── audit_fulfillment.py          #   AuditFulfillment (WIP)
│       ├── shopify.py                    #   ShopifyGraphQL (WIP)
│       └── notify_fulfillment_ops.py     #   NotifyFulfillmentOps (WIP)
│
├── scripts/                              # Manual execution entry points; run from project root
│   ├── run_send_shipments_to_RMI.py      # Runs: SendRMIShipments
│   ├── run_send_returns_to_RMI.py        # Runs: SendRMIReturns
│   ├── run_get_closed_shipments_from_rmi.py
│   ├── run_get_receipts_from_rmi.py
│   ├── run_get_rmas_from_rmi.py
│   ├── run_pack_shipments.py             # Runs: PackShipments
│   ├── run_confirm_open_shipments.py     # Runs: ShipmentsReadyToConfirm
│   ├── run_create_acu_receipt.py         # Runs: CreateAcuReceipt
│   ├── run_redstag.py                    # Runs: SendRedStagShipments
│   ├── run_redstag_inventory.py          # Runs: RedStagInventory
│   ├── run_acu_deletions.py              # Runs: AcumaticaDeletions
│   ├── run_address_validator.py          # Runs: AddressValidator
│   ├── run_criteo.py                     # Runs: Criteo
│   ├── run_acu_to_dbc_quotes.py          # Runs: AcuToDbcQuotes
│   ├── run_acu_to_dbc_sales_orders.py    # Runs: AcuToDbcSalesOrders
│   ├── run_acu_to_dbc_phone_revenue.py   # Runs: AcuToDbcPhoneRevenue
│   ├── run_sales_order_cleaner.py        # Runs: SalesOrderCleaner
│   ├── run_kustomer.py                   # Runs: SendOrderDetailsToKustomer (ingest or backfill)
│   ├── run_aftership_send.py             # Runs: SendToAfterShip
│   ├── run_aftership_update.py           # Runs: UpdateAfterShip
│   ├── run_aftership_to_dbc.py           # Runs: AfterShipToDbc
│   ├── run_hubspot_properties.py         # Runs: HubSpotProperties
│   ├── run_rmi_link_to_acu.py            # Runs: RMILinkToAcu
│   ├── run_notify_fulfillment_ops.py     # Runs: NotifyFulfillmentOps (dev)
│   ├── run_audit_fulfillments.py         # Runs: AuditFulfillment (dev)
│   ├── connector_executions.py           # Ad-hoc connector test runners
│   ├── cron_calendar.py                  # Renders a visual calendar of all Azure timer cron schedules
│   ├── db_table_config_generator.py      # Generates TABLES dict entries from a target table's columns
│   ├── delete_pycache_folders.py         # Recursively removes __pycache__ folders
│   ├── generate_azure_env_vars.py        # Bulk-formats .env content for Azure App Settings
│   ├── mark_sent_to_wh.py                # Manual SentToWH flag setter for stuck shipments
│   ├── query_acu_api.py                  # Ad-hoc Acumatica API query utility
│   └── shopify.py                        # Ad-hoc Shopify GraphQL runner
│
├── sql/
│   ├── queries/
│   │   ├── AcumaticaDb/                  # SendRMIShipments, SendRMIReturns, SendRedStagShipments,
│   │   │                                 #   OpenRCsNoReceipt, ShipmentsReadyToConfirm, PackShipment,
│   │   │                                 #   ValidateAddresses, SOOrderDeletions, SOLineDeletions,
│   │   │                                 #   SOShipmentDeletions, SOOrderShipmentDeletions,
│   │   │                                 #   AcuToDbc_Quotes, AcuToDbc_SalesOrders,
│   │   │                                 #   AcuToDbc_PhoneRevByMonth,
│   │   │                                 #   Kustomer_OrderIngest, Kustomer_OrderIngestBackfill,
│   │   │                                 #   Kustomer_ShipmentData, Kustomer_FilteredOutOrders,
│   │   │                                 #   Aftership_Shipments, RMI_Link3PL
│   │   └── db_CentralStore/              # ReturnsPendingReciept, StatusCheckRMI, PackShipmentRedStag,
│   │                                     #   PackShipmentRMI, RedStagEvents, AuditFulfillment,
│   │                                     #   NotifyFulfillmentOpsTeam, SalesOrderCleaner,
│   │                                     #   RMI_Link3PL_RMAStatus, aftership_shipments
│   ├── tables/                           # DDL for CentralStore tables (rmi_*, Redstag*, _util.*, criteo.*,
│   │                                     #   acu.*, hs.*, K_OrderIngest)
│   └── triggers/                         # AcumaticaDb triggers that capture row deletions for SOOrder,
│                                         #   SOLine, SOShipment, SOOrderShipment
│
├── transform/                            # Transformation logic imported by pipeline classes
│   ├── address_validator.py
│   ├── aftership.py                      # Used by SendToAfterShip / UpdateAfterShip / AfterShipToDbc
│   ├── audit_fulfillment.py
│   ├── cleaner.py
│   ├── create_acu_receipt.py
│   ├── criteo.py
│   ├── hubspot_snapshot.py
│   ├── kustomer.py
│   ├── notify_fulfillment_ops.py
│   ├── pack_shipment.py
│   ├── redstag_inventory.py
│   ├── redstag_send.py
│   ├── rmi_rmas.py                       # Used by GetRMAsFromRMI
│   ├── rmi_receipt_pull.py
│   └── rmi_send.py
│
├── function_app.py                       # Azure Functions entry point; all timer-triggered functions
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

Every pipeline subclasses [`Pipeline`](#pipeline-base-class) and is invoked with `.run()`. The base class drives the lifecycle: `extract()` → `transform()` → `load()` → `log_results()`.

---

## Pipeline - [`rmi_send_shipments.py`](pipelines/rmi_send_shipments.py)

### `SendRMIShipments`

Pipeline to send Open Sales-Order Shipments to RMI as **Type W** payloads. Targets shipments whose `AttributeRCSHP2WH` value is null or not equal to `1`.

### Execution Behavior

#### Extraction
- Pulls Shipments that are ready to be sent to RMI as Type Ws via the [`AcumaticaDbQueries.SendRMIShipments`](sql/queries/AcumaticaDb/SendRMIShipments.sql) query
  - `OrigOrderType != 'RC'`: Order associated with Shipment is **not** a Return
  - `Status not in ('C', 'L', 'F', 'I')`: i.e. **not** Completed, Cancelled, Confirmed, or Invoiced
  - `AttributeSHP2WH = 0`: Not yet sent to Warehouse
  - `SiteCD = 'RMI'`: Warehouse is RMI

#### Transformation
- Builds a dictionary keyed by `RMANumber`, where each value is a list of dicts representing each line on the shipment

#### Load
- Formats the W payload sent to RMI through [`RMIXML.post_W`](#rmixml). Per-line formatting happens in [`RMIXML._format_w_lines`](#rmixml).
- Posts each formatted payload to RMI via [`RMIXML.post_W`](#rmixml).

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)
- Inserts RMI XML interactions to **`_util.rmi_send_log`**

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded: list)`

---

## Pipeline - [`rmi_send_returns.py`](pipelines/rmi_send_returns.py)

### `SendRMIReturns`

Pipeline to send Open `RC` Sales Orders that have an `AttributeRCSHP2WH` value that is null or not equal to `1`. Sent to RMI as **Type 3** payloads.

### Execution Behavior

#### Extraction
- Pulls all `RC` Sales Orders in **Open** status whose `AttributeRCSHP2WH` is null or not `1` via the [`AcumaticaDbQueries.SendRMIReturns`](sql/queries/AcumaticaDb/SendRMIReturns.sql) query
  - `OrderType = 'RC'`
  - `Status = 'N'`
  - `SiteCD = 'RMI'`
  - `(AttributeRCSHP2WH != 1 OR AttributeRCSHP2WH IS NULL)`

#### Transformation
- Builds a dictionary keyed by `RMANumber`, where each value is a list of dicts representing each row (line) on the order

#### Load
- Formats the Type 3 payload sent to RMI through [`RMIXML.post_3`](#rmixml). Per-line formatting happens in [`RMIXML._format_3_lines`](#rmixml).
- Posts each formatted payload to RMI via [`RMIXML.post_3`](#rmixml).

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)
- Inserts RMI XML interactions to **`_util.rmi_send_log`**

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded: list)`

---

## Pipeline - [`get_closed_shipments_from_RMI.py`](pipelines/get_closed_shipments_from_RMI.py)

### `GetClosedShipmentsFromRMI`

Hits RMI's *ClosedShipmentsV1* endpoint, retrieves all Closed Shipments, and upserts to **`rmi_ClosedShipments`** in `db_CentralStore`.

### Execution Behavior

#### Extraction
- Extracts ClosedShipments data via [`RMIAPI.closed_shipments`](#rmiapi)

#### Transformation
- Transforms extracted data into the shape needed for upsert to **`rmi_ClosedShipments`** (driven by `transform/rmi_receipt_pull.py`'s `transform_closed_shipments`)

#### Load
- Upserts data to **`rmi_ClosedShipments`** via [`SQLConnector.checked_upsert`](#sqlconnector)

#### Results Logging
- None needed

### Functions
- `__init__(self)`: Initializes `GetClosedShipmentsFromRMI` Pipeline. Sets:
  ```python
  self.rmi = RMIAPI(self)
  self.transformer = Transform(self)
  ```
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`get_receipts_from_RMI.py`](pipelines/get_receipts_from_RMI.py)

### `GetReceiptsFromRMI`

Hits RMI's *Receipts* endpoint, retrieves all Receipts, and upserts to **`rmi_Receipts`** in `db_CentralStore`.

### Execution Behavior

#### Extraction
- Extracts Receipts via [`RMIAPI.get_receipts`](#rmiapi)

#### Transformation
- Transforms extracted data into the shape needed for upsert to **`rmi_Receipts`** (driven by `transform/rmi_receipt_pull.py`'s `transform_receipts`)

#### Load
- Upserts data to **`rmi_Receipts`** via [`SQLConnector.checked_upsert`](#sqlconnector)

#### Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`get_rmas_from_RMI.py`](pipelines/get_rmas_from_RMI.py)

### `GetRMAsFromRMI`

Pulls all RMAs modified in RMI within the last 120 days from RMI's `RMAs` endpoint and upserts to **`rmi_RMAStatus`** in `db_CentralStore`. This is the reference docstring format that the rest of the pipelines have been retrofitted toward (see [`docs/pipeline_docstring_analysis.md`](docs/pipeline_docstring_analysis.md)).

### Execution Behavior

#### Extraction
- Extracts RMA data via [`RMIAPI.target_api`](#rmiapi) against the `RMAs` endpoint with a 120-day lookback window

#### Transformation
- Flattens `rmaLines` per RMA into rows via [`Transform.transform_status_records`](transform/rmi_rmas.py)

#### Load
- Upserts data to **`rmi_RMAStatus`** via [`SQLConnector.checked_upsert`](#sqlconnector)

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`create_acu_receipt.py`](pipelines/create_acu_receipt.py)

### `CreateAcuReceipt`

Pipeline to create a Receipt for any `RC`-type Orders in Acumatica that: per RMI: have an `RMAStatus` of **CLOSED** and a `DFStatus` of **RECEIVED**.

### Execution Behavior

#### Extraction
- Queries `db_CentralStore` for any RMA orders with `RMAStatus = CLOSED` and `DFStatus = RECEIVED` (these orders should be Receipted in Acumatica if they aren't already): see [`CentralStoreQueries.ReturnsPendingReciept`](sql/queries/db_CentralStore/ReturnsPendingReciept.sql)
- Queries `AcumaticaDb` for any `RC` Orders that are pending Receipt creation: see [`AcumaticaDbQueries.OpenRCsNoReceipt`](sql/queries/AcumaticaDb/OpenRCsNoReceipt.sql)

#### Transformation
- Matches Orders across both datasets to find Acumatica Orders that are ready to be Receipted

#### Load
For **each** matched order:
- Check whether the Order has a Receipt (Shipment) via [`AcumaticaAPI.sales_order_get_shipment`](#acumaticaapi)
- **If a Receipt exists**:
  - Retrieve details via [`AcumaticaAPI.shipment_details`](#acumaticaapi)
  - Decide whether to add a package or just retrieve details
    - Add package via [`AcumaticaAPI.add_package`](#acumaticaapi)
    - Retrieve details via [`AcumaticaAPI.get_package_details`](#acumaticaapi)
- **If no Receipt exists**:
  - Create the Receipt via [`AcumaticaAPI.order_create_receipt`](#acumaticaapi)
  - Re-check for Shipment on Order via [`AcumaticaAPI.sales_order_get_shipment`](#acumaticaapi)
  - Retrieve details via [`AcumaticaAPI.shipment_details`](#acumaticaapi)
  - Add package via [`AcumaticaAPI.add_package`](#acumaticaapi)
- For each line: verify Reason Code is set to **RETURN**. If not, update via [`AcumaticaAPI.update_reason_code`](#acumaticaapi)
- Verify Shipment Details, Package Items, and Quantities match
- If all checks pass, confirm the Shipment via [`AcumaticaAPI.confirm_shipment`](#acumaticaapi)

The per-shipment orchestration above is driven by [`Load.load_receipts`](load/shipment_api.py) in [`load/shipment_api.py`](load/shipment_api.py).

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract: dict[str, pl.DataFrame])`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`pack_shipments.py`](pipelines/pack_shipments.py)

### `PackShipments`

Pipeline to pack any Acumatica Shipments that have tracking data from 3PLs (RedStag and RMI).

### Execution Behavior

#### Extraction
Four data sources are queried:
- **`central_extract`**: the original driver of the RedStag Confirmations Celigo flow. Pulls shipments from `CentralStore` using the `acu.rs_*` tables to determine which should be shipped.
  - Query: [`CentralStoreQueries.PackShipmentRedStag`](sql/queries/db_CentralStore/PackShipmentRedStag.sql)
- **`redstag_event_extract`**: uses the `json.RedStagEvents` table to get all rows where `json_value(jsonData, '$.topic') = 'shipment:packed'`.
  - Query: [`CentralStoreQueries.RedStagEvents`](sql/queries/db_CentralStore/RedStagEvents.sql)
- **`rmi_extract`**: pulls closed Shipments from `rmi_ClosedShipments`.
  - Query: [`CentralStoreQueries.PackShipmentRMI`](sql/queries/db_CentralStore/PackShipmentRMI.sql)
- **`acu_extract`**: query from ADF that populates `acu.rsFulfill`. Pulls Open Shipments without Tracking data.
  - Query: [`AcumaticaDbQueries.PackShipment`](sql/queries/AcumaticaDb/PackShipment.sql)

#### Transformation
- For each shipment in `acu_extract`, determine whether a match can be found in the other data sources. If a match is found, format the payloads to drop to Acumatica.

#### Load
- In [`Load.load_shipments`](load/shipment_api.py), for each shipment that was matched, hit the [`AcumaticaAPI`](#acumaticaapi) to get the Shipment's details ([`shipment_details`](#acumaticaapi)).
- If we need to add a package, add it and get details; otherwise just get details of the package.
  - [`AcumaticaAPI.add_package_v2`](#acumaticaapi) and [`AcumaticaAPI.get_package_details`](#acumaticaapi)

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`confirm_open_shipments.py`](pipelines/confirm_open_shipments.py)

### `ShipmentsReadyToConfirm`

Pipeline to confirm any **Open** Shipments that are **fully packed**.

### Execution Behavior

#### Extraction
- Extracts data using the [`AcumaticaDbQueries.ShipmentsReadyToConfirm`](sql/queries/AcumaticaDb/ShipmentsReadyToConfirm.sql) query, returning a `polars.DataFrame` of shipments with a Tracking Number that are ready to confirm.

#### Transformation
- Transforms the query result into a list of dictionaries, each containing a single `{'ShipmentNbr': '123456'}` pair.

#### Load
- Confirms each Shipment via [`AcumaticaAPI.confirm_shipment`](#acumaticaapi).

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)

### Functions

#### `__init__(self)`

#### `extract(self) → pl.DataFrame`
Extracts data using the [`AcumaticaDbQueries.ShipmentsReadyToConfirm`](sql/queries/AcumaticaDb/ShipmentsReadyToConfirm.sql) query.

- **Downstream**: [`SQLConnector.query_to_dataframe`](#sqlconnector): passes the query that returns all Open Shipments that have a Tracking Number and are ready to confirm; returns a polars DataFrame.
- **Returns**: `data_extract` (`pl.DataFrame`): Shipments that can be confirmed in Acumatica

#### `transform(self, data_extract: pl.DataFrame) → list`
Transforms the query result to a list of dictionaries, each containing `ShipmentNbr`.

- **Parameters**: `data_extract` (*pl.DataFrame*): result of the `ShipmentsReadyToConfirm` query
- **Returns**: `data_transformed` (`list`): list of dictionaries, each containing a single kvp pair `{'ShipmentNbr': '123456'}`

#### `load(self, data_transformed: list)`
Confirms Shipments coming from `transform`.

- **Downstream**: [`AcumaticaAPI.confirm_shipment`](#acumaticaapi): given a dictionary containing `ShipmentNbr`, confirms the Shipment in Acumatica
- **Parameters**: `data_transformed` (*list*): list of dictionaries, each `{'ShipmentNbr': '123456'}`
- **Returns**: `self.acu_api.data_log` (`list`): Data to send to [`SQLConnector.checked_upsert`](#sqlconnector)

#### `log_results(self, data_loaded)`

---

## Pipeline - [`redstag_inventory.py`](pipelines/redstag_inventory.py)

### `RedStagInventory`

Pipeline to retrieve inventory levels from RedStag 3PL through their API and load to `db_CentralStore` (**`RedstagInventorySummary`** and **`RedstagInventoryDetail`**).

### Execution Behavior

#### Extraction
- Extracts detailed inventory data from RedStag via [`RedStagAPI.target_api`](#redstagapi)

#### Transformation
- Transforms the response into a dictionary containing two lists of dicts for upsert to **`RedstagInventorySummary`** and **`RedstagInventoryDetail`**

#### Load
- Loads Inventory Summary and Detail level data via [`SQLConnector.checked_upsert`](#sqlconnector)

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`redstag_send_shipments.py`](pipelines/redstag_send_shipments.py)

### `SendRedStagShipments`

Pipeline to send Open Shipments to RedStag using the [`AcumaticaDbQueries.SendRedStagShipments`](sql/queries/AcumaticaDb/SendRedStagShipments.sql) query.

### Execution Behavior

#### Extraction
- Extracts Shipments that are ready to be sent to RedStag
  - `OrigOrderType != 'RC'`: Not a Return order
  - `Status not in ('C', 'L', 'F', 'I')`: Not Completed, Cancelled, Confirmed, or Invoiced
  - `AttributeSHP2WH = 0`: Not yet sent to Warehouse
  - `left(SiteCD, 7) = 'RedStag'`: covers `REDSTAGSWT` and `REDSTAGSLC`

#### Transformation
- Transforms extracted data into different payloads that may be sent to RedStag via [`RedStagAPI`](#redstagapi), as well as the corresponding payloads to be sent to Acumatica via [`AcumaticaAPI`](#acumaticaapi)
- For each row in `data_extract`:
  - Check whether the row's `ShipmentNbr` already exists as a key in the dictionary holding each shipment's transformed data
  - If `ShipmentNbr` doesn't exist, do a second check to verify there's no `rsOrderID` value on the row
  - If `rsOrderID` doesn't exist, transform the payload needed to look up the order at RedStag in [`Transform.transform_lookup_payload`](transform/redstag_send.py)
    - Send the formatted *lookup* payload to RedStag via [`RedStagAPI.target_api`](#redstagapi)
    - Parse the response in [`Transform.transform_lookup_response`](transform/redstag_send.py)
  - If the lookup response is an empty list, we'll send the Shipment. Otherwise, it already exists at RedStag and we stop here for that row.
  - **The remainder is only executed if the order does NOT exist at RedStag**
  - Create the payload needed to send the shipment data to RedStag via [`Transform.transform_order_create_payload`](transform/redstag_send.py) (Acumatica Shipment ↔ RedStag Order)
    - Determine if we need to do anything with the `ShipVia` value via [`Transform._determine_shipvia`](transform/redstag_send.py)
  - Add the row to the dictionary that holds each shipment's transformed data:
    ```python
    self.shipments_done[shipment_nbr] = {
        'ShipmentNbr': shipment_nbr,
        'CustomerID': customer_id,
        'lookup_payload': self.lookup_payload,
        'order_create_payload': self.order_create_payload,
        'execution_payload': self.lookup_payload if self.order_create_payload is None else self.order_create_payload,
        'execution_operation': f'{shipment_nbr}, order.' + ('search' if self.order_create_payload is None else 'create'),
        'note': self.note
    }
    ```

#### Load
- With the transformed shipment payloads, use [`RedStagAPI.target_api`](#redstagapi) to send the `execution_payload` and `execution_operation` values that determine our action
  - If `order_create_payload` doesn't exist (i.e. the shipment was already at RedStag), execute a lookup payload instead of a creation payload
- If `response['status']` equals `'unable_to_process'` or `'new'`, the data was sent successfully and we'll mark that shipment as sent in Acumatica
- If the `note` value from `data_transformed` is `'Already at RedStag'`, mark the shipment as sent in Acumatica so it doesn't come through again
- Format the payload containing relevant attribute values via [`Transform.transform_acu_attribute_payload`](transform/redstag_send.py)
- Drop the formatted payload to Acumatica via [`AcumaticaAPI.send_to_wh_v2`](#acumaticaapi)

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`acu_deletions.py`](pipelines/acu_deletions.py)

### `AcumaticaDeletions`

Pipeline to extract deleted records from **`SOOrder`**, **`SOLine`**, **`SOOrderShipment`**, and **`SOShipment`** in *Acumatica* and load them to *`db_CentralStore`*.

### Execution Behavior

#### Extraction
- Returns records from `AcumaticaDb` deleted within the past two hours from tracked tables (whose deletions are captured by the triggers in [`sql/triggers/`](sql/triggers/))
- Tracked queries (under [`sql/queries/AcumaticaDb/`](sql/queries/AcumaticaDb/)):
  - [`SOOrderDeletions`](sql/queries/AcumaticaDb/SOOrderDeletions.sql)
  - [`SOLineDeletions`](sql/queries/AcumaticaDb/SOLineDeletions.sql)
  - [`SOShipmentDeletions`](sql/queries/AcumaticaDb/SOShipmentDeletions.sql)
  - [`SOOrderShipmentDeletions`](sql/queries/AcumaticaDb/SOOrderShipmentDeletions.sql)

#### Transformation
- Transforms the *polars DataFrames* retrieved by `extract` into a list of dicts

#### Load
- Using [`SQLConnector.checked_upsert`](#sqlconnector), upserts to the respective `_util` table:
  - `SOOrderDeletions` → **`_util.SOOrderDeletions`**
  - `SOLineDeletions` → **`_util.SOLineDeletions`**
  - `SOShipmentDeletions` → **`_util.SOShipmentDeletions`**
  - `SOOrderShipmentDeletions` → **`_util.SOOrderShipmentDeletions`**
- After upserts complete, calls `clean()`.

#### Clean
After the deletions are loaded to `_util`, `clean()` cross-references those tables against CentralStore's mirror tables and removes any rows that correspond to deleted records:

| Cleaner | Source table | Join condition |
|---|---|---|
| `sales_order_del` | `acu.SalesOrders` | `OrderNumber = _util.SOOrderDeletions.OrderNbr` |
| `sales_order_line_del` | `acu.SalesOrders` | `OrderNumber = _util.SOLineDeletions.OrderNbr AND LineNbr = _util.SOLineDeletions.LineNbr` |
| `ship_del` | `acu.Shipments` | `ShipmentNbr = _util.SOShipmentDeletions.ShipmentNbr` |

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract: dict[str, pl.DataFrame])`
- `load(self, data_transformed: dict[str, list])`
- `clean(self)`: Cross-references `_util` deletion tables against `acu.SalesOrders` and `acu.Shipments`; issues `DELETE` via [`SQLConnector.raw_execute`](#sqlconnector)
- `log_results(self, data_loaded)`

---

## Pipeline - [`address_validator.py`](pipelines/address_validator.py)

### `AddressValidator`

Pipeline to **override, update, and validate** any unvalidated addresses on `WB` orders with a status of **On Hold**. Afterwards, **removes the hold** and **creates the Shipment**.

### Execution Behavior

#### Extraction
- Returns Sales Orders from `AcumaticaDb` that require address validation
  - Currently looks at `WB` orders in **Open** status that **do not have a validated address**: see [`AcumaticaDbQueries.ValidateAddresses`](sql/queries/AcumaticaDb/ValidateAddresses.sql)

#### Transformation
- Validates addresses with Avalara via [`AddressVerificationSystem.validate`](#addressverificationsystem-avs)
- Given a dictionary containing the AVS response, format the payload needed to override and update a Customer's `ShipTo` address on a particular Order: see [`Transform.format_order_address_payload`](transform/address_validator.py)
- Formats the constant pieces of the dict that will be loaded to **`_util.acu_api_log`** for both override/update and validate operations: [`Transform.format_acu_api_log_update_override`](transform/address_validator.py) and [`Transform.format_acu_api_log_validate`](transform/address_validator.py)
- Logs differences between the original and validated addresses via [`Transform._log_differences`](transform/address_validator.py)

#### Load
- Overrides and updates Order addresses via [`AcumaticaAPI.target_api`](#acumaticaapi)
- Validates Order address via [`AcumaticaAPI.validate_order_address`](#acumaticaapi)
- Removes Order from hold via [`AcumaticaAPI.order_remove_hold`](#acumaticaapi)
- Creates Shipment via [`AcumaticaAPI.order_create_shipment`](#acumaticaapi)

#### Results Logging
- Upserts Acumatica API interactions to **`_util.acu_api_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)

### Functions
- `__init__(self)`
- `extract(self) → pl.DataFrame`
- `transform(self, data_extract: pl.DataFrame) → list`
- `load(self, data_transformed: list)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`criteo.py`](pipelines/criteo.py)

### `Criteo`

Pipeline to load incremental Criteo ad data from their API to `db_CentralStore`.

### Execution Behavior

#### Extraction
- Extracts the existing data from **`criteo.campaign_performance_daily`** via [`SQLConnector.query_db`](#sqlconnector)
- Extracts data from Criteo's API with the parameters set in [`_re_init`](#__init__-and-_re_init) via [`CriteoAPI.fetch_campaign_data`](#criteoapi)

#### Transformation
- Transforms `data_extract` with `self.transformer`, an instance of [`transform/criteo.py`](transform/criteo.py)'s `Transform` class

#### Load
- Upserts data to **`criteo.campaign_performance_daily`** and **`criteo.diff_log`** via [`SQLConnector.checked_upsert`](#sqlconnector)
  - If any Campaigns are found to have different results vs. the initial database extraction, a difference row is written to **`criteo.diff_log`**

#### Results Logging
- None needed

### Functions

#### `__init__` and `_re_init`

`__init__(self)`: Initializes the Criteo Pipeline. Sets:
```python
self.criteoapi = CriteoAPI(self)
self.transformer = Transform(self)
self.lookback_days = 30
self.api_max_days = 90
self.incremental_end = datetime.now(ZoneInfo('America/New_York')).date()
self.backfill_end   = datetime.now(ZoneInfo('America/New_York')).date() - timedelta(days=1)
```

`_re_init(self, start_date: date, end_date: date, mode: Literal['incremental', 'backfill'] = 'incremental')`: Resets parameters sent to [`CriteoAPI.fetch_campaign_data`](#criteoapi). Sets `self.mode`, `self.start_date`, `self.end_date`.

- **Parameters**:
  - `start_date` (*date*): Low end of the date window used to filter the API query
  - `end_date` (*date*): High end of the date window used to filter the API query
  - `mode` (*'incremental' | 'backfill'*): Which mode to run in

#### `extract(self) → dict[str, pl.DataFrame]`
Extracts data from **`criteo.campaign_performance_daily`** via [`SQLConnector.query_db`](#sqlconnector). Extracts data from Criteo's API with the parameters set in `_re_init` via [`CriteoAPI.fetch_campaign_data`](#criteoapi).

- **Returns**: `data_extract` (`dict[str, pl.DataFrame]`): dict containing `db_extract` and `criteo_extract`

#### `transform(self, data_extract: dict[str, pl.DataFrame])`
Transforms `data_extract` with `self.transformer`, [`Transform`](transform/criteo.py).

- **Downstream**: [`Transform.landing`](transform/criteo.py): landing function that orchestrates transformation of Criteo data
- **Returns**: `data_transformed`: dict containing `diff_log` and `criteo_transformed`

#### `load(self, data_transformed: dict[str, list])`
Upserts data to **`criteo.campaign_performance_daily`** and **`criteo.diff_log`** via [`SQLConnector.checked_upsert`](#sqlconnector).

#### `log_results(self, data_loaded)`

---

## Pipeline - [`acu_to_dbc_quotes.py`](pipelines/acu_to_dbc_quotes.py)

### `AcuToDbcQuotes`

Pipeline to sync Quote data from `AcumaticaDb` into **`acu.Quotes`** in `db_CentralStore`. Only quotes not already present in CentralStore (i.e. with a `LastChecked` value) are loaded.

### Execution Behavior

#### Extraction
- Queries `AcumaticaDb` for Quote data via [`AcumaticaDbQueries.AcuToDbc_Quotes`](sql/queries/AcumaticaDb/AcuToDbc_Quotes.sql)
- Queries `db_CentralStore` for the distinct `QuoteNbr` values already synced (`LastChecked is not null`) from **`acu.Quotes`**

#### Transformation
- Anti-joins the two datasets to isolate Quotes not yet present in CentralStore
- Fills null `LineNbr` values with `99`

#### Load
- Stamps each row with `LastChecked = datetime.now()` (America/New_York)
- Upserts to **`acu.Quotes`** in batches of 500 via [`SQLConnector.checked_upsert`](#sqlconnector)

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self) → dict[str, pl.DataFrame]`
- `transform(self, data_extract: dict[str, pl.DataFrame]) → list`
- `load(self, data_transformed: list)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`sales_order_cleaner.py`](pipelines/sales_order_cleaner.py)

### `SalesOrderCleaner`

Pipeline that identifies and removes out-of-sync rows in **`acu.SalesOrders`** in `db_CentralStore`, rows whose recorded `Status` no longer matches the current status in `AcumaticaDb`.

### Execution Behavior

#### Extraction
- Queries `db_CentralStore` for rows in **`acu.SalesOrders`** that have conflicting statuses (self-join on `OrderNumber = OrderNumber` where `Status != Status`) via [`CentralStoreQueries.SalesOrderCleaner`](sql/queries/db_CentralStore/SalesOrderCleaner.sql)

#### Transformation
Driven by [`transform/cleaner.py`](transform/cleaner.py)'s `Transform` class:
- Builds a `WHERE … IN (…)` clause from the extracted `OrderNbr` values
- Queries `AcumaticaDb` for each order's current status (joining `SOOrder` to `jjStatusLookup`)
- Inner-joins the two datasets on `OrderNbr`
- Groups orders by their current Acumatica status and, for each non-empty group, formats a `DELETE` command that removes all `acu.SalesOrders` rows whose `Status` does not match the authoritative value

#### Load
- For each status group with rows to clean, executes the `delete_cmd` via [`SQLConnector.raw_execute`](#sqlconnector)

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self) → pl.DataFrame`
- `transform(self, data_extract: pl.DataFrame) → dict`
- `load(self, data_transformed: dict)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`acu_to_dbc_sales_orders.py`](pipelines/acu_to_dbc_sales_orders.py)

### `AcuToDbcSalesOrders`

Pipeline to sync Sales Order data from `AcumaticaDb` into **`acu.SalesOrders`** in `db_CentralStore`. Mirrors recent Sales Order changes (last few hours' worth) on the 10-minute cadence.

### Execution Behavior

#### Extraction
- Queries `AcumaticaDb` for Sales Order data via [`AcumaticaDbQueries.AcuToDbc_SalesOrders`](sql/queries/AcumaticaDb/AcuToDbc_SalesOrders.sql)

#### Transformation
- Fills null `LineNbr` values with `99`
- Converts the polars DataFrame to a list of dicts

#### Load
- Stamps each row with `LastChecked = datetime.now()` (America/New_York)
- Upserts to **`acu.SalesOrders`** via [`SQLConnector.checked_upsert`](#sqlconnector); batches of 100 when extract returns ≥ 100 rows, otherwise single upsert

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self) → pl.DataFrame`
- `transform(self, data_extract: pl.DataFrame) → list`
- `load(self, data_transformed: list)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`acu_to_dbc_phone_revenue.py`](pipelines/acu_to_dbc_phone_revenue.py)

### `AcuToDbcPhoneRevenue`

Pipeline to sync phone-channel revenue line items (`OrderType` of **`PH`** or **`BF`**) from `AcumaticaDb` into **`acu.PhoneRevByMonth`** in `db_CentralStore`. Limited to orders created or modified within the last day with a non-null shipping contact phone number; rows with warranty inventory (`InventoryCD like '%NSG-%'`) are excluded.

### Execution Behavior

#### Extraction
- Queries `AcumaticaDb` for PH/BF order lines via [`AcumaticaDbQueries.AcuToDbc_PhoneRevByMonth`](sql/queries/AcumaticaDb/AcuToDbc_PhoneRevByMonth.sql)

#### Transformation
- Converts the polars DataFrame to a list of dicts (already in target table shape)

#### Load
- Upserts to **`acu.PhoneRevByMonth`** via [`SQLConnector.checked_upsert_paginated`](#sqlconnector) in batches of 100

#### Results Logging
- None needed

### Functions
- `__init__(self)`
- `extract(self) → pl.DataFrame`
- `transform(self, data_extract: pl.DataFrame) → list`
- `load(self, data_transformed: list)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`kustomer.py`](pipelines/kustomer.py)

### `SendOrderDetailsToKustomer`

Pipeline to ship complete order details (order + lines + shipments + packages) to Kustomer via webhook. Same class is used by the `kustomer_order_ingest` and `kustomer_order_backfill` Azure Functions, only the query bound by `_re_init` differs.

### Execution Behavior

#### `__init__` and `_re_init`
`__init__(self)` sets:
```python
self.transformer = Transform(self)
self.api         = Kustomer(self)
self.loader      = Load(self)
```

`_re_init(self, mode: Literal['ingest', 'backfill'] = 'ingest')` selects between [`AcumaticaDbQueries.Kustomer_OrderIngest`](sql/queries/AcumaticaDb/Kustomer_OrderIngest.sql) (top 120, new orders only) and [`AcumaticaDbQueries.Kustomer_OrderIngestBackfill`](sql/queries/AcumaticaDb/Kustomer_OrderIngestBackfill.sql) (top 250, not checked in 1 hour) before invoking `.run()`.

#### Extraction
- Queries the chosen ingest query against `AcumaticaDb`
- Reads existing rows from `K_OrderIngest` (so already-sent orders can be filtered)

#### Transformation
- `format_data_extract`: normalizes rows to dicts; expands state/country abbreviations
- Pulls related shipment + package detail via [`AcumaticaDbQueries.Kustomer_ShipmentData`](sql/queries/AcumaticaDb/Kustomer_ShipmentData.sql)
- `add_shipments_to_orders`: builds order-line, shipment-line, and package DataFrames
- `smash_orders`: nests packages into shipments into lines into orders

#### Load
Driven by [`load/kustomer.py`](load/kustomer.py):
- Filters out orders whose JSON payload exactly matches what's already in `K_OrderIngest`
- `send_payloads`: POSTs each order JSON to the Kustomer webhook via [`Kustomer.target_api`](#kustomer)
- Batch-upserts to **`K_OrderIngest`** every 25 orders

#### Results Logging
- None beyond the standard `_util.Logs` insert

### Functions
- `__init__(self)`
- `_re_init(self, mode: Literal['ingest', 'backfill'] = 'ingest')`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`hubspot_snapshot.py`](pipelines/hubspot_snapshot.py)

### `HubSpotSnapshot`

Daily snapshot pipeline that pulls B2B deals and activity counts (calls, emails, meetings, tasks) from HubSpot and writes them to a tracking table plus dated snapshot tables in `db_CentralStore`.

### Execution Behavior

#### Extraction
- [`HubSpotAPI`](#hubspotapi) pre-loads `owners` and `deal pipelines` at init
- [`HubSpotAPI._set_snapshot_windows`](#hubspotapi) sets fiscal-year, month, and week window boundaries (America/New_York)
- `search_deals`: pulls all B2B deals
- `search_activities`: pulls calls, emails, meetings, tasks within window
- Returns `data_extract = { owners, deals, calls, emails, meetings, tasks, timestamp }`

#### Transformation
Driven by [`transform/hubspot_snapshot.py`](transform/hubspot_snapshot.py):
- `activity_counts`: sums activity per window per rep
- `smash_activity_counts`: flattens to SQL-ready rows
- `deals`: formats each deal for SQL

#### Load
- Upserts **`hs.deal_tracking`** (current state)
- Inserts **`hs.deal_snapshots`** (point-in-time history)
- Inserts **`hs.activity_snapshots`** (point-in-time activity counts)

#### Results Logging
- None beyond the standard `_util.Logs` insert

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`hubspot_properties.py`](pipelines/hubspot_properties.py)

### `HubSpotProperties`

Pipeline that mirrors HubSpot property metadata (per object type: contacts, calls, emails, meetings, tasks, leads) into **`hs.Properties`** in `db_CentralStore`. Lets analysts reference the live HubSpot property catalogue from SQL without hitting the API.

### Execution Behavior

#### Extraction
- For each object type, GETs `/crm/v3/properties/{objectType}` via [`HubSpotAPI`](#hubspotapi); tags each row with `ObjectType` and concatenates into a single list

#### Transformation
- Maps each property to the flat dict shape required by **`hs.Properties`**

#### Load
- Upserts to **`hs.Properties`** via [`SQLConnector.checked_upsert`](#sqlconnector)

#### Results Logging
- None beyond the standard `_util.Logs` insert

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`aftership_send.py`](pipelines/aftership_send.py)

### `SendToAfterShip`

Sends newly-tracking-able shipments to AfterShip's `POST /tracking/2026-01/trackings` endpoint so they're registered for tracking notifications.

### Execution Behavior

#### Extraction
- `SlugsAfterShip` query (currently commented out, kept for the future slug catalogue)
- Reads existing log rows from **`_util.AftershipLog`** so already-sent trackings can be filtered
- [`AcumaticaDbQueries.Aftership_Shipments`](sql/queries/AcumaticaDb/Aftership_Shipments.sql): shipments with tracking data shipped on/after `2026-01-01`
- Reads existing `acu.AftershipExport*` rows for additional dedup

#### Transformation
Driven by [`transform/aftership.py`](transform/aftership.py)'s `transform_send`:
- Anti-joins shipment extract vs. log extract on `ShipmentNbr + OrderNbr + Tracking`
- Anti-joins vs. existing AfterShip export records on `OrderNbr + Tracking`
- For each remaining row, builds the AfterShip `tracking` payload

#### Load
- For each row, POSTs to `tracking_endpoint` via [`AfterShip.post_data`](#aftership)
- `_parse_good_tracking_response` / `_parse_bad_tracking_response` builds a row for the log table from the API response (or, on 400, from the payload + metadata)
- Upserts to **`_util.AfterShipLog`**

#### Results Logging
- Upserts to **`_util.AfterShipLog`** (also serves as the load destination)

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`aftership_update.py`](pipelines/aftership_update.py)

### `UpdateAfterShip`

Compares existing AfterShip trackings against the current Acumatica shipment state and PATCHes any drift back to AfterShip (e.g. customer phone updates, shipment tags).

### Execution Behavior

#### Extraction
- `SlugsAfterShip` query (currently inert)
- [`AcumaticaDbQueries.Aftership_Shipments`](sql/queries/AcumaticaDb/Aftership_Shipments.sql): shipments with tracking data
- AfterShip `GET /tracking/2026-01/trackings` with a 5-day `updated_window`, paginated via cursor until `has_next_page = false`

#### Transformation
Driven by `transform/aftership.py`'s `transform_update`:
- Inner-joins AfterShip data vs. Acumatica shipment data on `tracking_number + order_number ↔ Tracking + OrderNbr`
- Builds a row payload containing `customers` + `shipment_tags`
- `filter_update`: compares each field (normalized/lowercased) and keeps only rows where `customer_diff` or `tag_diff` is true
- Output payload contains only the fields that changed

#### Load
- For each id → values pair, PUT `tracking_endpoint/{id}` via [`AfterShip.put_data`](#aftership)
- Builds log row from the response (good) or from response + ShipmentNbr/OrderNbr/Tracking fallback (bad)

#### Results Logging
- Upserts to **`_util.AfterShipLog`**

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`aftership_to_dbc.py`](pipelines/aftership_to_dbc.py)

### `AfterShipToDbc`

Pulls recent AfterShip trackings (plus their checkpoint history) back into `db_CentralStore` for analytics. Runs twice a day.

### Execution Behavior

#### Extraction
- AfterShip `GET /tracking/2026-01/trackings` with `created_at_min = now - 7 days`, paginated via cursor

#### Transformation
- Skips rows with no customer phone
- Maps each tracking to an `acu.AftershipExportv2` row, converting `updated_at`/`created_at` to America/New_York time
- For each checkpoint in `tracking.checkpoints`, maps to an `acu.AftershipExportDetailv2` row

#### Load
- Upserts **`acu.AftershipExportv2`** (one row per tracking)
- Upserts **`acu.AftershipExportDetailv2`** (one row per checkpoint)

#### Results Logging
- None beyond the standard `_util.Logs` insert

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Pipeline - [`rmi_link_to_acu.py`](pipelines/rmi_link_to_acu.py)

### `RMILinkToAcu`

Writes the RMI tracking URL (`Link3PL`) and `RMAID` values back to Acumatica's `SOShipmentKvExt` table so the links are visible directly on the Shipment record. Bridges what we know in `db_CentralStore` (via `rmi_RMAStatus`) into Acumatica without going through the REST API.

### Execution Behavior

#### Extraction
- [`AcumaticaDbQueries.RMI_Link3PL`](sql/queries/AcumaticaDb/RMI_Link3PL.sql): Shipments that still have a null 3PL link
- [`CentralStoreQueries.RMI_Link3PL_RMAStatus`](sql/queries/db_CentralStore/RMI_Link3PL_RMAStatus.sql): distinct Shipments from RMI `rmi_RMAStatus`

#### Transformation
Uses an in-memory `polars.SQLContext`:
- Registers `acu_extract` and `rmi_extract` as `acu` and `rmi` respectively
- Inner-joins on `a.ShipmentNbr = r.RMANumber`
- Derives `ValueString`:
  - `FieldName = 'AttributeLINK3PL'` → Link3PL URL
  - `FieldName = 'AttributeRMAID'`   → RMAID
  - otherwise NULL
- Converts to a list of dicts

#### Load
- Writes directly to `AcumaticaDb.SOShipmentKvExt` via paginated `checked_upsert` (note: this is the rare pipeline that writes back to Acumatica's database, not via the API)

#### Results Logging
- None beyond the standard `_util.Logs` insert

### Functions
- `__init__(self)`
- `extract(self)`
- `transform(self, data_extract)`
- `load(self, data_transformed)`
- `log_results(self, data_loaded)`

---

## Dev / Scaffolded Pipelines

These live under [`pipelines/dev/`](pipelines/dev/) and are not wired into any Azure Function timer. They're imported from `pipelines.__init__.py` so manual scripts can still exercise them.

| Class | File | Purpose |
|---|---|---|
| `AuditFulfillment` | [`pipelines/dev/audit_fulfillment.py`](pipelines/dev/audit_fulfillment.py) | Fulfillment audit pipeline (WIP). Driven by the [`CentralStoreQueries.AuditFulfillment`](sql/queries/db_CentralStore/AuditFulfillment.sql) query |
| `NotifyFulfillmentOps` | [`pipelines/dev/notify_fulfillment_ops.py`](pipelines/dev/notify_fulfillment_ops.py) | Notifies the Fulfillment Operations team about RMI return orders stuck with the *"Item does not exist"* error. Driven by [`CentralStoreQueries.NotifyFulfillmentOpsTeam`](sql/queries/db_CentralStore/NotifyFulfillmentOpsTeam.sql) |
| `ShopifyGraphQL` | [`pipelines/dev/shopify.py`](pipelines/dev/shopify.py) | Pulls daily sales data from Shopify via GraphQL/ShopifyQL. Uses [`ShopifyAPI`](#shopifyapi) |

---

# Connectors

Each connector wraps an external system and is initialized with a parent `pipeline` reference, which the connector uses for its logger name and (where applicable) for accumulating `data_log` entries that the pipeline later upserts to **`_util.acu_api_log`**.

---

## SQLConnector

**File:** [`connectors/sql.py`](connectors/sql.py)

Wraps a SQLAlchemy engine for either `db_CentralStore` or `AcumaticaDb`. Holds query bundles ([`CentralStoreQueries`](#centralstorequeries) or [`AcumaticaDbQueries`](#acumaticadbqueries)) loaded from [`sql/queries/`](sql/queries/) by name, and exposes a generic upsert driven by the [`TABLES`](config/settings.py) config.

### Methods

#### `__init__(self, pipeline, database_name: str)`
Initializes the SQLConnector for either `db_CentralStore` or `AcumaticaDb`.

- **Parameters**:
  - `pipeline`: Pipeline using the SQLConnector. If just using the connector without a pipeline, send a `str`.
  - `database_name` (*str*): Either `'db_CentralStore'` or `'AcumaticaDb'`
- **Sets**:
  ```python
  self.pipeline = pipeline
  self.logger = logging.getLogger(f'{database_name}')
  self.database_name = database_name
  self.config = DATABASES[database_name]
  self.engine = self._create_engine()
  self.raw_connection = self.engine.raw_connection()
  self.queries = _QUERY_CLASSES.get(database_name, Queries)(database_name)
  ```
- `_create_engine` builds the connection string from credentials in [`DATABASES`](config/settings.py).

#### `query_db(self, query: str) → pl.DataFrame`
Given a string of SQL text, executes and returns a polars DataFrame.

#### `query_to_dataframe(self, query: Query) → pl.DataFrame`
Given a [`Query`](#query-bundles) (from `AcumaticaDbQueries` or `CentralStoreQueries`), executes its text and returns a polars DataFrame.

#### `insert_df(self, df_data_loaded: pl.DataFrame, table_name: str)`
Given a polars DataFrame and the name of a table, inserts the contents of the DataFrame to that table.

#### `checked_upsert(self, table_name: str, data: list)`
Given a table name and a list of rows (dicts) to insert, performs an upsert to the database.

- **Downstream**: `_dict_to_params`: utility that formats keys, columns, and update_columns with their values into parameters
- **Parameters**:
  - `table_name` (*str*): Schema-qualified table name (e.g. `'_util.acu_api_log'`). `'AdDetails'` doesn't need a schema since it belongs to `dbo`, but you could pass `'dbo.AdDetails'`.
  - `data` (*list*): A list of dictionaries that correspond to the values declared in [`TABLES`](config/settings.py). Each dict should be formatted to contain the values that were mapped in the table configuration.

#### `_dict_to_params(self, d: dict, keys: list) → tuple`
Internal utility used by `checked_upsert` to format table keys, columns, and update_columns with their respective values into parameters.

#### `raw_execute(self, query: str)`
Given an Insert/Update/Delete command, returns the number of rows affected.

### Query Bundles

`SQLConnector` discovers SQL files at runtime by name. Each query class subclasses `Queries` and exposes its files as attributes.

#### CentralStoreQueries
Queries to be executed within `db_CentralStore` ([`sql/queries/db_CentralStore/`](sql/queries/db_CentralStore/)):

| Attribute | Purpose |
|---|---|
| [`ReturnsPendingReciept`](sql/queries/db_CentralStore/ReturnsPendingReciept.sql) | Checks **`rmi_RMAStatus`** for any *Closed* or *Receipted* Returns |
| [`StatusCheckRMI`](sql/queries/db_CentralStore/StatusCheckRMI.sql) | Pulls each distinct `RMANumber` from **`rmi_ClosedShipments`**, **`rmi_Receipts`**, and **`_util.rmi_send_log`** that has a null `RMAStatus`, a status not equal to `'CLOSED'`/blank, or a `LastChecked` value within the last two days that is not `'OPEN'` |
| [`AuditFulfillment`](sql/queries/db_CentralStore/AuditFulfillment.sql) | Reserved for the in-progress fulfillment audit pipeline |
| [`PackShipmentRedStag`](sql/queries/db_CentralStore/PackShipmentRedStag.sql) | Original driver of the RedStag Confirmations Celigo flow. Pulls shipments using the `acu.rs_*` tables to determine which should be shipped |
| [`PackShipmentRMI`](sql/queries/db_CentralStore/PackShipmentRMI.sql) | Pulls closed Shipments from `rmi_ClosedShipments` |
| [`RedStagEvents`](sql/queries/db_CentralStore/RedStagEvents.sql) | More robust version of `PackShipment`. Uses **`json.RedStagEvents`** to get all rows where `json_value(jsonData, '$.topic') = 'shipment:packed'` |
| [`NotifyFulfillmentOpsTeam`](sql/queries/db_CentralStore/NotifyFulfillmentOpsTeam.sql) | Pulls all Open Return orders from `_util.RMI_Send_Log` stuck with the *"Item does not exist"* error from RMI |
| [`SalesOrderCleaner`](sql/queries/db_CentralStore/SalesOrderCleaner.sql) | Pulls rows from `acu.SalesOrders` that have conflicting status values (self-join on `OrderNumber` where `Status != Status`) |
| [`RMI_Link3PL_RMAStatus`](sql/queries/db_CentralStore/RMI_Link3PL_RMAStatus.sql) | Pulls all distinct Shipments from `rmi_RMAStatus`, used by [`RMILinkToAcu`](#pipeline---rmi_link_to_acupy) to build the join against Acumatica |
| [`aftership_shipments`](sql/queries/db_CentralStore/aftership_shipments.sql) | Helper view used by AfterShip dedup logic |

#### AcumaticaDbQueries
Queries to be executed within `AcumaticaDb` ([`sql/queries/AcumaticaDb/`](sql/queries/AcumaticaDb/)):

| Attribute | Purpose |
|---|---|
| [`SendRMIReturns`](sql/queries/AcumaticaDb/SendRMIReturns.sql) | Pulls all `RC` Sales Orders in **Open** status with `AttributeRCSHP2WH` null or != 1 |
| [`SendRMIShipments`](sql/queries/AcumaticaDb/SendRMIShipments.sql) | Pulls Shipments ready to be sent to RMI as Type Ws |
| [`SendRedStagShipments`](sql/queries/AcumaticaDb/SendRedStagShipments.sql) | Pulls Shipments ready to be sent to RedStag |
| [`OpenRCsNoReceipt`](sql/queries/AcumaticaDb/OpenRCsNoReceipt.sql) | Pulls all Open `RC` Orders that have been sent to RMI and don't have a Shipment (Receipt) |
| [`ShipmentsReadyToConfirm`](sql/queries/AcumaticaDb/ShipmentsReadyToConfirm.sql) | Pulls all Open Shipments with a Tracking Number that are ready to be confirmed |
| [`PackShipment`](sql/queries/AcumaticaDb/PackShipment.sql) | Query from ADF that populates `acu.rsFulfill` |
| [`ValidateAddresses`](sql/queries/AcumaticaDb/ValidateAddresses.sql) | Pulls Orders with unvalidated addresses |
| [`SOOrderDeletions`](sql/queries/AcumaticaDb/SOOrderDeletions.sql) | Pulls records from `SOOrder` that have been deleted in Acumatica for transfer to `db_CentralStore` |
| [`SOLineDeletions`](sql/queries/AcumaticaDb/SOLineDeletions.sql) | Pulls records from `SOLine` that have been deleted in Acumatica for transfer to `db_CentralStore` |
| [`SOShipmentDeletions`](sql/queries/AcumaticaDb/SOShipmentDeletions.sql) | Pulls records from `SOShipment` that have been deleted in Acumatica for transfer to `db_CentralStore` |
| [`SOOrderShipmentDeletions`](sql/queries/AcumaticaDb/SOOrderShipmentDeletions.sql) | Pulls records from `SOOrderShipment` that have been deleted in Acumatica for transfer to `db_CentralStore` |
| [`AcuToDbc_Quotes`](sql/queries/AcumaticaDb/AcuToDbc_Quotes.sql) | Pulls Quote data from `AcumaticaDb` for sync to `acu.Quotes` in `db_CentralStore` |
| [`AcuToDbc_SalesOrders`](sql/queries/AcumaticaDb/AcuToDbc_SalesOrders.sql) | Pulls Sales Order data for sync to `acu.SalesOrders` in `db_CentralStore` |
| [`Kustomer_OrderIngest`](sql/queries/AcumaticaDb/Kustomer_OrderIngest.sql) | Top 120 new orders for Kustomer **ingest** mode |
| [`Kustomer_OrderIngestBackfill`](sql/queries/AcumaticaDb/Kustomer_OrderIngestBackfill.sql) | Top 250 orders not checked in 1 hour for Kustomer **backfill** mode |
| [`Kustomer_ShipmentData`](sql/queries/AcumaticaDb/Kustomer_ShipmentData.sql) | Shipment + package detail for the orders selected by `Kustomer_OrderIngest*` |
| [`Kustomer_FilteredOutOrders`](sql/queries/AcumaticaDb/Kustomer_FilteredOutOrders.sql) | Orders explicitly excluded from the Kustomer push |
| [`Aftership_Shipments`](sql/queries/AcumaticaDb/Aftership_Shipments.sql) | Shipments with tracking data eligible to be sent to / updated in AfterShip |
| [`RMI_Link3PL`](sql/queries/AcumaticaDb/RMI_Link3PL.sql) | Shipments that have a null 3PL link value at RMI, used by [`RMILinkToAcu`](#pipeline---rmi_link_to_acupy) |

---

## AcumaticaAPI

**File:** [`connectors/acu_api.py`](connectors/acu_api.py)

Cookie-authenticated REST client for Acumatica ERP (`erp.journeyhl.com`, version `22.200.001`, endpoint `pyplatform`).

### Methods

#### `__init__(self, pipeline)`
Initializes the AcumaticaAPI connector and authenticates against `erp.journeyhl.com` using credentials from [`ACUMATICA_API`](config/settings.py).

- **Sets**:
  ```python
  self.logger        = logging.getLogger(f'{pipeline.pipeline_name}.acu_api')
  self.pipeline      = pipeline
  self.version       = '22.200.001'
  self.auth_type     = 'Cookie'
  self.uri           = 'https://erp.journeyhl.com/entity'
  self.endpoint_name = 'pyplatform'
  self.base_uri      = f'{self.uri}/{self.endpoint_name}/{self.version}'
  self.username      = ACUMATICA_API['username']
  self.password      = ACUMATICA_API['password']
  self.company       = 'JHL'
  self.data_log      = []
  self.session       = requests.Session()
  self._auth()
  ```

#### Sales Order operations

##### `order_create_receipt(self, order_data: dict)`
Creates Receipt for an Open `RC` Sales Order (RC orders only). `order_data` must contain `OrderType`, `OrderNbr`, and `AcctCD`.

##### `order_create_shipment(self, order_data: dict)`
Creates a Shipment for an Open Sales Order. `order_data` must contain `OrderType`, `OrderNbr`, and `AcctCD`.

##### `sales_order_get_shipment(self, order_data)`
Returns Shipment details for a given Sales Order. `order_data` must contain `OrderType` and `OrderNbr`. Returns a formatted dict of Shipment data; `Shipment` is `None` if no shipment exists.

##### `rc_send_to_wh(self, OrderNbr, OrderType, CustomerID)`
Marks an Order's *RC Ship to Warehouse* attribute (`AttributeRCSHP2WH`) to true via PUT.

- **Returns**: `self.status_description` (str), `body` (dict)

##### `validate_order_address(self, order_data: dict)`
Given a dict containing `OrderType` and `OrderNbr`, attempts to validate a Sales Order's addresses.

##### `target_api(self, endpoint: str, payload_data: dict, operation: str = 'put', descr: str = None)`
Generic dispatcher for ad-hoc operations against an Acumatica endpoint.

- **Parameters**:
  - `endpoint` (*str*): Endpoint to append to the base URI
  - `payload_data` (*dict*): Dictionary containing `target_api_update_payload`, `log_update_success`, `log_update_error`, `acu_api_data_log`
  - `operation` (*str*): `PUT`, `POST`, or `GET`
  - `descr` (*str*): Description of what the payload will do (e.g. `Override & Update`)
- **Returns**: `return_bool` (`bool`): If `descr == 'Override & Update'`, returns `True` on success, `False` otherwise.

##### `order_remove_hold(self, order_data: dict)`
Removes the `On Hold` flag from an Order.

#### Shipment operations

##### `shipment_details(self, shipment_data: dict)`
GETs Shipment details for a given Shipment. `shipment_data` must include `ShipmentNbr`. Returns the parsed JSON response.

##### `shipment_details_attr(self, shipment_data: dict)`
GETs Shipment details for a given Shipment along with whether or not it was Sent to WH. Requires `ShipmentNbr` and `OrderNbr`.

##### `add_package(self, shipment_data: dict)`
Given a Shipment, creates and packs a package. Required keys on `shipment_data`: `ShipmentNbr`, `ExtRefNbr` or `TrackingNbr`, `details`, `details.InventoryCD`, `details.Qty`, `details.SplitLineNbr`. Returns `shipment_data` augmented with package details.

##### `add_package_v2(self, shipment_data: dict)`
Revised version of `add_package`, used in RedStag. Instead of formatting the package inside the function like `add_package`, format it beforehand and pass the completed payload inside `shipment_data` as `PackagePayload`. Required: `ShipmentNbr`, `PackagePayload`.

##### `get_package_details(self, shipment_data, body=None)`
Given a Shipment, returns Package details. `shipment_data` must include `ShipmentNbr`.

##### `confirm_shipment(self, shipment_data: dict)`
Confirms a Shipment. Requires `ShipmentNbr`.

##### `update_reason_code(self, shipment_data: dict, line_data: dict)`
Updates Reason Code on the line of a Shipment to *RETURN*. Returns `line_data` with the updated Reason Code.

##### `send_to_wh(self, ShipmentNbr, CustomerID)`
Generic Send-to-Warehouse handler. Returns `self.status_description` (str) and `body` (dict).

##### `send_to_wh_v2(self, ShipmentNbr: str, CustomerID: str, attribute_payload: dict = {})`
Marks a Shipment's *Ship to Warehouse* attribute (`AttributeSHP2WH`) to true via PUT, while accepting a dynamic payload of additional attribute values to populate.

- **Returns**: `self.status_description` (str), `body` (dict)

#### Utilities

##### `parse_shipment_details(self, shipment_data: dict, response: requests.Response)`
Given the Acumatica API's response, converts to JSON and combines with `shipment_data`. The API response contains the detailed information regarding the shipment, including line details. Returns `shipment_data` augmented with `Status`, `package_count`, `line_count`, and `details` (per-line `LineNbr`, `InventoryCD`, `Qty`, `SplitLineNbr`, `ReasonCode`, `id`).

##### `parse_response(self, response: requests.Response, entity_type: dict)`
Internal response parser used by several other methods.

#### Customer / Contact

##### `customers(self, query=None, limit=100)`
GET Customer details. `query` filters response (default `None`); `limit` defaults to `100`.

##### `contact(self, query=None, limit=100)`
GET Customer Contact details. Same parameters as `customers`.

#### Authentication

##### `_auth(self)`
Logs into the Acumatica API and stashes a session cookie.

##### `_logout(self)`
Logs out of the Acumatica API.

#### Work in Progress

##### `update_customer_address(self, payload: dict)`
Not yet wired into a pipeline.

##### `validate_customer_address(self, customer_data: dict)`
**Work in Progress**

---

## AcuOData

**File:** [`connectors/acu_odata.py`](connectors/acu_odata.py)

OData protocol client for Acumatica.

### Methods
- `__init__(self, pipeline)`
- `get_data(self, url: str)`: Performs the OData GET against the supplied URL.

---

## RMIAPI

**File:** [`connectors/rmi_api.py`](connectors/rmi_api.py)

Token-based REST client for RMI / BackTrackSRL (`https://api.backtracksrl.com/`).

### Methods

#### `__init__(self, pipeline)`
Initializes the RMIAPI connector and authenticates using credentials from [`RMI`](config/settings.py).

- **Sets**:
  ```python
  self.logger    = logging.getLogger(f'{pipeline.pipeline_name}.rmi_api')
  self.base_uri  = 'https://api.backtracksrl.com/'
  self.auth_type = 'Token'
  self.headers   = {
      'Content-Type': 'application/json',
      'ident': '64A648DD-E186-42E1-8A46-23D76A401FF0'
  }
  self.username  = RMI['username']
  self.password  = RMI['password']
  self.session   = requests.Session()
  self._auth()
  ```

#### `_auth(self)`
Logs into RMI's API. On success, retrieves a token and sets `self.token`.

#### `closed_shipments(self)`
Hits the `ClosedShipmentsV1` endpoint and returns Closed Shipments from the last 21 days.

#### `get_receipts(self)`
Hits the `Receipts` endpoint and returns Receipts from the last 21 days.

#### `target_api(self, endpoint: str, ...)`
Generic dispatcher. Used by [`GetRMAsFromRMI`](#pipeline---get_rmas_from_rmipy) against the `RMAs` endpoint to pull RMA status records from the last 120 days in one call.

#### `get_rma(self, rma_number)`
Legacy single-RMA status fetch; superseded by `target_api` against `RMAs`, but kept for ad-hoc lookups.

---

## RMIXML

**File:** [`connectors/rmi_xml.py`](connectors/rmi_xml.py)

SOAP/XML client used to send Shipments (Type W) and Returns (Type 3) to `https://jhl.returnsmanagement.com/webserviceV2/rma/rmaservice.asmx`.

### Methods

#### `__init__(self, pipeline)`
Initializes the RMIXML connector. Logs in using credentials from [`RMI`](config/settings.py).

- **Sets**:
  ```python
  self.pipeline      = pipeline
  self.logger        = logging.getLogger(f'{pipeline.pipeline_name}.rmi_xml')
  self.session       = requests.Session()
  self.login()
  self.send_url      = 'https://jhl.returnsmanagement.com/webserviceV2/rma/rmaservice.asmx'
  self.send_headers  = {
      'Content-Type': 'text/xml; charset=utf-8',
      'SOAPAction':   'http://bactracs.bactracksrl.com/rmaservice/CreateNew'
  }
  self.results       = []
  ```

#### Other methods
- `login(self)`: Authenticates against the SOAP endpoint.
- `initiate_send(self, data_transformed)`: Top-level orchestrator over a transformed shipment/return payload.
- `post_W(self, shipment)`: Sends a Type W payload (used by [`SendRMIShipments`](#pipeline---rmi_send_shipmentspy)).
- `post_3(self, return_order)`: Sends a Type 3 payload (used by [`SendRMIReturns`](#pipeline---rmi_send_returnspy)).
- `_format_w_lines(self, shipment)`: Formats per-line XML for a Type W shipment.
- `_format_3_lines(self, shipment)`: Formats per-line XML for a Type 3 return.
- `get_rmi_msg(self, rmi_response: requests.Response)`: Extracts the message body / status from an RMI SOAP response.

---

## RedStagAPI

**File:** [`connectors/redstag_api.py`](connectors/redstag_api.py)

JSON-RPC client for RedStag Fulfillment (`https://wms.redstagfulfillment.com/api/jsonrpc`).

### Methods

#### `__init__(self, pipeline)`
Initializes the RedStagAPI connector and authenticates using credentials from [`REDSTAG`](config/settings.py).

- **Sets**:
  ```python
  self.logger    = logging.getLogger(f'{pipeline.pipeline_name}.redstag_api')
  self.base_uri  = 'https://wms.redstagfulfillment.com/api/jsonrpc'
  self.auth_type = 'Token'
  self.username  = REDSTAG['username']
  self.password  = REDSTAG['password']
  self.headers   = {"content-type": "application/json", "accept": "application/json"}
  self.session   = requests.Session()
  self._auth()
  ```

#### `_auth(self)`
Logs into RedStag's API. On success, retrieves a token and sets `self.token`.

#### `target_api(self, payload_target: list, operation: str)`
Instead of a method per RedStag operation (`order.create`, `order.search`, etc.), `target_api` allows all API operations to be completed through one tight function.

- **Parameters**:
  - `operation` (*str*): Description of what the function will be doing during execution; used for logging
  - `payload_target` (*list*): list formatted for RedStag's API

---

## CriteoAPI

**File:** [`connectors/criteo_api.py`](connectors/criteo_api.py)

API connector for the Criteo ad API.

### Methods

#### `__init__(self, pipeline: Criteo)`
Initializes the Criteo API connector and authenticates by requesting a fresh Bearer token via `_auth`.

- **Sets**:
  ```python
  self.pipeline      = pipeline
  self.logger        = logging.getLogger(f'{pipeline.pipeline_name}.redstag_api')
  self.token_url     = 'https://api.criteo.com/oauth2/token'
  self.stats_url     = 'https://api.criteo.com/2026-01/statistics/report'
  self.client_id     = CRITEO['client_id']
  self.client_secret = CRITEO['client_secret']
  self.ad_id         = CRITEO['ad_id']
  self.headers       = {"Content-Type": "application/x-www-form-urlencoded"}
  self.session       = requests.Session()
  self._auth()
  ```
- `client_id`, `client_secret`, and `ad_id` come from [`CRITEO`](config/settings.py).

#### `_auth(self)`
Requests a fresh Bearer token from the Criteo OAuth2 endpoint. Returns the token used for subsequent authentication.

#### `fetch_campaign_data(self) → pl.DataFrame`
Using `self.ad_id`, `self.pipeline.start_date`, and `self.pipeline.end_date`, builds a JSON payload and POSTs to the Criteo statistics endpoint. Uses `self.token` from `_auth` as the Bearer Authorization header. Returns a polars DataFrame containing the API response.

---

## AddressVerificationSystem (AVS)

**File:** [`connectors/avs.py`](connectors/avs.py)

Avalara AVS REST client (`https://rest.avatax.com`).

### Methods

#### `__init__(self, pipeline)`
Initializes the AVS connector with Basic-auth headers built from [`AVS`](config/settings.py) (`account` + `license`).

- **Sets**:
  ```python
  self.logger             = logging.getLogger(f'{pipeline.pipeline_name}.avs')
  self.pipeline           = pipeline
  self.base_uri           = 'https://rest.avatax.com'
  self.endpoint_validate  = f'{self.base_uri}/api/v2/addresses/resolve'
  self.credentials        = f"{AVS['account']}:{AVS['license']}"
  self.encoded            = b64encode(self.credentials.encode()).decode()
  self.headers            = {
      "Authorization": f"Basic {self.encoded}",
      "Content-Type":  "application/json"
  }
  self.session            = requests.Session()
  ```

#### `validate(self, order_data: dict, s_or_b: str)`
Given a dict with Shipping and/or Billing address data, validate the address with Avalara's AVS.

- **Downstream**: `_parse_response`: adds the validated address to `order_data` and returns it
- **Parameters**:
  - `order_data` (*dict*): dict containing `OrderNbr` and address data
  - `s_or_b` (*str*): Whether we are validating the **S**hipping or **B**illing address
- **Returns**: `order_data` with the validated address included

#### `_parse_response(self, order_data: dict, s_or_b: str) → dict`
Given a response from AVS, adds the validated address to `order_data` and returns it. Called by `validate`.

---

## HubSpotAPI

**File:** [`connectors/hubspot_api.py`](connectors/hubspot_api.py)

HubSpot CRM client used by [`HubSpotSnapshot`](#pipeline---hubspot_snapshotpy) and [`HubSpotProperties`](#pipeline---hubspot_propertiespy). Authenticates via Bearer token from [`HUBSPOT`](config/settings.py); pre-loads owners and deal pipelines at init.

### Methods

#### `__init__(self, pipeline: HubSpotSnapshot | HubSpotProperties)`
Initializes the connector, sets `Authorization: Bearer {HUBSPOT["access_token"]}`, and calls `_get_deal_pipelines` + `_get_owners`.

#### `_set_snapshot_windows(self)`
Computes `fiscal_year_start`, `week_start`, `month_start` (America/New_York) for [`HubSpotSnapshot`](#pipeline---hubspot_snapshotpy).

#### `_request(self, method, path, **kwargs) → dict`
Generic JSON HTTP wrapper around `self.session`.

#### Search / catalog helpers
- `search_deals(self, ...)`: paginated deal search
- `search_activities(self, ...)`: paginated activity search (calls, emails, meetings, tasks)
- `_get_owners(self)` / `_get_deal_pipelines(self)`: catalog preload
- `_get_properties(self, object_type)`: GET `/crm/v3/properties/{object_type}` (used by [`HubSpotProperties`](#pipeline---hubspot_propertiespy))

---

## AfterShip

**File:** [`connectors/aftership.py`](connectors/aftership.py)

REST client for AfterShip's tracking API (`https://api.aftership.com/tracking/2026-01/trackings`). Authenticates with the `as-api-key` header from [`AFTERSHIP`](config/settings.py).

### Methods

#### `__init__(self, pipeline: SendToAfterShip | UpdateAfterShip | AfterShipToDbc)`
Sets `self.tracking_endpoint`, initializes a `requests.Session`, and prepares the `trackings` buffer used by paginated reads.

#### `get_data(self, endpoint, params={}) → dict`
GET wrapper around `self.session`. Used by `retrieve_trackings` and `paginate_tracking`.

#### `retrieve_trackings(self, pipeline_name: str, updated_window: timedelta = timedelta(hours=2))`
Drives tracking retrieval: calls `get_data`, then recursively calls `paginate_tracking` until `has_next_page = false`. Extends `self.trackings`.

#### `paginate_tracking(self, ...)`
Recursive paginator that walks `next_cursor` until exhausted.

#### `post_data(self, endpoint, payload)`
POSTs a new tracking to `/trackings`. Used by [`SendToAfterShip`](#pipeline---aftership_sendpy).

#### `put_data(self, endpoint, payload)`
PUT/PATCHes an existing tracking by id. Used by [`UpdateAfterShip`](#pipeline---aftership_updatepy).

---

## Kustomer

**File:** [`connectors/kustomer.py`](connectors/kustomer.py)

Webhook-based push client for Kustomer. Authenticates via the webhook URL from [`KUSTOMER`](config/settings.py); no bearer token because Kustomer's incoming webhook embeds the secret in the URL.

### Methods

#### `__init__(self, pipeline: SendOrderDetailsToKustomer)`
Sets `self.webhook` and initializes a `requests.Session`.

#### `target_api(self, payload_data: dict, operation: str = 'post', descr: str = None)`
Single dispatcher used by [`load/kustomer.py`](load/kustomer.py)'s `Load.send_payloads`. POSTs `payload_data['execution_payload']` to the webhook and logs `log_update_success` or `log_update_error` based on response status. Returns the raw `requests.Response`.

---

## ShopifyAPI

**File:** [`connectors/shopify.py`](connectors/shopify.py)

GraphQL/ShopifyQL client for Shopify Admin (`https://{domain}/admin/api/{version}/graphql.json`). Currently used by the scaffolded [`ShopifyGraphQL`](#dev--scaffolded-pipelines) dev pipeline.

### Methods
- `__init__(self, pipeline)`: Loads credentials from [`SHOPIFY`](config/settings.py); auto-builds a default ShopifyQL sales-utm-report query via `_set_shopql`
- `post(self, url='', headers={}, payload={})`: POST wrapper that defaults to the configured GraphQL endpoint and the auto-built ShopifyQL payload
- `_set_shopql(self)`: Sets `self.shopifyql` and `self.graphql_query`

---

## Teams

**File:** [`connectors/teams.py`](connectors/teams.py)

Minimal Microsoft Teams webhook poster used for ad-hoc alerting. Credentials from [`TEAMS`](config/settings.py).

### Methods
- `__init__(self, pipeline)`: Loads webhook URL plus tenant / client / object identifiers; raises `ValueError` if `TEAMS_WEBHOOK_URL` is unset
- `send_message(self, message: str)`: POSTs `{'text': message}` to the configured webhook

---

## Sharepoint

**File:** [`connectors/sharepoint.py`](connectors/sharepoint.py)

Microsoft Graph client for SharePoint file retrieval. Authenticates via the OAuth2 client-credentials flow against `https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token`, then resolves the SharePoint site ID. Credentials from [`SHAREPOINT`](config/settings.py).

### Methods
- `__init__(self, pipeline)`: Sets default sites (`marketing = …/sites/Marketing`) and calls `_auth`
- `_auth(self)`: Obtains an access token and stores `self.site_id`
- `get_file(self, server_relative_path: str) → bytes`: GET the binary content of a SharePoint file via Graph

---

## TransunionAPI

**File:** [`connectors/transunion.py`](connectors/transunion.py)

TransUnion connector: present in the codebase but not used in any active pipeline.

### Methods
- `__init__(self) -> None`

---

# Transforms

The [`transform/`](transform/) modules contain the data-shaping logic each pipeline calls between `extract()` and `load()`. Each module exposes a single `Transform` class.

| Module | Pipeline | Key methods |
|---|---|---|
| [`transform/rmi_send.py`](transform/rmi_send.py) | [`SendRMIShipments`](#pipeline---rmi_send_shipmentspy), [`SendRMIReturns`](#pipeline---rmi_send_returnspy) | `transform(data_extract: pl.DataFrame)` |
| [`transform/rmi_receipt_pull.py`](transform/rmi_receipt_pull.py) | [`GetReceiptsFromRMI`](#pipeline---get_receipts_from_rmipy), [`GetClosedShipmentsFromRMI`](#pipeline---get_closed_shipments_from_rmipy) | `transform_receipts`, `transform_closed_shipments` |
| [`transform/rmi_rmas.py`](transform/rmi_rmas.py) | [`GetRMAsFromRMI`](#pipeline---get_rmas_from_rmipy) | `transform_status_records` |
| [`transform/create_acu_receipt.py`](transform/create_acu_receipt.py) | [`CreateAcuReceipt`](#pipeline---create_acu_receiptpy) | `transform(data_extract: dict[str, pl.DataFrame])` |
| [`transform/pack_shipment.py`](transform/pack_shipment.py) | [`PackShipments`](#pipeline---pack_shipmentspy) | `transform`, `group_tracking`, `_format_package`, `_format_friendly_package_payload`, `transform_redstag_events` |
| [`transform/redstag_inventory.py`](transform/redstag_inventory.py) | [`RedStagInventory`](#pipeline---redstag_inventorypy) | `transform_inventory` |
| [`transform/redstag_send.py`](transform/redstag_send.py) | [`SendRedStagShipments`](#pipeline---redstag_send_shipmentspy) | `transform`, `transform_lookup_payload`, `transform_lookup_response`, `transform_order_create_payload`, `transform_acu_attribute_payload`, `_check_shipvia`, `_determine_shipvia` |
| [`transform/criteo.py`](transform/criteo.py) | [`Criteo`](#pipeline---criteopy) | `landing`, `transform_criteo`, `find_differences`, `_format_table` |
| [`transform/address_validator.py`](transform/address_validator.py) | [`AddressValidator`](#pipeline---address_validatorpy) | `transform`, `format_order_address_payload`, `format_acu_api_log_update_override`, `format_acu_api_log_validate`, `_log_differences` |
| [`transform/cleaner.py`](transform/cleaner.py) | [`SalesOrderCleaner`](#pipeline---sales_order_cleanerpy) | `transform`, `clean`, `parse_orders` |
| [`transform/kustomer.py`](transform/kustomer.py) | [`SendOrderDetailsToKustomer`](#pipeline---kustomerpy) | `format_data_extract`, `add_shipments_to_orders`, `smash_orders` |
| [`transform/hubspot_snapshot.py`](transform/hubspot_snapshot.py) | [`HubSpotSnapshot`](#pipeline---hubspot_snapshotpy) | `activity_counts`, `smash_activity_counts`, `deals` |
| [`transform/aftership.py`](transform/aftership.py) | [`SendToAfterShip`](#pipeline---aftership_sendpy), [`UpdateAfterShip`](#pipeline---aftership_updatepy), [`AfterShipToDbc`](#pipeline---aftership_to_dbcpy) | `transform_send`, `transform_update`, `filter_update`, `_lander`, `_parse_good_tracking_response`, `_parse_bad_tracking_response` |
| [`transform/notify_fulfillment_ops.py`](transform/notify_fulfillment_ops.py) | [`NotifyFulfillmentOps`](#dev--scaffolded-pipelines) (dev) | `transform(data_extract: pl.DataFrame)` |
| [`transform/audit_fulfillment.py`](transform/audit_fulfillment.py) | [`AuditFulfillment`](#dev--scaffolded-pipelines) (dev) | `transform` (WIP) |

### Notable transform docstrings

#### `transform/pack_shipment.py`: `Transform.group_tracking(data_transformed: list)`
Adds one entry per shipment to the shipments dictionary, keyed by `ShipmentNbr`.

- **Returns**: `shipments` (*dict*): dict of Shipments keyed by `ShipmentNbr`. Prevents duplicating the same shipment.
- Each entry must contain `ShipmentNbr`, `PackagePayload`, and `_FriendlyPackagePayload`.
  - `PackagePayload`: payload to be sent to Acumatica API via [`add_package_v2`](#acumaticaapi)
  - `_FriendlyPackagePayload`: more readable version of `PackagePayload`; not used in API calls.
  ```python
  shipments = {
      'ShipmentNbr': '078983',
      'PackagePayload': {...},
      '_FriendlyPackagePayload': [{...}]
  }
  ```

#### `transform/pack_shipment.py`: `Transform._format_package(shipment_line_data, shipment_data)`
For the Shipment passed in `shipment_line_data`, formats the Acumatica API payload to add package(s) to an Acumatica shipment.

- **Returns**: `package_payload` (*dict*): dictionary containing `ShipmentNbr` and `Packages` (list of all packages to add)

#### `transform/redstag_send.py`: `Transform`
Should return and populate the `rsOrderID` value back to Acumatica afterwards.

- `transform(data_extract: pl.DataFrame) → dict`: Transforms `data_extract`. If there's no RedStag `OrderID` value in Acumatica, looks up the `ShipmentNbr` at RedStag to confirm it hasn't been sent yet. Returns `self.shipments_done` (`dict[str, dict]`).
- `transform_lookup_payload(shipment_nbr: str)`: Transforms `data_extract` to the format needed for looking up order information via the *RedStag API*. Sets `self.lookup_response`.
- `transform_lookup_response(self)`: Parses and formats the response from the RedStag order lookup. Sets `self.lookup_result` (*list*).
- `transform_order_create_payload(shipment: dict, data_extract: pl.DataFrame)`: Transforms `shipment` into the format needed for posting an Order (the Acumatica Shipment) to RedStag. `shipment` must contain `ShipmentNbr`, `ShipToName`, `ShipToAddress1`, `ShipToCity`, `ShipToState`, `ShipToZip`, `ShipToCountry`, `ShipToPhone`. Sets `self.order_create_payload`.
- `transform_acu_attribute_payload(data: dict) → dict`: Once the `order_id` (`rsOrderID`) is known, builds the payload that marks the shipment as Sent to WH. `data` must contain `data_3pl`, the response from RedStag.
- `_check_shipvia(iterator: int, item: dict, item_payload: list)`: Determines what `ShipVia` value should be set on the parent level on multi-item Shipments. Not called for the first row of a Shipment, but for every subsequent row. Returns `True` if the line's `ShipVia` matches the line at the index specified in `iterator`, otherwise `False`.
- `_determine_shipvia(shipment: dict, item_payload: list)`: Following a call to `_check_shipvia`, determines what the parent `ShipVia` value should be. Returns `(item_payload, ship_via)`.

#### `transform/criteo.py`: `Transform`
- `landing(data_extract: dict[str, pl.DataFrame])`: Landing function called from [`Criteo.transform`](#pipeline---criteopy). Downstream: `transform_criteo` (cleans Criteo data prior to upserting `criteo.campaign_performance_daily`) and `find_differences` (compares pre-extraction `criteo.campaign_performance_daily` data against the cleaned Criteo response from [`CriteoAPI.fetch_campaign_data`](#criteoapi)).
- `transform_criteo(criteo_extract: pl.DataFrame)`: Handles primary `data_extract` transformation that cleans data prior to upserting `criteo.campaign_performance_daily`.
- `find_differences(db_extract: pl.DataFrame, criteo_transformed: pl.DataFrame)`: Compares pre-extraction `criteo.campaign_performance_daily` data against the cleaned Criteo response. Downstream: `_format_table`. Returns `diff_log` (data for `criteo.diff_log`) and `criteo` (data for `criteo.campaign_performance_daily`).
- `_format_table(row: dict)`: If a row is to be inserted/updated, formats it for `criteo.campaign_performance_daily`.

#### `transform/address_validator.py`: `Transform`
- `format_order_address_payload(order_avs: dict)`: Given a dictionary containing a response from AVS, formats the payload needed to override and update a Customer's `ShipTo` address on a particular Order. Required keys on `order_avs`: `OrderType`, `OrderNbr`, `CustomerID`, and the formatted AVS response (`vAddressLine1`, `vAddressLine2`, `vCity`, `vState`, `vPostalCode`, `vCountryID`). Downstream: `_log_differences`.
- `format_acu_api_log_update_override(order_avs: dict)`: Formats the constant part of the dict loaded to `_util.acu_api_log` when overriding/updating an address.
- `format_acu_api_log_validate(order_avs: dict)`: Formats the constant part of the dict loaded to `_util.acu_api_log` when validating an address.
- `_log_differences(order_avs: dict)`: Notes differences between the original Acumatica address and the AVS response.

---

# Load Helpers

Reusable load orchestration extracted from individual pipelines, kept under [`load/`](load/).

## `load/shipment_api.py`: `Load`
Class for smart handling of Acumatica API interactions, used by [`CreateAcuReceipt`](#pipeline---create_acu_receiptpy), [`PackShipments`](#pipeline---pack_shipmentspy), and [`SendRedStagShipments`](#pipeline---redstag_send_shipmentspy).

- `load_shipments(data_transformed: dict)`: Iterates through `data_transformed` items and adds a Package for each Shipment in Acumatica if it meets all conditions. Each entry's key is the `ShipmentNbr` and the value contains `PackagePayload`.
- `load_receipts(data_transformed)`: Used by [`CreateAcuReceipt`](#pipeline---create_acu_receiptpy). Iterates through each shipment needing a receipt, determines if it's ready to be created, and acts accordingly.
- `check_reason_code(receipt_response, line)`: Verifies reason code for a single shipment line.
- `check_if_ready_for_confirm(receipt_response)`: Determines whether the shipment can be confirmed.

## `load/load_redstag_send.py`: `Load`
Class for smart handling of Acumatica API interactions, used by [`SendRedStagShipments`](#pipeline---redstag_send_shipmentspy).

- `send_shipments(data_transformed)`: Once the `order_id` (`rsOrderID`) is known from RedStag, marks the shipment as Sent to WH.

## `load/kustomer.py`: `Load`
Class used by [`SendOrderDetailsToKustomer`](#pipeline---kustomerpy) to orchestrate Kustomer pushes.

- `send_payloads(data_transformed)`: For each order, POSTs the formatted payload via [`Kustomer.target_api`](#kustomer), batches `format_db_row` results, and upserts to **`K_OrderIngest`** every 25 orders
- `format_db_row(order, response)`: Formats a single `K_OrderIngest` row from the order payload and Kustomer response

## `load/address_validator.py`: `Load`
Class used by [`AddressValidator`](#pipeline---address_validatorpy).

- `landing(data_transformed)`: Per-order orchestrator that drives the override/update → validate → remove-hold → create-shipment chain via the Acumatica API

---

# Pipeline Base Class

**File:** [`pipelines/base.py`](pipelines/base.py)

Every pipeline subclasses `Pipeline`. The base class:

- Initializes both [`SQLConnector`](#sqlconnector) instances (`self.centralstore`, `self.acudb`) so subclasses get authenticated database access for free
- Names a per-pipeline `logging.Logger` (`self.logger`) and configures `colorlog` with millisecond-precision timestamps (America/New_York) the first time any pipeline runs
- Attaches a `LogHistory` handler so every log emit is captured in `self.logs` for later persistence
- Stamps `self.run_timestamp` at the start of every `run()`
- Drives the lifecycle in [`Pipeline.run`](pipelines/base.py):

  ```python
  def run(self):
      self.run_timestamp = datetime.now(ZoneInfo('America/New_York'))
      self.logger.info(f'Starting {self.pipeline_name}')

      self.logger.info('Extracting...')
      data_extract     = self.extract()

      self.logger.info('Transforming...')
      data_transformed = self.transform(data_extract)

      self.logger.info('Loading...')
      data_loaded      = self.load(data_transformed)

      self.logger.info('Logging...')
      self.log_results(data_loaded)

      # Persist the in-memory log buffer to _util.Logs
      try:
          self.centralstore.insert_df(pl.DataFrame(self.logs), '_util.Logs')
      except Exception:
          self.logger.warning("Couldn't insert logs to SQL but pipeline execution was successful")

      return {
          'pipeline':    self.pipeline_name,
          'status':      'success',
          'extracted':   data_extract,
          'transformed': data_transformed,
          'loaded':      data_loaded
      }
  ```

`extract`, `transform`, `load`, and `log_results` are all `@abstractmethod`: every concrete pipeline must implement them.

---

# Documentation

In addition to this README, the [`docs/`](docs/) folder contains per-pipeline and per-Azure-Function reference material:

- [`docs/pipelines/`](docs/pipelines/), one mermaid flowchart per pipeline, showing the `__init__` → `extract` → `transform` → `load` → `log_results` path with all SQL queries, API calls, and downstream tables labeled in-line
- [`docs/functions/`](docs/functions/), one document per Azure Function in [`function_app.py`](function_app.py). Each includes:
  - the cron schedule
  - which pipeline(s) it invokes
  - the embedded pipeline flowchart(s)
  - the SQL queries each pipeline depends on, with relative links to the `.sql` files

The mermaid diagrams in `docs/` render directly in GitHub's web UI and in most Markdown previewers.
