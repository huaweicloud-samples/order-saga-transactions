# Order Saga Transactions: Long-Lived Transactions with FunctionGraph

**English** | [简体中文](README.zh-CN.md)

An e-commerce order saga on Huawei Cloud `cn-north-4`: one orchestrator function drives the
forward transaction chain `order-new → payment-debit → inventory-reserve`; when any step fails
with a business error, the already-completed work is compensated in reverse order
(`inventory-release → payment-credit → order-update`), the saga outcome is published to an SMN
topic, and the full orchestration trace is written to LTS. Business state is stored in three
tables of an externally provisioned GeminiDB DynamoDB-compatible instance, with deterministic
UUIDv5 transaction IDs so task retries overwrite instead of duplicating records.

## Architecture

```text
Verification script / client
  |  synchronous invoke (event body = order payload, scenario injected via order_id prefix)
  v
FunctionGraph orchestrator (Node.js18.15, 128 MB / 180 s, agency: op_svc_cff)
  |  inter-function synchronous calls: delegated token + /v2/{project_id}/fgs/functions/{urn}/invocations
  |  transient errors: FULL-jitter retry (Interval 2 s, MaxAttempts 3, BackoffRate 2, MaxDelay 8 s)
  |  business errors: no retry, go straight to compensation
  |--------- forward chain ---------------|--------- compensation chain (reverse) ---------|
  |-- order-new          (orders table)   |-- inventory-release   (inventory table)         |
  |-- payment-debit      (payments table) |-- payment-credit     (payments table)          |
  |-- inventory-reserve  (inventory table)|-- order-update       (orders table)            |
  |
  |-- saga trace: [SAGA] structured logs -> LTS log group + 7 per-function log streams
  |-- outcome notification: {order_id, status: "success" | "failed", error} -> SMN topic (REST publish, delegated token)
  v
GeminiDB (DynamoDB-compatible interface, externally provisioned)
  |-- {app_name}-orders     PK order_id
  |-- {app_name}-payments   PK transaction_id
  `-- {app_name}-inventory  PK transaction_id
```

- The orchestrator keeps the "last successful order payload" in memory and hands it to every
  compensating transaction, mirroring the source state machine's workflow-variable semantics:
  compensations always receive a clean order payload, never one with error data bolted on.
- A compensation failure publishes a failure notification and aborts the remaining compensations;
  a forward business failure with fully successful compensation ends the saga without a
  notification (both semantics come from the source state machine).
- Payment/inventory transaction IDs are deterministic UUIDv5 values derived from
  `order_id + "|" + transaction_type`, so a retried task overwrites its own prior record.

## Contents

```text
.
├── main.tf / variables.tf / outputs.tf / versions.tf
├── src/
│   ├── orchestrator.js        # Saga orchestrator function (forward chain, retry, compensation, notify)
│   ├── order-new.js           # Save the order, set order_status = "New"
│   ├── order-update.js        # Compensation: set order_status = "Pending"
│   ├── payment-debit.js       # Write the "Debit" payment transaction
│   ├── payment-credit.js      # Compensation: write the negated "Credit" refund
│   ├── inventory-reserve.js   # Write the "Reserve" inventory transaction
│   └── inventory-release.js   # Compensation: write the "Release" inventory transaction
├── fixtures/scenario-1..7.json   # Failure-injection payloads (order_id prefix drives the scenario)
├── scripts/verify_e2e.py          # Post-deploy E2E verification (response + LTS trace + database + idempotency)
└── scripts/static_check.py        # node --check, secret-literal scan, UUIDv5 cross-check
```

## Prerequisites

1. A Huawei Cloud account with permissions for FunctionGraph, SMN, LTS, and creating the IAM
   agency; access keys are injected only through environment variables.
2. Terraform >= 1.5.0 with the `huaweicloud` provider >= 1.95.0.
3. A **pre-provisioned GeminiDB instance whose interface type is DynamoDB-compatible**
   (console: Database > GeminiDB, choose the DynamoDB-compatible interface). Recommended form:
   the cluster mode with **at least 3 nodes** so the data plane is highly available. The instance
   is charged on demand (the console is authoritative); destroy it in the console after
   verification.
   This directory does not create the database instance and does not declare its resources.
4. The GeminiDB DynamoDB-compatible data plane authenticates only with the instance account:
   the user name is fixed to **rwuser** and the password is the **instance administrator
   password** set at creation time. IAM AK/SK and agency temporary credentials are **not**
   accepted by the data plane. The cluster form serves the data plane on **port 80** (the
   single-node form uses 8000); `geminidb_port` defaults to 80 accordingly.
5. `geminidb_access_key` and `geminidb_secret_key` are never written into variable files, source
   code, fixtures, or logs. Inject them through Terraform sensitive variable environment
   variables before deployment. The connection address is the instance load-balancing private
   IP; when the FunctionGraph runtime subnet can reach it, no VPC configuration is needed on the
   functions.

## Resource inventory

| Resource | Count | Naming | Billing |
|---|---|---|---|
| FunctionGraph functions | 7 | `${app_name}-orchestrator` and `${app_name}-{step}` | On-demand; negligible for sample traffic |
| IAM agency (delegated to op_svc_cff) | 1 | `${app_name}-agency-<random>` | Free |
| SMN topic | 1 | `${app_name}-saga-topic` | Billed per published message; ≤1 per saga |
| LTS log group | 1 | `${app_name}-logs` (TTL 7 days) | Log storage on demand; tiny volume |
| LTS log streams | 7 | one per function | Included with LTS storage |
| GeminiDB tables | 3 | `${app_name}-orders/payments/inventory` | Included with the instance |

No APIG, OBS, VPC, gateway, queue, container, or virtual machine resources are created; the
GeminiDB instance itself is external and pre-provisioned.

## Terraform configuration and deployment

Sensitive values are injected only through `TF_VAR_...` environment variables; do not write them
into `.tfvars` files.

```bash
cd Repositories/order-saga-transactions
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy

terraform init -plugin-dir=../../.terraform-plugin-cache
terraform validate

export TF_VAR_geminidb_endpoint="<private-geminidb-lb-ip>"
export TF_VAR_geminidb_port="80"
export TF_VAR_geminidb_access_key="rwuser"
export TF_VAR_geminidb_secret_key="$(secret-provider read geminidb-admin-password)"
terraform plan
# After reviewing the plan and confirming account permissions:
terraform apply
```

The `secret-provider` placeholder stands for the tenant's existing secure injection tool; do not
substitute real credentials into files in this directory. `geminidb_*` are sensitive variables,
so the Terraform state contains sensitive configuration and must be kept in a protected backend
with access control; `outputs.tf` outputs no credentials. Terraform passes the data-plane
parameters into FunctionGraph `user_data`; each step function receives its own `TABLE_NAME`, and
the orchestrator receives the six step-function URNs, the SMN topic URN, and the region.

## Verification

`scripts/verify_e2e.py` invokes all seven scenarios, asserts the saga terminal state, the LTS
`(eventType, step)` trace, the three-table database outcome, and the UUIDv5 idempotency
(overwriting re-invoke), then writes `verification-result.json`.

```bash
# Huawei Cloud API signing keys: read from ../../credentials.csv row 1 (row[1]=AK, row[2]=SK)
# GeminiDB data-plane credentials for direct database assertions:
export GEMINIDB_ENDPOINT="<private-geminidb-lb-ip>"
export GEMINIDB_PORT="80"
export GEMINIDB_ACCESS_KEY="rwuser"
export GEMINIDB_SECRET_KEY="$(secret-provider read geminidb-admin-password)"
export GEMINIDB_REGION="cn-north-4"

E:/code/huawei-samples/.tmp-gemini-key-venv/Scripts/python.exe scripts/verify_e2e.py
```

The script ensures the three business tables exist (CreateTable, idempotent; primary keys match
the source template), then runs each scenario. Expected outcomes per scenario (source testdata
semantics; `order_id` prefix drives the injected failure):

| Scenario | order_id prefix | Injected error | Expected path |
|---|---|---|---|
| 1 | `1` | ErrProcessOrder | order-new fails → order-update compensation → saga failed |
| 2 | `11` | ErrProcessOrder + ErrUpdateOrderStatus | order-new fails → order-update also fails → notify → abort |
| 3 | `2` | ErrProcessPayment | payment-debit fails → payment-credit + order-update compensations |
| 4 | `22` | ErrProcessPayment + ErrProcessRefund | payment-debit fails → payment-credit also fails → notify → abort |
| 5 | `3` | ErrReserveInventory | inventory-reserve fails → release + credit + order-update compensations |
| 6 | `33` | ErrReserveInventory + ErrReleaseInventory | inventory-reserve fails → inventory-release also fails → notify → abort |
| 7 | `4`–`9` | none | full forward chain → success notification → saga succeeded |

Database assertions per scenario: scenario 7 has all three tables populated (order status
`New`, one `Debit`, one `Reserve`); scenario 3 has both `Debit` and `Credit` records with
different deterministic transaction IDs (the source unit test `TestDebitAndCreditIDsDiffer`);
scenario 5 leaves the reverse-compensation traces (`Release` + `Credit` + order `Pending`);
scenarios 1/2 leave only the order row with status `Pending`; scenarios 4/6 stop the remaining
compensations, so the order stays `New` and only the steps that ran before the abort are
recorded. Every payment/inventory transaction ID must equal the deterministic
`uuid5(8b03bc51-…, order_id + "|" + type)` value.

Static checks run locally without any cloud access:

```bash
python scripts/static_check.py   # node --check all src/*.js, secret-literal scan, UUIDv5 cross-check vs RFC 4122
```

## Cleanup

```bash
unset HTTP_PROXY HTTPS_PROXY http_proxy https_proxy ALL_PROXY all_proxy
terraform destroy
```

Deleting this directory's resources removes the functions, agency, SMN topic, and LTS group with
its streams. The GeminiDB instance is external and its lifecycle is not managed here: destroy it
in the console when it is no longer needed. Table data inside the instance can be removed by
deleting the `${app_name}-*` tables or resetting the instance.

## Architecture and runtime characteristics

- **Orchestration pattern**: Implements an in-function synchronous saga pattern where the central orchestrator coordinates order creation, payment deduction, and inventory reservation within a single invocation session (180 s execution budget).
- **Retry strategy**: Transient network and service errors are handled with exponential backoff and full jitter; business validation failures immediately initiate rollback compensations.
- **Compensation flow**: The orchestrator tracks the transactional state and executes inverse operations (e.g. refunding payment, releasing inventory) when subsequent steps fail.
- **Event notifications**: Integrated with Simple Message Notification (SMN) to broadcast alerts when critical compensation steps fail.
- **Persistence interface**: Interacts with GeminiDB via its DynamoDB-compatible protocol with SigV4 client signing.
- **Observability**: Each microservice function logs directly into a dedicated LTS log stream for centralized tracking.
