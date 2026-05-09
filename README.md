
## Problem Statement

Three squads at Mal (Cards, Transfers, Bill Payments) each built their own payment event pipelines with different schemas – different column names, status values, timestamp formats, and missing fields. This makes it impossible to get a single, reliable view of all payments for finance, compliance, or product analytics.

## Solution Overview

1. Ingest three CSV files (mounted in a Databricks Volume) using PySpark.
2. Validate each row: required fields, data types, positive amounts, valid status.
3. Transform each source to a common canonical schema via dedicated mapping functions.
4. Union all valid rows into a single DataFrame and write to Parquet .
5. Data Contract Versioning – example migration from v1 (amount + currency) to v2 (combined `amount_currency`).
6. SQL Queries – demonstrate how downstream teams would query the unified model.

## Databricks Notebooks

### 1. `setup_file.py`
- Creates a Unity Catalog schema (e.g., `bank.default`) if not exists.
- Creates a Volume (e.g., `/Volumes/bank/default/bank_data/landing`) to host raw CSV files.
- Uploads the three mock CSV files from local or generates them.
- Sets up the target directory for output Parquet.

### 2. `tranform_data.py`
- Reads the three CSVs from the Volume.
- For each source, applies a transformation function that maps source columns to the canonical schema.
- Adds a `payment_type` column (`card`, `transfer`, `bill_payment`).
- Performs schema validation (using PySpark DataFrame filters or custom UDFs).
- Unifies all valid records and writes to `unified_payments` table in Parquet format.
- Executes three example SQL queries (total volume by type, failed payments trend, customer average amount) and prints results.

### 3. `migrate_v1_to_v2.py`
- Reads the v1 Parquet file from the main pipeline.
- Creates a new column `amount_currency` as `concat(amount, ' ', currency)`.
- Drops the old `amount` and `currency` columns.
- Writes the result as `unified_payments_v2.parquet`.
- Shows a simple example of how a breaking change would be rolled out while keeping v1 available.

## How to Run (on Databricks)

### Prerequisites
- Databricks workspace with Unity Catalog enabled.
- Cluster with DBR 13.3 LTS or higher (includes PySpark).
- Access to create schemas/volumes (or modify paths to existing locations).

### Steps

1. Clone or Upload the notebooks to your Databricks workspace.
2. Run `setup_file` first – this creates the volume and uploads the mock CSV data.
   - Note: If you already have a volume with the three CSV files, you can skip this step.
3. Open `01_main_pipeline`** – attach it to your cluster and run all cells.
   - The pipeline will output table:
     - `unified_payments` (valid records)
4. Verify the output by running the SQL cells at the bottom.
5. Run `02_migrate_v1_to_v2` to see the schema evolution example.

### Expected Outputs
- Unified Parquet location: `/Volumes/bank/default/bank_data/payment_event/`
- Console output shows sample rows and SQL query results.

## Key Design Decisions

| Aspect | Decision | Rationale |
|--------|----------|-----------|
| Canonical schema | `event_id`, `payment_type`, `customer_id`, `amount`, `currency`, `status`, `timestamp`, `payment_method_details` (JSON) | Covers all finance/compliance needs while extensible via JSON field. |
| Schema validation | PySpark DataFrame filters + optional `pydantic` for complex rules | Fast for large data; can be extended. |
| Partitioning | By `payment_type` and `date` | Speeds up queries that filter by type or time. |
| Versioning | v1 → v2 via separate script; old data remains accessible | Non‑breaking migration path for downstream consumers. |

## Production Considerations (for 100K+ events/day)

- Use Auto Loader instead of static CSV reads for incremental ingestion.
- Replace error log table with a dead‑letter queue (DLQ)** in S3/ADLS.
- Implement idempotent writes using `event_id` as a deduplication key.

## Dependencies

- PySpark (included in Databricks Runtime)
- No external libraries required – all code uses built-in Spark functions.

## Author

Mohammed Haider – Data Engineer

## License

This project is for demonstration purposes as part of a technical assessment for Mal Bank.
