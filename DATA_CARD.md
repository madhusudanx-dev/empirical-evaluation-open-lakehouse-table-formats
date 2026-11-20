# Data Card: TPC-DS Retail Benchmark Dataset & Lakehouse Evaluation Artifacts

## 1. Dataset Overview
* **Dataset Name:** Synthetic TPC-DS Retail Store Sales Dataset (Scale Factor 1)
* **Generator:** DuckDB `tpcds` extension (`CALL dsdgen(sf=1)`)
* **Domain:** Retail / E-Commerce Point-of-Sale Transactions
* **Temporal Coverage:** 5 years (1998–2003), partitioned into 60 monthly buckets (`sold_ym` in `YYYY-MM` format)
* **Baseline Storage Format:** Snappy-compressed Apache Parquet (`137.8 MB` across `store_sales`)
* **License & Usage:** Synthetic benchmark dataset derived from the Transaction Processing Performance Council (TPC-DS) specification; contains zero Personally Identifiable Information (PII) or real customer records.

---

## 2. Schema & Table Composition

The benchmark generates five relational tables modeled after a standard retail star schema:

| Table Name | Role | Approximate Row Count (SF=1) | Primary / Join Keys | Description |
| :--- | :--- | :---: | :--- | :--- |
| **`store_sales`** | Fact Table | ~2,879,987 | `(ss_item_sk, ss_ticket_number)` | Granular retail order line items including quantities, list/sales prices, coupons, taxes, and net paid amounts. |
| **`date_dim`** | Dimension | ~73,049 | `d_date_sk` | Calendar attributes used to derive the `sold_ym` (`YYYY-MM`) partition key and filter fiscal periods. |
| **`item`** | Dimension | ~18,000 | `i_item_sk` | Product catalog metadata including brand, category, and manufacturer IDs (`i_manufact_id`). |
| **`customer`** | Dimension | ~100,000 | `c_customer_sk` | Synthetic customer demographics and account identifiers. |
| **`store`** | Dimension | ~12 | `s_store_sk` | Physical retail store metadata. |

### Key Columns in `store_sales` Used in Workloads
* `ss_item_sk` (INT): Foreign key to `item`; part 1 of composite business key.
* `ss_ticket_number` (BIGINT): Order transaction identifier; part 2 of composite business key.
* `ss_sold_date_sk` (INT): Foreign key to `date_dim`.
* `ss_customer_sk` (INT): Foreign key to `customer` (nullable; filtered in Top-100 customer query).
* `ss_quantity` (INT -> widened to BIGINT in DDL test): Units purchased.
* `ss_ext_sales_price` (DECIMAL): Extended sales price aggregated in TPC-DS Q3 star join.
* `ss_net_paid` (DECIMAL): Net revenue paid; mutated (`* 1.01`) during upsert batches and hashed for ML audit verification.
* `ss_coupon_amt` (DECIMAL -> renamed to `ss_coupon_amount` in DDL test): Discount amount.
* `ss_ext_tax` (DECIMAL -> dropped in DDL test): Tax amount.
* `sold_ym` (STRING): Derived `YYYY-MM` partition column (~60 partitions).
* `updated_at` (TIMESTAMP): Ingestion/mutation timestamp used as the Hudi precombine ordering field.

---

## 3. Synthetic Mutation & Audit Splits

During execution, the benchmark programmatically derives two additional data splits from `store_sales`:
1. **Upsert & Fragmentation Batches (`make_batch`):**
   * **Composition:** 50% updates to existing `(ss_item_sk, ss_ticket_number)` keys (modifying `ss_net_paid` by +1%) and 50% synthetic new inserts (offsetting `ss_ticket_number` by `+10,000,000,000`).
   * **Distributions:** *Recent* (sampled exclusively from the latest 2 monthly partitions) and *Scattered* (sampled uniformly across all 60 monthly partitions) at `0.1%`, `1.0%`, and `5.0%` volume fractions.
2. **ML Training Audit Window (`1999-01` to `2001-12`):**
   * **Size:** ~1,692,986 rows (3-year historical slice).
   * **Deterministic Fingerprint:** Order-independent row count paired with `SUM(CAST(xxhash64(ss_item_sk, ss_ticket_number, ss_quantity, ss_net_paid) AS DECIMAL(38,0)))` to verify bit-exact dataset reproducibility before and after table mutations, compaction, and history expiration.

---

## 4. Known Limitations & Intended Use
* **Intended Use:** Reproducible evaluation of lakehouse table formats (Iceberg, Delta Lake, Hudi) on Apache Spark for batch analytics, MERGE throughput, schema evolution, and ML data lineage governance.
* **Limitations:** Scale Factor 1 represents a small-scale functional and relative-ratio benchmark (~138 MB). Partition sizes (~2.3 MB/month) fall below the 128 MB target file size threshold, limiting observable file-count reduction during compaction tests. Production capacity planning should validate findings at Scale Factors 10–100+ on distributed cloud object storage (S3/GCS/ADLS).
