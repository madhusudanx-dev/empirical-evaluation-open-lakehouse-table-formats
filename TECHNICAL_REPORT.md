# Technical Evaluation of Open Lakehouse Table Formats: Apache Iceberg, Delta Lake, and Apache Hudi

**Target Architecture:** Retail Analytics & Regulated Machine Learning Data Platform  
**Benchmark Engine:** Apache Spark 3.5.3 | TPC-DS Scale Factor 1 | Copy-on-Write (CoW)  
**Formats Evaluated:** Apache Iceberg (v1.6.1), Delta Lake (v3.2.0), Apache Hudi (v0.15.0)

---

## 1. Executive Summary

Modern cloud data lakes rely on open table formats to bring ACID transactions, schema evolution, time travel, and snapshot isolation to immutable Parquet files stored on cheap object storage. Without a metadata ledger, a directory of Parquet files lacks concurrency control, version history, and atomic mutation guarantees. To select an optimal storage foundation for a retail data platform with strict machine learning (ML) auditability requirements, we conducted an empirical benchmark comparing **Apache Iceberg (v1.6.1)**, **Delta Lake (v3.2.0)**, and **Apache Hudi (v0.15.0)** under identical compute, data, and configuration constraints.

Using an audit-first weighted evaluation rubric across 59 measured metrics and 22 workflow stages, **Apache Iceberg achieved the highest overall score (91.9/100)**, outperforming **Delta Lake (78.2/100)** and **Apache Hudi (59.5/100)**. Iceberg delivered the fastest initial bulk load (45.6s), led all five analytical query benchmarks, won all six upsert scenarios, and was the **only format capable of reproducing an exact historical ML training dataset after routine table cleanup** via its native named snapshot tagging mechanism. Delta Lake demonstrated superior storage efficiency (1.20x raw Parquet footprint) and the fastest compaction command execution (1.8s), making it attractive for Databricks-centric environments where storage cost and operational simplicity outweigh cross-engine portability and tag-based governance. Apache Hudi trailed in Copy-on-Write batch workloads and encountered an incomplete audit test, reflecting its architectural specialization in Merge-on-Read (MoR) streaming ingestion rather than CoW batch analytics.

---

## 2. Experimental Methodology & Testbed Architecture

### 2.1 Hardware and Runtime Environment
All experiments were executed on a single-node environment configured with **2 vCPUs**, **12 GB RAM**, and local disk storage. To eliminate cross-engine interference and classpath conflicts, each table format ran in an isolated **PySpark 3.5.3** session with Adaptive Query Execution (AQE) enabled, UTC timezone normalization, `8g` driver heap memory, and `8` SQL shuffle partitions.

### 2.2 Dataset & Fairness Controls
We generated a 5-year retail order dataset using DuckDB's TPC-DS generator (`store_sales` fact table plus `date_dim`, `item`, `customer`, and `store` dimensions) at **Scale Factor 1**:
* **Volume:** Approximately 2.88 million order lines spanning 60 monthly partitions (`sold_ym` formatted as `YYYY-MM`).
* **Baseline Size:** 137.8 MB of Snappy-compressed Parquet files.
* **Primary Business Key:** Composite key `(ss_item_sk, ss_ticket_number)`.
* **Storage Parity:** All formats were configured with a **128 MB target file size** (`134,217,728` bytes) and **Copy-on-Write (CoW)** update semantics.

### 2.3 Workload Suite (8 Stages, 59 Metrics)
1. **Bulk Load:** Full historical ingestion from source Parquet into each table format.
2. **Analytical Queries:** 5 representative retail queries measured across 1 cold run and the median of 3 warm runs (full scan aggregation, single-month partition pruning, point lookup, TPC-DS Q3 star join, and top-100 customer ranking).
3. **Upserts (MERGE):** 6 batch scenarios combining 50% updates to existing records and 50% new inserts across batch sizes of `0.1%`, `1.0%`, and `5.0%` under two key distributions: *recent* (latest 2 months, simulating late-arriving corrections) and *scattered* (uniformly distributed across all 60 months, simulating historical restatements).
4. **Compaction:** 20 consecutive micro-commits (`0.02%` batch size) to induce small-file fragmentation, followed by format-native file compaction (`rewrite_data_files`, `OPTIMIZE`, or `run_clustering`).
5. **Storage & Cloud Cost Modeling:** Disk footprint before and after history expiration, normalized against the 137.8 MB raw source and extrapolated to a 10 TB S3 deployment (`$0.023/GB-month`).
6. **Time Travel:** Historical table reads by explicit version/snapshot ID and by commit timestamp.
7. **Schema Evolution:** In-place DDL execution for adding (`ADD COLUMN`), renaming (`RENAME COLUMN`), type-widening (`INT` to `BIGINT`), and dropping (`DROP COLUMN`) columns.
8. **ML Audit & Replay:** Fingerprinting a 3-year ML training window (`1999-01` to `2001-12`, ~1.69M rows) via order-independent `xxhash64` summation, mutating and compacting the table, running routine retention cleanup (`expire_snapshots`, `VACUUM 0`, or `run_clean`), and attempting deterministic dataset reconstruction.

---

## 3. Empirical Results & Performance Analysis

### 3.1 Bulk Ingestion & Analytical Query Latency
Iceberg ingested the 2.88M-row dataset in **45.6s** (63,199 rows/s), outpacing Delta Lake (57.0s; 50,505 rows/s) by 20% and Hudi (103.2s; 27,912 rows/s) by 2.26x. Iceberg's initial on-disk footprint (**106 MB**) compressed tighter than the raw source Parquet (137.8 MB), whereas Delta Lake (147 MB) and Hudi (185 MB) incurred initial write overhead.

Across the five analytical queries (median of 3 warm runs), Iceberg achieved the lowest latency on every query, earning a normalized query score of **5.0/5.0** compared to Hudi's **3.6/5.0** and Delta Lake's **2.5/5.0**:

| Query Workload | Apache Iceberg | Delta Lake | Apache Hudi | Iceberg Speedup vs. Slowest |
| :--- | :---: | :---: | :---: | :---: |
| **Q1: Full Scan Aggregation** | **2.33s** | 3.39s | 3.60s | 1.55x faster than Hudi |
| **Q2: One-Month Partition Prune** | **0.248s** | 1.19s | 0.252s | 4.80x faster than Delta |
| **Q3: Composite Key Point Lookup** | **0.62s** | 1.60s | 1.51s | 2.58x faster than Delta |
| **Q4: Star Join (TPC-DS Q3)** | **2.24s** | 3.07s | 2.31s | 1.37x faster than Delta |
| **Q5: Top-100 Customers Ranking** | **1.42s** | 2.76s | 2.52s | 1.94x faster than Delta |

Iceberg's advantage in partition pruning and point lookups stems from its hierarchical manifest list and manifest file architecture, which stores column-level lower/upper bounds at the file level and enables aggressive metadata pruning prior to task scheduling.

### 3.2 Upsert (MERGE) Throughput & Write Amplification
In Copy-on-Write mode, modifying a single row requires rewriting the entire Parquet file containing that record. Iceberg won all 6 upsert scenarios, running **1.2x to 3.7x faster** than Delta Lake and Hudi while writing the fewest new bytes:

* **Recent Partitions (Latest 2 Months):**
  * `0.1%` batch: Iceberg **13.6s** vs. Hudi 25.6s vs. Delta Lake 32.0s
  * `1.0%` batch: Iceberg **7.2s** vs. Hudi 17.7s vs. Delta Lake 21.8s
  * `5.0%` batch: Iceberg **8.6s** vs. Delta Lake 23.7s vs. Hudi 24.8s
* **Scattered Across 5 Years (All 60 Partitions):**
  * `0.1%` batch: Iceberg **75.0s** vs. Delta Lake 90.0s vs. Hudi 158.0s
  * `1.0%` batch: Iceberg **51.0s** vs. Delta Lake 81.0s vs. Hudi 178.0s
  * `5.0%` batch: Iceberg **48.8s** vs. Delta Lake 80.5s vs. Hudi 177.8s

Scattered updates wrote between 134 MB and 197 MB of rewritten data compared to 5–12 MB for recent updates, confirming that partition-wide rewrite amplification dominates CoW mutation cost. Hudi's CoW upsert latency plateaued near ~178s on scattered edits due to index lookup and file-sizing overhead.

### 3.3 Compaction, Storage Footprint & Cloud Cost Extrapolation
Following 20 micro-commits, Delta Lake executed `OPTIMIZE` in **1.8s**, twice as fast as Iceberg's `rewrite_data_files` (**3.6s**) and 22x faster than Hudi's `run_clustering` (**39.9s**). However, at Scale Factor 1 (~1.8 MB per monthly partition), total data file counts did not decrease in any format (Iceberg: 294 → 294; Delta: 358 → 358; Hudi: 588 → 618) because individual partitions were already smaller than the 128 MB target file size threshold.

After completing all mutation stages and running retention cleanup, disk footprints varied substantially:
* **Delta Lake:** **165 MB** (`1.20x` raw Parquet) — projected **$282/month** at 10 TB on S3.
* **Apache Iceberg:** **270 MB** (`1.96x` raw Parquet) — projected **$460/month** at 10 TB on S3.
* **Apache Hudi:** **624 MB** (`4.53x` raw Parquet) — projected **$1,066/month** at 10 TB on S3.

### 3.4 Time Travel, Schema Evolution & ML Audit Reproducibility
* **Time Travel Speed:** Iceberg queried historical versions in **1.1s** (by snapshot ID) and **0.5s** (by timestamp), outperforming Hudi (11.4s / 4.9s) and Delta Lake (14.7s / 9.5s) by up to 19x.
* **Schema Evolution:** All three formats passed 4/4 DDL operations (`ADD`, `RENAME`, `WIDEN`, `DROP`). Notably, querying a pre-DDL snapshot returned the historical column list on Delta Lake, whereas Iceberg and Hudi projected the current schema onto historical reads unless columns were explicitly selected.
* **ML Training Set Reproducibility (Audit Stage):**
  * **Apache Iceberg (PASS):** A named tag (`training_run_2026_01`) was created at $t_0$ pinning the 1,692,986-row training snapshot. After upserts, deletes, compaction, and `expire_snapshots`, Iceberg deterministically reproduced the exact `xxhash64` fingerprint. Iceberg also exposed the richest audit metadata (18 fields).
  * **Delta Lake (FAIL AFTER CLEANUP):** Open-source Delta Lake lacks native named snapshot tags. While pre-cleanup replay succeeded (15 audit fields exposed), executing `VACUUM 0` purged the unpinned Parquet files, causing post-cleanup replay to fail.
  * **Apache Hudi (INCONCLUSIVE / DID NOT FINISH):** Hudi's audit script failed mid-run during Spark SQL procedure execution (`show_commits` / `run_clean`), preventing verification of post-cleanup savepoint survival.

---

## 4. Weighted Scorecard & Decision Matrix

To align technical selection with enterprise governance requirements, criteria were weighted on a 1–8 scale prioritizing auditability, portability, and query/upsert performance:

| Criterion | Weight ($w$) | Apache Iceberg | Delta Lake | Apache Hudi | Basis |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Audit / Reproducibility** | 8 | **5.0** | 4.5 | 3.0 | Rubric + Measured Replay |
| **Schema Evolution** | 6 | **5.0** | 4.0 | 4.0 | Rubric + Measured DDL |
| **Query Performance** | 6 | **5.0** | 2.5 | 3.6 | Measured Warm Median |
| **Ecosystem / Portability** | 6 | **5.0** | 4.0 | 3.0 | Architectural Rubric |
| **Upsert / CDC Performance** | 5 | **5.0** | 2.7 | 1.9 | Measured MERGE Latency |
| **Storage Efficiency** | 4 | 3.1 | **5.0** | 1.3 | Measured Post-Cleanup Size |
| **Operational Simplicity** | 4 | 3.0 | **5.0** | 2.0 | Architectural Rubric |
| **Weighted Total (out of 100)** | — | **91.9** | **78.2** | **59.5** | **Iceberg Recommended** |

Sensitivity analysis confirms that **Apache Iceberg remains #1 even under equal weighting** (~4.4/5.0 vs. Delta Lake 4.0/5.0 vs. Hudi 2.7/5.0).

---

## 5. Limitations, Risks & 90-Day Production Rollout Plan

### 5.1 Experimental Caveats
1. **Small Scale & Local Storage:** Scale Factor 1 (~138 MB) on a single 2-vCPU node does not capture distributed S3/ADLS object-store list/commit latency, network shuffle bottlenecks, or multi-gigabyte partition compaction dynamics.
2. **Copy-on-Write Only:** Merge-on-Read (MoR) and Delta Deletion Vectors were disabled. Workloads dominated by high-frequency streaming CDC upserts should re-evaluate Hudi MoR and Delta Deletion Vectors before finalizing architecture.
3. **Single-Run Variance:** Aside from warm queries (median of 3 runs), operational stages executed once per format; sub-second differences (e.g., 0.248s vs. 0.252s) represent practical ties.

### 5.2 Recommended 90-Day Rollout Plan (Apache Iceberg)
* **Days 0–30 (Pilot & Shadow Validation):** Migrate one core fact table (`store_sales`) and one dimension table (`item`). Dual-write from existing ingestion pipelines and validate daily row counts and `xxhash64` content fingerprints.
* **Days 30–60 (Operational Hardening):** Automate Iceberg maintenance procedures (`rewrite_data_files`, `rewrite_manifests`, and `expire_snapshots`) via scheduled orchestration. Monitor small-file ratios, manifest sizes, and optimistic concurrency commit retries.
* **Days 60–90 (Cutover & ML Governance Enforcement):** Cut over BI dashboards and feature engineering pipelines to Iceberg tables, freeze legacy tables read-only, and enforce automated snapshot tagging (`CREATE TAG`) prior to every ML model training run.

### 5.3 Mandatory ML Audit Policy (Format-Agnostic)
1. **Retention Window:** Configure default snapshot retention strictly longer than the regulatory audit horizon (e.g., 13+ months).
2. **Immutable Training Manifest:** Log table identifier, snapshot/version ID, commit timestamp, row count, and `xxhash64` content fingerprint in the ML experiment tracker for every training run.
3. **Explicit Snapshot Pinning:** Pin every training snapshot using native tags (Iceberg) or savepoints (Hudi) prior to running garbage collection, and schedule expiration jobs strictly after compaction completes.
