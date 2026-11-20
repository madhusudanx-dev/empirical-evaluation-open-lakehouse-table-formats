# Open Table Format Benchmark: Apache Iceberg vs. Delta Lake vs. Apache Hudi

A reproducible, containerized benchmarking suite evaluating **Apache Iceberg (v1.6.1)**, **Delta Lake (v3.2.0)**, and **Apache Hudi (v0.15.0)** on **Apache Spark 3.5.3** across 8 core data engineering and ML governance workloads.

## Architecture Diagram

```text
+-----------------------------------------------------------------------------+
|                        BENCHMARK ORCHESTRATOR                               |
|                (run_benchmark.sh / Colab Notebook Runner)                   |
+--------------------------------------+--------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
|                     DATA GENERATION LAYER (DuckDB)                          |
|  - Generates TPC-DS Retail Dataset (SF=1, ~2.88M rows, 5 years, 60 months)  |
|  - Outputs Snappy-compressed Parquet baseline (137.8 MB)                    |
+--------------------------------------+--------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
|                ISOLATED SPARK 3.5.3 EXECUTION SESSIONS                      |
|  (2 vCPUs, 12 GB RAM, 128 MB Target File Size, Copy-on-Write Mode)          |
|                                                                             |
|  +-----------------------+  +-----------------------+  +-----------------+  |
|  |   Apache Iceberg      |  |     Delta Lake        |  |   Apache Hudi   |  |
|  |       v1.6.1          |  |       v3.2.0          |  |     v0.15.0     |  |
|  | (Hadoop Catalog +     |  | (Delta Catalog +      |  | (Hoodie Catalog |  |
|  |  Named Snapshot Tags) |  |  Transaction Log)     |  |  + Timeline)    |  |
|  +-----------+-----------+  +-----------+-----------+  +--------+--------+  |
+--------------|--------------------------|-----------------------|-----------+
               |                          |                       |
               v                          v                       v
+-----------------------------------------------------------------------------+
|                        8-STAGE BENCHMARK SUITE                              |
|  [01] Bulk Load        [02] 5 Analytical Queries   [03] 6 Upsert Scenarios  |
|  [04] Compaction       [05] Storage & Cost Model   [06] Time Travel         |
|  [07] Schema Evolution [08] ML Training Set Audit & Post-Cleanup Replay     |
+--------------------------------------+--------------------------------------+
                                       |
                                       v
+-----------------------------------------------------------------------------+
|                     AGGREGATION & REPORTING LAYER                           |
|  - JSONL Raw Metrics -> summary.json & comparison.csv                       |
|  - Weighted Audit-First Scorecard & Automated Recommendation Engine         |
+-----------------------------------------------------------------------------+
```

## Repository Structure

```text
lakehouse-benchmark-submission/
├── README.md                  # Architecture overview and execution instructions
├── Dockerfile                 # Reproducible Spark 3.5.3 + Java 17 + Python 3.11 image
├── docker-compose.yml         # Resource-bounded container orchestration
├── requirements.txt           # Pinned Python dependencies
├── run_benchmark.sh           # End-to-end automated execution script
├── config.py                  # Central benchmark parameters (SF, modes, keys)
├── scripts/
│   ├── common.py              # Spark session factory & format-specific adapters
│   ├── 00_generate_data.py    # DuckDB TPC-DS Parquet generator
│   ├── 01_load.py             # Bulk ingestion benchmark
│   ├── 02_queries.py          # 5 analytical queries (1 cold + 3 warm runs)
│   ├── 03_upserts.py          # 6 MERGE scenarios (recent vs. scattered)
│   ├── 04_compaction.py       # Micro-commit fragmentation & file compaction
│   ├── 05_storage.py          # Disk footprint & S3 cost modeling
│   ├── 06_time_travel_schema.py # Version/timestamp travel & DDL schema evolution
│   ├── 07_audit_repro.py      # ML training set fingerprinting & post-cleanup replay
│   └── aggregate.py           # Result aggregation into CSV, JSON, and JS
├── benchmark_results.ipynb    # Interactive results analysis & weighted scorecard notebook
├── TECHNICAL_REPORT.md        # Comprehensive technical evaluation report
└── DATA_CARD.md               # Dataset & benchmark artifact documentation card
```

## Setup & Execution Steps

### Prerequisites
* **Docker** & **Docker Compose** (v2.0+)
* Minimum **8 GB RAM** allocated to Docker (12 GB recommended) and **10 GB free disk space**.

### Quickstart (Docker Compose)
1. **Extract the archive** and navigate into the project directory:
   ```bash
   unzip lakehouse-benchmark-submission.zip
   cd lakehouse-benchmark-submission
   ```
2. **Build and run the full benchmark pipeline**:
   ```bash
   docker compose up --build
   ```
   *Note: On the first run, Spark will resolve and cache the Maven connector JARs for Iceberg, Delta Lake, and Hudi in a persistent Docker volume (`ivy_cache`).*
3. **Inspect Outputs**:
   Once complete, results are saved to `./results/`:
   * `results/comparison.csv`: Side-by-side tabular summary of all 59 measured metrics.
   * `results/summary.json`: Full structured output including error traces and metadata.
   * `site/results.js`: Formatted payload for interactive reporting.

### Local Execution (Without Docker)
If running directly on a Linux/macOS host with Java 17 and Python 3.11 installed:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TPCDS_SF=1 TABLE_MODE=cow BENCH_ROOT=./bench_data SPARK_MASTER="local[*]" DRIVER_MEM=8g SHUFFLE_PARTS=8
./run_benchmark.sh
```

### Configuration Overrides
Customize scale factor, table mode, or Spark memory in `docker-compose.yml` via environment variables:
* `TPCDS_SF`: Scale factor (`1` = ~2.88M rows / 137.8 MB; `10` = ~28.8M rows).
* `TABLE_MODE`: `cow` (Copy-on-Write) or `mor` (Merge-on-Read / Deletion Vectors).
* `DRIVER_MEM`: Spark driver heap memory (default `8g`).
* `SHUFFLE_PARTS`: Number of Spark SQL shuffle partitions (default `8` for single-node runs).
