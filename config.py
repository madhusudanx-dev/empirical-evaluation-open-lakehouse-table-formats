"""Central benchmark configuration. Override with environment variables."""
import os
SF            = int(os.getenv("TPCDS_SF", "1"))                  # 1 .. 1000
ROOT          = os.getenv("BENCH_ROOT", os.path.abspath("./bench_data"))   # local path or s3a://bucket/prefix
DATA_DIR      = f"{ROOT}/tpcds_sf{SF}/source"                    # raw TPC-DS parquet (written by 00_generate_data.py)
WAREHOUSE     = f"{ROOT}/tpcds_sf{SF}/warehouse"                 # one sub-dir per format
RESULTS_DIR   = os.path.abspath("./results/raw")
FORMATS       = ["iceberg", "delta", "hudi"]
TARGET_FILE_BYTES = 128 * 1024 * 1024                            # same target file size for all formats
TABLE_MODE    = os.getenv("TABLE_MODE", "cow")                   # cow | mor  (copy-on-write vs merge-on-read where supported)
SPARK_MASTER  = os.getenv("SPARK_MASTER", "local[*]")
DRIVER_MEM    = os.getenv("DRIVER_MEM", "8g")
WARM_RUNS     = 3                                                # + 1 cold run per query
UPSERT_FRACTIONS = [0.001, 0.01, 0.05]                           # share of table rows per batch
UPSERT_SCENARIOS = ["recent", "scattered"]                       # late-arriving corrections vs. random history edits
FRAGMENT_BATCHES = 20                                            # tiny commits used to create small files before compaction
S3_USD_PER_GB_MONTH = 0.023                                      # storage price used for the cost model
KEY           = ["ss_item_sk", "ss_ticket_number"]               # business key (order line)
PARTITION_COL = "sold_ym"                                        # derived YYYY-MM  -> ~60 partitions over 5 years
SHUFFLE_PARTS = os.getenv("SHUFFLE_PARTS", "8")                  # lower (e.g. 8) for small local runs
