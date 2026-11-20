"""Spark session factory + one thin adapter per table format so every test runs the *same* logic."""
import os, sys, json, time, datetime as dt, argparse
from contextlib import contextmanager
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as C
from pyspark.sql import SparkSession, functions as F

PKGS = {
    "iceberg": "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
    "delta":   "io.delta:delta-spark_2.12:3.2.0",
    "hudi":    "org.apache.hudi:hudi-spark3.5-bundle_2.12:0.15.0",
}
VERSIONS = {"iceberg": "1.6.1", "delta": "3.2.0", "hudi": "0.15.0", "spark": "3.5.3"}
COND = " AND ".join(f"t.{k}=s.{k}" for k in C.KEY)

def get_spark(fmt):
    b = (SparkSession.builder.appName(f"bench-{fmt}").master(C.SPARK_MASTER)
         .config("spark.jars.packages", PKGS[fmt])
         .config("spark.driver.memory", C.DRIVER_MEM)
         .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.shuffle.partitions", C.SHUFFLE_PARTS))
    if fmt == "iceberg":
        b = (b.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
              .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
              .config("spark.sql.catalog.ice.type", "hadoop")
              .config("spark.sql.catalog.ice.warehouse", f"{C.WAREHOUSE}/iceberg")
              .config("spark.sql.catalog.ice.cache-enabled", "false"))
    elif fmt == "delta":
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
              .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
              .config("spark.databricks.delta.optimize.maxFileSize", str(C.TARGET_FILE_BYTES)))
    elif fmt == "hudi":
        b = (b.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
              .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
              .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
              .config("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar"))
    s = b.getOrCreate(); s.sparkContext.setLogLevel("WARN"); return s

@contextmanager
def timed():
    r = {}; t = time.perf_counter()
    yield r
    r["seconds"] = round(time.perf_counter() - t, 3)

def record(fmt, test, metric, value, unit="", **detail):
    os.makedirs(C.RESULTS_DIR, exist_ok=True)
    row = {"format": fmt, "test": test, "metric": metric, "value": value, "unit": unit, "detail": detail,
           "sf": C.SF, "mode": C.TABLE_MODE, "versions": VERSIONS, "ts": dt.datetime.utcnow().isoformat() + "Z"}
    with open(f"{C.RESULTS_DIR}/{test}_{fmt}.jsonl", "a") as f: f.write(json.dumps(row, default=str) + "\n")
    print(f"[{fmt}] {test}.{metric} = {value} {unit}")

def state_path(fmt): return os.path.join(C.RESULTS_DIR, "..", f"state_{fmt}.json")
def state_get(fmt):
    p = state_path(fmt); return json.load(open(p)) if os.path.exists(p) else {}
def state_put(fmt, **kw):
    s = state_get(fmt); s.update(kw); json.dump(s, open(state_path(fmt), "w"), default=str)

def _fs(spark, path):
    p = spark._jvm.org.apache.hadoop.fs.Path(path); return p, p.getFileSystem(spark._jsc.hadoopConfiguration())
def dir_bytes(spark, path):
    p, fs = _fs(spark, path); return fs.getContentSummary(p).getLength() if fs.exists(p) else 0
def data_files(spark, path):
    p, fs = _fs(spark, path); it = fs.listFiles(p, True); n = 0
    while it.hasNext():
        s = it.next().getPath().toString()
        if s.endswith(".parquet") and "/.hoodie/" not in s and "/metadata/" not in s: n += 1
    return n
def meta_bytes(spark, path):
    return sum(dir_bytes(spark, f"{path}/{d}") for d in ("metadata", "_delta_log", ".hoodie"))

def fingerprint(df):
    """Order-independent content hash of a training window: row count + sum of per-row xxhash64."""
    r = df.select(F.count("*").alias("c"),
                  F.sum(F.xxhash64("ss_item_sk", "ss_ticket_number", "ss_quantity", "ss_net_paid").cast("decimal(38,0)")).alias("h")).first()
    return {"rows": r["c"], "hash": str(r["h"])}

class Base:
    def __init__(s, spark, fmt):
        s.spark, s.fmt = spark, fmt
        s.path = f"{C.WAREHOUSE}/{fmt}/store_sales"
    def root(s): return f"{C.WAREHOUSE}/iceberg/bench/store_sales" if s.fmt == "iceberg" else s.path
    def size(s): return dir_bytes(s.spark, s.root())
    def files(s): return data_files(s.spark, s.root())
    def meta(s): return meta_bytes(s.spark, s.root())
    def view(s, name="fact", **tt):
        s.read(**tt).createOrReplaceTempView(name); return name

class Iceberg(Base):
    ID = "ice.bench.store_sales"
    def write_initial(s, df):
        s.spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.bench"); s.spark.sql(f"DROP TABLE IF EXISTS {s.ID} PURGE")
        mode = "merge-on-read" if C.TABLE_MODE == "mor" else "copy-on-write"
        (df.sortWithinPartitions(C.PARTITION_COL).writeTo(s.ID).using("iceberg").partitionedBy(F.col(C.PARTITION_COL))
           .tableProperty("format-version", "2").tableProperty("write.target-file-size-bytes", str(C.TARGET_FILE_BYTES))
           .tableProperty("write.update.mode", mode).tableProperty("write.merge.mode", mode).tableProperty("write.delete.mode", mode)
           .tableProperty("write.spark.fanout.enabled", "true").create())
    def read(s, ref=None, ts_ms=None):
        r = s.spark.read
        if ref is not None: r = r.option("snapshot-id", ref)
        if ts_ms is not None: r = r.option("as-of-timestamp", ts_ms)
        return r.table(s.ID)
    def upsert(s, src):
        src.createOrReplaceTempView("src")
        s.spark.sql(f"MERGE INTO {s.ID} t USING src s ON {COND} WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
    def delete(s, rows):
        rows.createOrReplaceTempView("src")
        s.spark.sql(f"MERGE INTO {s.ID} t USING src s ON {COND} WHEN MATCHED THEN DELETE")
    def compact(s):
        s.spark.sql(f"CALL ice.system.rewrite_data_files(table => 'bench.store_sales', options => map('min-input-files','2','target-file-size-bytes','{C.TARGET_FILE_BYTES}'))")
        s.spark.sql("CALL ice.system.rewrite_manifests('bench.store_sales')")
    def expire(s):
        now = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        s.spark.sql(f"CALL ice.system.expire_snapshots(table => 'bench.store_sales', older_than => TIMESTAMP '{now}', retain_last => 1)")
    def history(s):
        rows = s.spark.sql(f"SELECT snapshot_id ref, CAST(unix_millis(committed_at) AS LONG) ts_ms, operation FROM {s.ID}.snapshots ORDER BY committed_at").collect()
        return [r.asDict() for r in rows]
    def audit_fields(s):
        return sorted(s.spark.sql(f"SELECT summary FROM {s.ID}.snapshots LIMIT 1").first()["summary"].keys()) + ["parent_id", "manifest_list", "named tags/branches (refs table)"]
    def tag(s, name): s.spark.sql(f"ALTER TABLE {s.ID} CREATE OR REPLACE TAG `{name}`")
    def ddl(s, op):
        q = {"add": f"ALTER TABLE {s.ID} ADD COLUMN loyalty_tier STRING",
             "rename": f"ALTER TABLE {s.ID} RENAME COLUMN ss_coupon_amt TO ss_coupon_amount",
             "widen": f"ALTER TABLE {s.ID} ALTER COLUMN ss_quantity TYPE BIGINT",
             "drop": f"ALTER TABLE {s.ID} DROP COLUMN ss_ext_tax"}[op]
        s.spark.sql(q)

class Delta(Base):
    def tbl(s): return f"delta.`{s.path}`"
    def write_initial(s, df):
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy(C.PARTITION_COL).save(s.path)
        if C.TABLE_MODE == "mor":
            s.spark.sql(f"ALTER TABLE {s.tbl()} SET TBLPROPERTIES ('delta.enableDeletionVectors'='true')")
    def read(s, ref=None, ts_ms=None):
        r = s.spark.read.format("delta")
        if ref is not None: r = r.option("versionAsOf", ref)
        if ts_ms is not None: r = r.option("timestampAsOf", dt.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3])
        return r.load(s.path)
    def upsert(s, src):
        from delta.tables import DeltaTable
        (DeltaTable.forPath(s.spark, s.path).alias("t").merge(src.alias("s"), COND)
            .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
    def delete(s, rows):
        from delta.tables import DeltaTable
        DeltaTable.forPath(s.spark, s.path).alias("t").merge(rows.alias("s"), COND).whenMatchedDelete().execute()
    def compact(s):
        from delta.tables import DeltaTable
        DeltaTable.forPath(s.spark, s.path).optimize().executeCompaction()
    def expire(s):
        from delta.tables import DeltaTable
        DeltaTable.forPath(s.spark, s.path).vacuum(0)
    def history(s):
        rows = s.spark.sql(f"DESCRIBE HISTORY {s.tbl()}").selectExpr("version ref", "CAST(unix_millis(timestamp) AS LONG) ts_ms", "operation").orderBy("version").collect()
        return [r.asDict() for r in rows]
    def audit_fields(s): return s.spark.sql(f"DESCRIBE HISTORY {s.tbl()}").columns
    def tag(s, name): pass
    def ddl(s, op):
        t = s.tbl()
        if op in ("rename", "drop"):
            s.spark.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('delta.columnMapping.mode'='name','delta.minReaderVersion'='2','delta.minWriterVersion'='5')")
        q = {"add": f"ALTER TABLE {t} ADD COLUMNS (loyalty_tier STRING)",
             "rename": f"ALTER TABLE {t} RENAME COLUMN ss_coupon_amt TO ss_coupon_amount",
             "widen": f"ALTER TABLE {t} ALTER COLUMN ss_quantity TYPE BIGINT",
             "drop": f"ALTER TABLE {t} DROP COLUMN ss_ext_tax"}[op]
        s.spark.sql(q)

class Hudi(Base):
    TBL = "hudi_fact"
    def reg(s):
        s.spark.sql(f"CREATE TABLE IF NOT EXISTS {s.TBL} USING hudi LOCATION '{s.path}'"); return s.TBL
    def opts(s, op):
        return {"hoodie.table.name": "store_sales", "hoodie.datasource.write.recordkey.field": ",".join(C.KEY),
                "hoodie.datasource.write.partitionpath.field": C.PARTITION_COL,
                "hoodie.datasource.write.precombine.field": "updated_at",
                "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator",
                "hoodie.datasource.write.table.type": "MERGE_ON_READ" if C.TABLE_MODE == "mor" else "COPY_ON_WRITE",
                "hoodie.datasource.write.operation": op, "hoodie.parquet.max.file.size": str(C.TARGET_FILE_BYTES),
                "hoodie.cleaner.commits.retained": "1000", "hoodie.clean.automatic": "false", "hoodie.metadata.enable": "true",
                "hoodie.upsert.shuffle.parallelism": C.SHUFFLE_PARTS, "hoodie.insert.shuffle.parallelism": C.SHUFFLE_PARTS,
                "hoodie.bulkinsert.shuffle.parallelism": C.SHUFFLE_PARTS, "hoodie.delete.shuffle.parallelism": C.SHUFFLE_PARTS,
                "hoodie.schema.on.read.enable": "true", "hoodie.datasource.write.reconcile.schema": "true"}
    def write_initial(s, df):
        df.write.format("hudi").options(**s.opts("bulk_insert")).mode("overwrite").save(s.path)
    def read(s, ref=None, ts_ms=None):
        r = s.spark.read.format("hudi")
        if ref is not None: r = r.option("as.of.instant", ref)
        if ts_ms is not None: r = r.option("as.of.instant", dt.datetime.utcfromtimestamp(ts_ms / 1000).strftime("%Y%m%d%H%M%S%f")[:-3])
        return r.load(s.path)
    def upsert(s, src): src.write.format("hudi").options(**s.opts("upsert")).mode("append").save(s.path)
    def delete(s, rows): rows.write.format("hudi").options(**s.opts("delete")).mode("append").save(s.path)
    def compact(s):
        if C.TABLE_MODE == "mor": s.spark.sql(f"call run_compaction(op => 'scheduleandexecute', path => '{s.path}')")
        s.spark.sql(f"call run_clustering(path => '{s.path}', op => 'scheduleandexecute', options => 'hoodie.clean.automatic=false')")
    def expire(s):
        s.spark.sql(f"call run_clean(table => '{s.reg()}', clean_policy => 'KEEP_LATEST_COMMITS', retain_commits => 1)")
    def history(s):
        rows = s.spark.sql(f"call show_commits(table => '{s.reg()}', limit => 10000)").collect()
        out = [{"ref": r["commit_time"], "ts_ms": int(dt.datetime.strptime(r["commit_time"][:17], "%Y%m%d%H%M%S%f").replace(tzinfo=dt.timezone.utc).timestamp() * 1000), "operation": "commit"} for r in rows]
        return sorted(out, key=lambda x: x["ref"])
    def audit_fields(s): return s.spark.sql(f"call show_commits(table => '{s.reg()}', limit => 1)").columns + ["extraMetadata", "savepoints"]
    def tag(s, name): pass
    def ddl(s, op):
        s.reg()
        s.spark.sql("SET hoodie.schema.on.read.enable=true")
        q = {"add": "ALTER TABLE hudi_fact ADD COLUMNS (loyalty_tier STRING)",
             "rename": "ALTER TABLE hudi_fact RENAME COLUMN ss_coupon_amt TO ss_coupon_amount",
             "widen": "ALTER TABLE hudi_fact ALTER COLUMN ss_quantity TYPE BIGINT",
             "drop": "ALTER TABLE hudi_fact DROP COLUMN ss_ext_tax"}[op]
        s.spark.sql(q)

def get_table(spark, fmt): return {"iceberg": Iceberg, "delta": Delta, "hudi": Hudi}[fmt](spark, fmt)

def cli():
    ap = argparse.ArgumentParser(); ap.add_argument("--format", required=True, choices=C.FORMATS); return ap.parse_args()

def make_batch(tbl, total_rows, fraction, scenario, seed, offset):
    """Half updates (existing keys, changed price) + half inserts (new keys). Materialised before timing."""
    base = tbl.read()
    base = base.drop(*[c for c in base.columns if c.startswith("_hoodie_")])
    if scenario == "recent":
        top = sorted(r[0] for r in base.select(C.PARTITION_COL).distinct().collect())[-2:]
        base = base.where(F.col(C.PARTITION_COL).isin(top))
    n = max(base.count(), 1)
    half = min(1.0, (fraction * total_rows / 2) / n)
    upd = base.sample(False, half, seed).withColumn("ss_net_paid", F.round(F.col("ss_net_paid") * 1.01, 2)).withColumn("updated_at", F.current_timestamp())
    ins = (base.sample(False, half, seed + 1).withColumn("ss_ticket_number", F.col("ss_ticket_number") + F.lit(10_000_000_000 + offset))
               .withColumn("updated_at", F.current_timestamp()))
    b = upd.unionByName(ins).persist(); return b, b.count()
