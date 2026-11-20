"""Storage cost after the whole run, before/after expiring history."""
from common import *
fmt = cli().format; spark = get_spark(fmt); t = get_table(spark, fmt)
gb = lambda x: x / 1024**3
b0, m0, src = t.size(), t.meta(), dir_bytes(spark, f"{C.DATA_DIR}/store_sales")
with timed() as r: t.expire()
b1 = t.size()
record(fmt, "storage", "bytes_with_history", b0, "bytes", metadata_bytes=m0)
record(fmt, "storage", "bytes_after_expire", b1, "bytes", expire_seconds=r["seconds"])
record(fmt, "storage", "overhead_vs_source_parquet", round(b1 / src, 3), "x")
record(fmt, "storage", "usd_per_month_with_history", round(gb(b0) * C.S3_USD_PER_GB_MONTH, 4), "USD")
record(fmt, "storage", "usd_per_month_after_expire", round(gb(b1) * C.S3_USD_PER_GB_MONTH, 4), "USD")
