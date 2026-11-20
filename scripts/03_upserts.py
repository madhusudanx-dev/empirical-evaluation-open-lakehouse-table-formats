"""Upsert (MERGE) latency + bytes written for 0.1 / 1 / 5 % batches."""
from common import *
fmt = cli().format; spark = get_spark(fmt); t = get_table(spark, fmt)
total = state_get(fmt)["initial_rows"]; k = 0
for scen in C.UPSERT_SCENARIOS:
    for frac in C.UPSERT_FRACTIONS:
        k += 1; batch, rows = make_batch(t, total, frac, scen, seed=100 + k, offset=k * 1_000_000)
        size0, files0 = t.size(), t.files()
        with timed() as r: t.upsert(batch)
        tag = f"{scen}_{frac*100:g}pct"
        record(fmt, "upserts", f"{tag}_seconds", r["seconds"], "s", batch_rows=rows)
        record(fmt, "upserts", f"{tag}_rows_per_second", round(rows / r["seconds"]), "rows/s")
        record(fmt, "upserts", f"{tag}_bytes_added", t.size() - size0, "bytes", files_added=t.files() - files0)
        batch.unpersist()
