"""Create small-file fragmentation with many tiny commits, then time compaction."""
from common import *
import statistics
fmt = cli().format; spark = get_spark(fmt); t = get_table(spark, fmt)
total = state_get(fmt)["initial_rows"]
for i in range(C.FRAGMENT_BATCHES):
    b, _ = make_batch(t, total, 0.0002, "recent", seed=500 + i, offset=(900 + i) * 1_000_000); t.upsert(b); b.unpersist()
def q2():
    t.view("fact"); xs = []
    for _ in range(4):
        with timed() as r: spark.sql("SELECT count(*), sum(ss_net_paid) FROM fact WHERE sold_ym >= '2002-01' AND sold_ym <= '2002-06'").collect()
        xs.append(r["seconds"])
    return statistics.median(xs[1:])
f0, b0, q0 = t.files(), t.size(), q2()
with timed() as r: t.compact()
f1, b1, q1 = t.files(), t.size(), q2()
record(fmt, "compaction", "compaction_seconds", r["seconds"], "s")
record(fmt, "compaction", "files_before", f0, "files"); record(fmt, "compaction", "files_after", f1, "files")
record(fmt, "compaction", "bytes_before", b0, "bytes"); record(fmt, "compaction", "bytes_after", b1, "bytes")
record(fmt, "compaction", "pruned_query_before", q0, "s"); record(fmt, "compaction", "pruned_query_after", q1, "s")
