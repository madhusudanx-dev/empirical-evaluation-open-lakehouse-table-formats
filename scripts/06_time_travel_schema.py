"""Time travel by version and timestamp, then schema evolution."""
from common import *
fmt = cli().format; spark = get_spark(fmt); t = get_table(spark, fmt); st = state_get(fmt)
h = t.history()
with timed() as r: n_ver = t.read(ref=st["v0"]).count()
record(fmt, "timetravel", "by_version_ok", int(n_ver == st["initial_rows"]), "bool", rows=n_ver, expected=st["initial_rows"])
record(fmt, "timetravel", "by_version_seconds", r["seconds"], "s")
mid = h[len(h) // 2]
with timed() as r: n_ts = t.read(ts_ms=mid["ts_ms"]).count()
record(fmt, "timetravel", "by_timestamp_seconds", r["seconds"], "s", rows=n_ts, snapshots_listed=len(h))
record(fmt, "timetravel", "history_entries", len(h), "versions")
cols_before = t.read().columns
for op in ("add", "rename", "widen", "drop"):
    try:
        f0 = t.files(); b0 = t.size()
        with timed() as r: t.ddl(op)
        ok, err = 1, ""
    except Exception as e:
        ok, err, r = 0, str(e)[:300], {"seconds": None}
    record(fmt, "schema", f"{op}_ok", ok, "bool", error=err, seconds=r["seconds"], data_rewritten=bool(ok and t.size() > b0 * 1.05))
try:
    old_cols = t.read(ref=st["v0"]).columns
    record(fmt, "schema", "old_version_returns_old_schema", int(old_cols == cols_before), "bool", old=len(old_cols), now=len(t.read().columns))
except Exception as e:
    record(fmt, "schema", "old_version_returns_old_schema", 0, "bool", error=str(e)[:300])
