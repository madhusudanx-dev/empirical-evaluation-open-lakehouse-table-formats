"""Reproducibility of a past ML training set."""
from common import *
fmt = cli().format; spark = get_spark(fmt); t = get_table(spark, fmt); total = state_get(fmt)["initial_rows"]
W = "sold_ym BETWEEN '1999-01' AND '2001-12'"
def err(e): return str(e).splitlines()[0][:300] if str(e) else repr(e)[:300]
def try_fp(label, **tt):
    try: return fingerprint(t.read(**tt).where(W)), ""
    except Exception as e: print(f"[{fmt}] {label} failed: {err(e)}"); return None, err(e)
def try_do(label, fn):
    try: fn(); return ""
    except Exception as e: print(f"[{fmt}] {label} failed: {err(e)}"); return err(e)
ref0 = t.history()[-1]; fp0 = fingerprint(t.read(ref=ref0["ref"]).where(W))
tag_err = try_do("tag", lambda: t.tag("training_run_2026_01"))
record(fmt, "audit", "fingerprint_t0", fp0["rows"], "rows", tag_error=tag_err, **fp0)
b, _ = make_batch(t, total, 0.01, "scattered", seed=900, offset=77_000_000); t.upsert(b)
d, _ = make_batch(t, total, 0.001, "scattered", seed=901, offset=0)
t.delete(d.where(F.col("ss_ticket_number") < 10_000_000_000)); b.unpersist(); d.unpersist()
compact_err = try_do("compaction", t.compact)
now, e_now = try_fp("current read")
record(fmt, "audit", "current_differs_from_t0", int(now is not None and now != fp0), "bool", now=now, error=e_now, compaction_error=compact_err)
by_ver, e_ver = try_fp("read by version", ref=ref0["ref"])
by_ts, e_ts = try_fp("read by timestamp", ts_ms=ref0["ts_ms"])
record(fmt, "audit", "reproduced_by_version", int(by_ver == fp0), "bool", error=e_ver)
record(fmt, "audit", "reproduced_by_timestamp", int(by_ts == fp0), "bool", error=e_ts)
try: fields = t.audit_fields()
except Exception as e: fields = []; print(f"[{fmt}] audit_fields failed: {err(e)}")
record(fmt, "audit", "audit_fields", len(fields), "fields", fields=fields)
exp_err = try_do("expire", t.expire)
after, e_after = try_fp("read after expire", ref=ref0["ref"])
record(fmt, "audit", "reproducible_after_expire_default", int(after == fp0), "bool", error=e_after or exp_err,
       note="unpinned history is lost on expiry; Iceberg tag (taken at t0) keeps the snapshot alive")
