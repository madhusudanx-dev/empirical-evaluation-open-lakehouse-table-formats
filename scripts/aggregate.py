"""Merge results/raw/*.jsonl -> results/summary.json, results/comparison.csv and site/results.js."""
import glob, json, os, csv, collections
root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
rows = [json.loads(l) for f in glob.glob(f"{root}/results/raw/*.jsonl") for l in open(f)]
latest = {}
for r in rows: latest[(r["format"], r["test"], r["metric"])] = r
tbl = collections.defaultdict(dict)
for (fmt, test, m), r in latest.items(): tbl[f"{test}.{m}"][fmt] = {"value": r["value"], "unit": r["unit"], "detail": r["detail"]}
out = {"metrics": tbl, "runs": len(rows), "sf": rows[0]["sf"] if rows else None, "mode": rows[0]["mode"] if rows else None,
       "versions": rows[0]["versions"] if rows else None}
json.dump(out, open(f"{root}/results/summary.json", "w"), indent=1, default=str)
with open(f"{root}/results/comparison.csv", "w", newline="") as f:
    w = csv.writer(f); w.writerow(["metric", "unit", "iceberg", "delta", "hudi"])
    for k, v in sorted(tbl.items()): w.writerow([k, next(iter(v.values()))["unit"]] + [v.get(x, {}).get("value", "") for x in ("iceberg", "delta", "hudi")])
open(f"{root}/site/results.js", "w").write("window.RESULTS = " + json.dumps(out, default=str) + ";")
print("metrics:", len(tbl), "| rows:", len(rows))
