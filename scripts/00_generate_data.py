"""Generate TPC-DS parquet with DuckDB's dsdgen."""
import os, sys, duckdb
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config as C

TABLES = ["store_sales", "date_dim", "item", "customer", "store"]
os.makedirs(C.DATA_DIR, exist_ok=True)
con = duckdb.connect()
con.execute("INSTALL tpcds; LOAD tpcds;")
con.execute(f"CALL dsdgen(sf={C.SF})")
for t in TABLES:
    out = f"{C.DATA_DIR}/{t}"
    os.makedirs(out, exist_ok=True)
    con.execute(f"COPY {t} TO '{out}' (FORMAT PARQUET, PER_THREAD_OUTPUT TRUE, COMPRESSION SNAPPY)")
    print("wrote", out, con.execute(f"SELECT count(*) FROM {t}").fetchone()[0], "rows")
