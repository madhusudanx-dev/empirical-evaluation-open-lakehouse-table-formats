"""Query latency: 1 cold + N warm runs per query."""
import statistics
from common import *
fmt = cli().format; spark = get_spark(fmt); t = get_table(spark, fmt)
for d in ("date_dim", "item", "customer"): spark.read.parquet(f"{C.DATA_DIR}/{d}").createOrReplaceTempView(d)
t.view("fact")
pick = t.read().where("sold_ym='2001-06'").select(*C.KEY).limit(1).first()
Q = {
 "q1_full_scan_agg": "SELECT d.d_year, sum(f.ss_net_paid) s FROM fact f JOIN date_dim d ON f.ss_sold_date_sk=d.d_date_sk GROUP BY d.d_year ORDER BY 1",
 "q2_partition_prune": "SELECT count(*) c, sum(ss_net_paid) s FROM fact WHERE sold_ym='2001-06'",
 "q3_point_lookup": f"SELECT * FROM fact WHERE ss_item_sk={pick[0]} AND ss_ticket_number={pick[1]}",
 "q4_star_join_tpcds_q3": """SELECT d.d_year, i.i_brand_id, i.i_brand, sum(f.ss_ext_sales_price) s FROM fact f
        JOIN date_dim d ON f.ss_sold_date_sk=d.d_date_sk JOIN item i ON f.ss_item_sk=i.i_item_sk
        WHERE i.i_manufact_id=128 AND d.d_moy=11 GROUP BY 1,2,3 ORDER BY 1,4 DESC,2 LIMIT 100""",
 "q5_top_customers": "SELECT ss_customer_sk, sum(ss_net_paid) s FROM fact WHERE ss_customer_sk IS NOT NULL GROUP BY 1 ORDER BY s DESC LIMIT 100",
}
for name, sql in Q.items():
    runs = []
    for i in range(1 + C.WARM_RUNS):
        with timed() as r: spark.sql(sql).collect()
        runs.append(r["seconds"])
    record(fmt, "queries", f"{name}_cold", runs[0], "s")
    record(fmt, "queries", f"{name}_warm_median", statistics.median(runs[1:]), "s", runs=runs)
