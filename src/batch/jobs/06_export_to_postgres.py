import os

from pyspark import SparkConf
from pyspark.sql import SparkSession

HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("Export Batch Queries to PostgreSQL") \
    .setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)

spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

spark.sql("USE stock_analytics")

JDBC_URL = "jdbc:postgresql://pg:5432/stock_analytics"
JDBC_DRIVER = "org.postgresql.Driver"

EXPORTS = [
    ("/batch/queries/q01_top3_per_exchange_per_year.sql",           "q01_top_performers_by_exchange_year"),
    ("/batch/queries/q02_yoy_return_change.sql",                    "q02_year_over_year_return_change"),
    ("/batch/queries/q03_biggest_drop_vs_avg.sql",                  "q03_biggest_monthly_drop_vs_average"),
    ("/batch/queries/q04_biggest_3month_recovery.sql",              "q04_largest_3month_recovery"),
    ("/batch/queries/q05_cumulative_return_ranking.sql",            "q05_cumulative_return_ranking"),
    ("/batch/queries/q06_consistently_outperformed_exchange.sql",   "q06_consistent_exchange_outperformers"),
    ("/batch/queries/q07_volatility_above_annual_avg.sql",          "q07_above_average_volatility_days"),
    ("/batch/queries/q08_volume_spike_price_correlation.sql",       "q08_volume_spike_next_day_direction"),
    ("/batch/queries/q09_intraday_volatility_percentile.sql",       "q09_intraday_volatility_percentile"),
    ("/batch/queries/q10_consecutive_positive_months.sql",          "q10_longest_positive_month_streaks"),
]

for sql_file, table_name in EXPORTS:
    print(f"Running {sql_file} -> {table_name} ...")
    with open(sql_file) as f:
        query = f.read().strip()

    df = spark.sql(query)

    df.write \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", table_name) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("overwrite") \
        .save()

    print(f"  Done: {table_name} ({df.count()} rows)")

spark.stop()
