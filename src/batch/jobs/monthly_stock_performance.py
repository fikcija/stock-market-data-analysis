import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("MonthlyStockPerformance").setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

df = spark.read.parquet(HDFS_NAMENODE + "/data/processed/stock_prices/")

window_asc = Window.partitionBy("ticker", "year", "month").orderBy(F.asc("trade_date"))
window_desc = Window.partitionBy("ticker", "year", "month").orderBy(F.desc("trade_date"))

ranked_df = df \
    .withColumn("rn_asc", F.row_number().over(window_asc)) \
    .withColumn("rn_desc", F.row_number().over(window_desc))

monthly_df = ranked_df.groupBy("ticker", "year", "month").agg(
    F.min("trade_date").alias("first_trade_date"),
    F.max("trade_date").alias("last_trade_date"),
    F.count("*").alias("trading_days"),
    F.max(F.when(F.col("rn_asc") == 1, F.col("open_price"))).alias("month_open_price"),
    F.max(F.when(F.col("rn_desc") == 1, F.col("close_price"))).alias("month_close_price"),
    F.max("high_price").alias("month_high_price"),
    F.min("low_price").alias("month_low_price"),
    F.sum("volume").alias("total_volume"),
    F.round(F.avg("volume"), 0).cast("bigint").alias("avg_daily_volume"),
    F.round(F.avg("daily_range"), 6).alias("avg_intraday_range"),
    F.max("daily_range").alias("max_intraday_range"),
    F.round(F.avg(
        F.when(F.col("open_price") > 0,
            (F.col("daily_range") / F.col("open_price")) * 100
        ).otherwise(None)
    ), 4).alias("avg_intraday_range_pct"),
).withColumn(
    "monthly_return_pct",
    F.round(
        F.when(F.col("month_open_price") > 0,
            ((F.col("month_close_price") - F.col("month_open_price")) / F.col("month_open_price")) * 100
        ).otherwise(None),
        4
    )
)

monthly_df.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .parquet(HDFS_NAMENODE + "/data/processed/monthly_stock_performance/")

spark.sql("USE stock_analytics")
spark.sql("DROP TABLE IF EXISTS monthly_stock_performance")
spark.sql(f"""
    CREATE EXTERNAL TABLE monthly_stock_performance (
        ticker STRING,
        first_trade_date DATE,
        last_trade_date DATE,
        trading_days BIGINT,
        month_open_price DECIMAL(18,6),
        month_close_price DECIMAL(18,6),
        month_high_price DECIMAL(18,6),
        month_low_price DECIMAL(18,6),
        monthly_return_pct DECIMAL(10,4),
        total_volume BIGINT,
        avg_daily_volume BIGINT,
        avg_intraday_range DECIMAL(18,6),
        max_intraday_range DECIMAL(18,6),
        avg_intraday_range_pct DECIMAL(10,4)
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
    LOCATION '{HDFS_NAMENODE}/data/processed/monthly_stock_performance/'
""")
spark.sql("MSCK REPAIR TABLE monthly_stock_performance")

spark.stop()