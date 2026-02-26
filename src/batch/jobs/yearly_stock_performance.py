import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("YearlyStockPerformance").setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

df = spark.read.parquet(HDFS_NAMENODE + "/data/processed/stock_prices/")

window_asc = Window.partitionBy("ticker", "year").orderBy(F.asc("trade_date"))
window_desc = Window.partitionBy("ticker", "year").orderBy(F.desc("trade_date"))

ranked_df = df \
    .withColumn("rn_asc", F.row_number().over(window_asc)) \
    .withColumn("rn_desc", F.row_number().over(window_desc))

yearly_df = ranked_df.groupBy("ticker", "year").agg(
    F.min("trade_date").alias("first_trade_date"),
    F.max("trade_date").alias("last_trade_date"),
    F.count("*").alias("trading_days"),
    F.max(F.when(F.col("rn_asc") == 1, F.col("open_price"))).alias("year_open_price"),
    F.max(F.when(F.col("rn_desc") == 1, F.col("close_price"))).alias("year_close_price"),
    F.max("high_price").alias("year_high_price"),
    F.min("low_price").alias("year_low_price"),
    F.sum("volume").alias("total_volume"),
    F.round(F.avg("volume"), 0).cast("bigint").alias("avg_daily_volume"),
    F.round(F.avg("daily_change_pct"), 6).alias("avg_daily_return_pct"),
    F.round(F.stddev("daily_change_pct"), 6).alias("stddev_daily_return"),
    F.sum(F.when(F.col("daily_change_pct") > 0, 1).otherwise(0)).alias("positive_days"),
    F.sum(F.when(F.col("daily_change_pct") < 0, 1).otherwise(0)).alias("negative_days"),
).withColumn(
    "yearly_return_pct",
    F.round(
        F.when(F.col("year_open_price") > 0,
            ((F.col("year_close_price") - F.col("year_open_price")) / F.col("year_open_price")) * 100
        ).otherwise(None),
        4
    )
)

yearly_df.write \
    .partitionBy("year") \
    .mode("overwrite") \
    .parquet(HDFS_NAMENODE + "/data/processed/yearly_stock_performance/")

spark.sql("USE stock_analytics")
spark.sql("DROP TABLE IF EXISTS yearly_stock_performance")
spark.sql(f"""
    CREATE EXTERNAL TABLE yearly_stock_performance (
        ticker STRING,
        first_trade_date DATE,
        last_trade_date DATE,
        trading_days BIGINT,
        year_open_price DOUBLE,
        year_close_price DOUBLE,
        year_high_price DOUBLE,
        year_low_price DOUBLE,
        yearly_return_pct DOUBLE,
        total_volume BIGINT,
        avg_daily_volume BIGINT,
        avg_daily_return_pct DOUBLE,
        stddev_daily_return DOUBLE,
        positive_days BIGINT,
        negative_days BIGINT
    )
    PARTITIONED BY (year INT)
    STORED AS PARQUET
    LOCATION '{HDFS_NAMENODE}/data/processed/yearly_stock_performance/'
""")
spark.sql("MSCK REPAIR TABLE yearly_stock_performance")

spark.stop()