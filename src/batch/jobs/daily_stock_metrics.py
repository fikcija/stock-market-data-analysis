import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("DailyStockMetrics").setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

df = spark.read.parquet(HDFS_NAMENODE + "/data/processed/stock_prices/")

window_7d = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-6, 0)
window_20d = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-20, -1)
window_30d = Window.partitionBy("ticker").orderBy("trade_date").rowsBetween(-29, 0)
window_prev = Window.partitionBy("ticker").orderBy("trade_date")

daily_df = df \
    .withColumn("prev_close_price", F.lag("close_price", 1).over(window_prev)) \
    .withColumn("avg_volume_20d", F.round(F.avg("volume").over(window_20d), 0).cast(LongType())) \
    .withColumn("avg_volume_7d", F.round(F.avg("volume").over(window_7d), 0).cast(LongType())) \
    .withColumn("rolling_high_7d", F.max("high_price").over(window_7d)) \
    .withColumn("rolling_low_7d", F.min("low_price").over(window_7d)) \
    .withColumn("volume_ratio",
        F.when(F.col("avg_volume_20d") > 0,
            F.round(F.col("volume") / F.col("avg_volume_20d"), 4)
        ).otherwise(None)
    ) \
    .withColumn("is_volume_spike", F.col("volume_ratio") > 2.0) \
    .withColumn("is_significant_drop", F.col("daily_change_pct") < -10) \
    .withColumn("rolling_volatility_30d", F.round(F.stddev("daily_change_pct").over(window_30d), 6)) \
    .select(
        "ticker", "trade_date", "year",
        "open_price", "high_price", "low_price", "close_price", "adjusted_close_price",
        "volume", "daily_change_pct", "prev_close_price",
        "avg_volume_20d", "avg_volume_7d", "volume_ratio",
        "is_volume_spike", "is_significant_drop",
        "rolling_volatility_30d", "rolling_high_7d", "rolling_low_7d"
    )

daily_df.write \
    .partitionBy("year") \
    .mode("overwrite") \
    .parquet(HDFS_NAMENODE + "/data/processed/daily_stock_metrics/")

spark.sql("USE stock_analytics")
spark.sql("DROP TABLE IF EXISTS daily_stock_metrics")
spark.sql(f"""
    CREATE EXTERNAL TABLE daily_stock_metrics (
        ticker STRING,
        trade_date DATE,
        open_price DECIMAL(18,6),
        high_price DECIMAL(18,6),
        low_price DECIMAL(18,6),
        close_price DECIMAL(18,6),
        adjusted_close_price DECIMAL(18,6),
        volume BIGINT,
        daily_change_pct DECIMAL(10,6),
        prev_close_price DECIMAL(18,6),
        avg_volume_20d BIGINT,
        avg_volume_7d BIGINT,
        volume_ratio DOUBLE,
        is_volume_spike BOOLEAN,
        is_significant_drop BOOLEAN,
        rolling_volatility_30d DOUBLE,
        rolling_high_7d DECIMAL(18,6),
        rolling_low_7d DECIMAL(18,6)
    )
    PARTITIONED BY (year INT)
    STORED AS PARQUET
    LOCATION '{HDFS_NAMENODE}/data/processed/daily_stock_metrics/'
""")
spark.sql("MSCK REPAIR TABLE daily_stock_metrics")

spark.stop()