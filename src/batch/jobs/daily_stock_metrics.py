import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

conf = SparkConf().setAppName("DailyStockMetrics").setMaster("spark://spark-master:7077")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

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

spark.stop()
