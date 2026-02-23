import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

conf = SparkConf().setAppName("ProcessStockPrices").setMaster("spark://spark-master:7077")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

raw_df = spark.read.parquet(HDFS_NAMENODE + "/data/raw/stock_prices/")

df = raw_df \
    .withColumnRenamed("date", "trade_date") \
    .withColumn("year", F.year(F.col("trade_date"))) \
    .withColumn("month", F.month(F.col("trade_date"))) \
    .withColumn("quarter", F.quarter(F.col("trade_date"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("trade_date"))) \
    .withColumnRenamed("open", "open_price") \
    .withColumnRenamed("high", "high_price") \
    .withColumnRenamed("low", "low_price") \
    .withColumnRenamed("close", "close_price") \
    .withColumnRenamed("adjusted_close", "adjusted_close_price") \
    .filter(
        F.col("trade_date").isNotNull() &
        (F.col("open_price") > 0) &
        (F.col("high_price") > 0) &
        (F.col("low_price") > 0) &
        (F.col("close_price") > 0) &
        F.col("volume").isNotNull() &
        (F.col("volume") >= 0) &
        (F.col("high_price") >= F.col("low_price"))
    )

ticker_exchanges_df = df \
    .select("ticker", "source_exchange") \
    .distinct() \
    .withColumn("exchange_id",
        F.when(F.col("source_exchange") == "nasdaq", 1)
         .when(F.col("source_exchange") == "nyse", 2)
         .when(F.col("source_exchange") == "sp500", 3)
         .when(F.col("source_exchange") == "forbes2000", 4)
         .cast(IntegerType())
    ) \
    .drop("source_exchange")

ticker_exchanges_df.write \
    .mode("overwrite") \
    .parquet(HDFS_NAMENODE + "/data/processed/ticker_exchanges/")


deduped_df = df \
    .select(
        "ticker", "trade_date", "year", "month", "quarter", "day_of_week",
        "open_price", "high_price", "low_price", "close_price", "adjusted_close_price", "volume"
    ) \
    .dropDuplicates(["ticker", "trade_date"]) \
    .withColumn("daily_range", F.col("high_price") - F.col("low_price")) \
    .withColumn("daily_change", F.col("close_price") - F.col("open_price")) \
    .withColumn("daily_change_pct",
        F.when(F.col("open_price") > 0,
            ((F.col("close_price") - F.col("open_price")) / F.col("open_price")) * 100
        ).otherwise(None).cast(DecimalType(10, 6))
    )

deduped_df.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .parquet(HDFS_NAMENODE + "/data/processed/stock_prices/")

spark.stop()
