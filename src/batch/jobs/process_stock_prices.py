import os
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]

conf = SparkConf().setAppName("ProcessStockPrices").setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

raw_df = spark.read.parquet(HDFS_NAMENODE + "/data/raw/stock_prices/")

df = raw_df \
    .withColumnRenamed("date", "trade_date") \
    .withColumn("year", F.year(F.col("trade_date"))) \
    .withColumn("month", F.month(F.col("trade_date"))) \
    .withColumn("quarter", F.quarter(F.col("trade_date"))) \
    .withColumn("day_of_week", F.dayofweek(F.col("trade_date"))) \
    .withColumn("open_price", F.col("open").cast(DoubleType())) \
    .withColumn("high_price", F.col("high").cast(DoubleType())) \
    .withColumn("low_price", F.col("low").cast(DoubleType())) \
    .withColumn("close_price", F.col("close").cast(DoubleType())) \
    .withColumn("adjusted_close_price", F.col("adjusted_close").cast(DoubleType())) \
    .drop("open", "high", "low", "close", "adjusted_close") \
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
        ).otherwise(None)
    )

deduped_df.write \
    .partitionBy("year", "month") \
    .mode("overwrite") \
    .parquet(HDFS_NAMENODE + "/data/processed/stock_prices/")

spark.sql("CREATE DATABASE IF NOT EXISTS stock_analytics")
spark.sql("USE stock_analytics")

spark.sql("DROP TABLE IF EXISTS exchange_dim")
spark.sql("""
    CREATE TABLE exchange_dim (
        exchange_id INT,
        exchange_name STRING
    )
""")
spark.sql("""
    INSERT OVERWRITE TABLE exchange_dim
    SELECT 1, 'nasdaq'
    UNION ALL SELECT 2, 'nyse'
    UNION ALL SELECT 3, 'sp500'
    UNION ALL SELECT 4, 'forbes2000'
""")

spark.sql("DROP TABLE IF EXISTS ticker_exchanges")
spark.sql(f"""
    CREATE EXTERNAL TABLE ticker_exchanges (
        ticker STRING,
        exchange_id INT
    )
    STORED AS PARQUET
    LOCATION '{HDFS_NAMENODE}/data/processed/ticker_exchanges/'
""")

spark.sql("DROP TABLE IF EXISTS stock_prices")
spark.sql(f"""
    CREATE EXTERNAL TABLE stock_prices (
        ticker STRING,
        trade_date DATE,
        quarter INT,
        day_of_week INT,
        open_price DOUBLE,
        high_price DOUBLE,
        low_price DOUBLE,
        close_price DOUBLE,
        adjusted_close_price DOUBLE,
        volume BIGINT,
        daily_range DOUBLE,
        daily_change DOUBLE,
        daily_change_pct DOUBLE
    )
    PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
    LOCATION '{HDFS_NAMENODE}/data/processed/stock_prices/'
""")
spark.sql("MSCK REPAIR TABLE stock_prices")

spark.stop()