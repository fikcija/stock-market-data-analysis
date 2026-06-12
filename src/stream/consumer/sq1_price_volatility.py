"""
SQ1 — Intraday price volatility per symbol.
Pure streaming: stddev(price) / avg(price) * 100 per symbol
in a tumbling 10-minute window.
Output table: sq1_price_volatility
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, from_json, round as spark_round,
    stddev, to_timestamp, window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
import os

KAFKA_BROKERS = "kafka1:29092"
JDBC_URL = "jdbc:postgresql://pg:5432/stock_analytics"
JDBC_DRIVER = "org.postgresql.Driver"

trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp_ms", LongType()),
    StructField("volume", DoubleType()),
])

spark = (
    SparkSession.builder
    .appName("SQ1 - Price Volatility")
    .config("spark.sql.shuffle.partitions", "8")
    .config("hive.metastore.uris", os.environ["HIVE_SITE_CONF_hive_metastore_uris"])
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

trades = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", "stock_trades")
    .option("startingOffsets", "latest")
    .load()
    .select(from_json(col("value").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp(col("timestamp_ms") / 1000))
    .withWatermark("event_time", "5 minutes")
    .filter(col("price").isNotNull() & (col("price") > 0))
)

windowed = (
    trades
    .groupBy(window("event_time", "10 minutes"), col("symbol"))
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("stddev_price"),
        count("price").alias("trade_count"),
    )
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    result = (
        batch_df
        .withColumn(
            "volatility_coeff",
            spark_round(col("stddev_price") / col("avg_price") * 100, 4),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            spark_round(col("avg_price"), 4).alias("avg_price"),
            spark_round(col("stddev_price"), 4).alias("stddev_price"),
            col("volatility_coeff"),
            col("trade_count"),
        )
        .filter(col("stddev_price").isNotNull())
        .distinct()
    )

    result.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq1_price_volatility") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print(f"[SQ1] batch {batch_id} saved", flush=True)


query = (
    windowed.writeStream
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq1")
    .start()
)

query.awaitTermination()
