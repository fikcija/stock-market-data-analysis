"""
SQ3 — Trade frequency vs baseline.
Stream-batch join: count trades per symbol in 10-min tumbling window.
Flag as high-frequency if current rate > 2x baseline.
Baseline: avg daily trade count across 7-day window, normalized to 10-min period.
Since daily snapshots don't include volume, we use a neutral baseline of
10 trades per 10-min window (1/trade per min) per symbol.
Output table: sq3_trade_frequency
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, from_json, lit, to_timestamp, window,
)
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
import os

KAFKA_BROKERS = "kafka1:29092"
JDBC_URL = "jdbc:postgresql://pg:5432/stock_analytics"
JDBC_DRIVER = "org.postgresql.Driver"

# Neutral baseline: 10 trades per 10-min window per symbol
BASELINE_TRADES_PER_WINDOW = 10

trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp_ms", LongType()),
    StructField("volume", DoubleType()),
])

spark = (
    SparkSession.builder
    .appName("SQ3 - Trade Frequency")
    .config("spark.sql.shuffle.partitions", "8")
    .config("hive.metastore.uris", os.environ["HIVE_SITE_CONF_hive_metastore_uris"])
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Load 7d aggregate to confirm symbols present in baseline data
agg_baseline = (
    spark.table("stock_analytics.streaming_7d_aggregates")
    .select(
        col("symbol"),
        col("days_count"),
    )
)

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
    .agg(count("price").alias("trade_count"))
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    from pyspark.sql.functions import broadcast

    result = (
        batch_df.join(broadcast(agg_baseline), on="symbol", how="left")
        .withColumn(
            "is_high_frequency",
            col("trade_count") > (lit(BASELINE_TRADES_PER_WINDOW) * lit(2)),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            col("trade_count"),
            col("is_high_frequency"),
        )
        .distinct()
    )

    result.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq3_trade_frequency") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print(f"[SQ3] batch {batch_id} saved", flush=True)


query = (
    windowed.writeStream
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq3")
    .start()
)

query.awaitTermination()
