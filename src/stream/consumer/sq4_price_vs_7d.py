"""
SQ4 — Current price position vs 7-day high/low.
Stream-batch join: for each symbol in a 5-min window, get the latest price
and compare against max_7d_high and min_7d_low from the 7-day aggregates.
Classify as near_high / near_low / neutral (within 2%).
Output table: sq4_price_vs_7d
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, from_json, round as spark_round,
    to_timestamp, when, window,
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
    .appName("SQ4 - Price vs 7d Extremes")
    .config("spark.sql.shuffle.partitions", "8")
    .config("hive.metastore.uris", os.environ["HIVE_SITE_CONF_hive_metastore_uris"])
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Load 7-day aggregates once as static batch DF
extremes = (
    spark.table("stock_analytics.streaming_7d_aggregates")
    .select(
        col("symbol"),
        col("max_7d_high"),
        col("min_7d_low"),
    )
    .filter(
        col("max_7d_high").isNotNull()
        & col("min_7d_low").isNotNull()
        & (col("max_7d_high") > 0)
        & (col("min_7d_low") > 0)
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
    .withWatermark("event_time", "10 minutes")
    .filter(col("price").isNotNull() & (col("price") > 0))
)

windowed = (
    trades
    .groupBy(window("event_time", "5 minutes"), col("symbol"))
    .agg(avg("price").alias("current_price"))
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    from pyspark.sql.functions import broadcast

    joined = batch_df.join(broadcast(extremes), on="symbol", how="inner")

    result = (
        joined
        .withColumn(
            "pct_from_high",
            spark_round(
                (col("max_7d_high") - col("current_price")) / col("max_7d_high") * 100,
                4,
            ),
        )
        .withColumn(
            "pct_from_low",
            spark_round(
                (col("current_price") - col("min_7d_low")) / col("min_7d_low") * 100,
                4,
            ),
        )
        .withColumn(
            "position",
            when(col("pct_from_high") <= 2.0, "near_high")
            .when(col("pct_from_low") <= 2.0, "near_low")
            .otherwise("neutral"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            spark_round(col("current_price"), 4).alias("current_price"),
            spark_round(col("max_7d_high"), 4).alias("max_7d_high"),
            spark_round(col("min_7d_low"), 4).alias("min_7d_low"),
            col("pct_from_high"),
            col("pct_from_low"),
            col("position"),
        )
        .distinct()
    )

    result.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq4_price_vs_7d") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print(f"[SQ4] batch {batch_id} saved", flush=True)


query = (
    windowed.writeStream
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq4")
    .start()
)

query.awaitTermination()
