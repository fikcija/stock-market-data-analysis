"""
SQ2 — Market sentiment per exchange.
Stream-batch join: classify each trade as up/down vs prev_close from
the most recent daily snapshot; aggregate per exchange in 1-min window.
Output table: sq2_market_sentiment
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, count, explode, from_json, max as spark_max,
    round as spark_round, to_timestamp, when, window,
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
    .appName("SQ2 - Market Sentiment")
    .config("spark.sql.shuffle.partitions", "8")
    .config("hive.metastore.uris", os.environ["HIVE_SITE_CONF_hive_metastore_uris"])
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Load most recent daily snapshot as static batch DF
max_date_row = spark.table("stock_analytics.streaming_daily_quotes").agg(
    spark_max("quote_date").alias("max_date")
).collect()
max_date = max_date_row[0]["max_date"] if max_date_row else None

if max_date:
    snapshot = (
        spark.table("stock_analytics.streaming_daily_quotes")
        .filter(col("quote_date") == max_date)
        .select(
            col("symbol"),
            col("prev_close"),
            explode(col("exchange")).alias("exchange"),
        )
        .filter(col("prev_close").isNotNull() & (col("prev_close") > 0))
    )
else:
    snapshot = spark.createDataFrame(
        [],
        StructType([
            StructField("symbol", StringType()),
            StructField("prev_close", DoubleType()),
            StructField("exchange", StringType()),
        ]),
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
    .withWatermark("event_time", "2 minutes")
    .filter(col("price").isNotNull() & (col("price") > 0))
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    enriched = (
        batch_df.join(broadcast(snapshot), on="symbol", how="inner")
        .withColumn(
            "direction",
            when(col("price") > col("prev_close"), "up")
            .when(col("price") < col("prev_close"), "down")
            .otherwise("flat"),
        )
    )

    windowed = (
        enriched
        .groupBy(window("event_time", "1 minute"), col("exchange"))
        .agg(
            count(when(col("direction") == "up", 1)).alias("rising_count"),
            count(when(col("direction") == "down", 1)).alias("falling_count"),
        )
    )

    result = (
        windowed
        .withColumn(
            "sentiment_ratio",
            spark_round(
                col("rising_count") / (col("rising_count") + col("falling_count")),
                4,
            ),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("exchange"),
            col("rising_count"),
            col("falling_count"),
            col("sentiment_ratio"),
        )
        .filter((col("rising_count") + col("falling_count")) > 0)
        .distinct()
    )

    result.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq2_market_sentiment") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print(f"[SQ2] batch {batch_id} saved", flush=True)


query = (
    trades.writeStream
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq2")
    .start()
)

query.awaitTermination()
