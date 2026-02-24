"""
SQ5 — Current price deviation from previous close.
Stream-batch join: join live price against prev_close from the most
recent daily snapshot; compute deviation_pct per symbol in 1-min window.
Output table: sq5_vs_prev_close
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, from_json, round as spark_round,
    to_timestamp, when, window,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
import os

KAFKA_BROKERS = "kafka1:29092"
JDBC_URL = "jdbc:postgresql://pg:5432/streaming"
JDBC_DRIVER = "org.postgresql.Driver"

trade_schema = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("timestamp_ms", LongType()),
    StructField("volume", DoubleType()),
])

spark = (
    SparkSession.builder
    .appName("SQ5 - vs Prev Close")
    .config("spark.sql.shuffle.partitions", "8")
    .config("hive.metastore.uris", os.environ["HIVE_SITE_CONF_hive_metastore_uris"])
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Load most recent daily snapshot prev_close per symbol
max_date_row = spark.table("stock_analytics.streaming_daily_quotes").agg(
    spark_max("quote_date").alias("max_date")
).collect()
max_date = max_date_row[0]["max_date"] if max_date_row else None

if max_date:
    prev_close_df = (
        spark.table("stock_analytics.streaming_daily_quotes")
        .filter(col("quote_date") == max_date)
        .select(
            col("symbol"),
            col("prev_close"),
        )
        .filter(col("prev_close").isNotNull() & (col("prev_close") > 0))
    )
else:
    prev_close_df = spark.createDataFrame(
        [],
        StructType([
            StructField("symbol", StringType()),
            StructField("prev_close", DoubleType()),
        ]),
    )

trades = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", "stock_trades")
    .option("startingOffsets", "earliest")
    .load()
    .select(from_json(col("value").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("event_time", to_timestamp(col("timestamp_ms") / 1000))
    .withWatermark("event_time", "2 minutes")
    .filter(col("price").isNotNull() & (col("price") > 0))
)

windowed = (
    trades
    .groupBy(window("event_time", "1 minute"), col("symbol"))
    .agg(avg("price").alias("current_price"))
)


def save_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    from pyspark.sql.functions import broadcast

    joined = batch_df.join(broadcast(prev_close_df), on="symbol", how="inner")

    result = (
        joined
        .withColumn(
            "deviation_pct",
            spark_round(
                (col("current_price") - col("prev_close")) / col("prev_close") * 100,
                4,
            ),
        )
        .withColumn(
            "direction",
            when(col("deviation_pct") > 0, "up")
            .when(col("deviation_pct") < 0, "down")
            .otherwise("flat"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("symbol"),
            spark_round(col("current_price"), 4).alias("current_price"),
            spark_round(col("prev_close"), 4).alias("prev_close"),
            col("deviation_pct"),
            col("direction"),
        )
    )

    result.write.format("jdbc") \
        .option("url", JDBC_URL) \
        .option("driver", JDBC_DRIVER) \
        .option("dbtable", "sq5_vs_prev_close") \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .mode("append").save()
    print(f"[SQ5] batch {batch_id} saved", flush=True)


query = (
    windowed.writeStream
    .outputMode("update")
    .trigger(processingTime="30 seconds")
    .foreachBatch(save_to_postgres)
    .option("checkpointLocation", "/tmp/ckpt/sq5")
    .start()
)

query.awaitTermination()
