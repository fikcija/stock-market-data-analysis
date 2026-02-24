import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, DoubleType, LongType, StringType, StructField, StructType,
)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
HIVE_METASTORE_URIS = os.environ["HIVE_SITE_CONF_hive_metastore_uris"]
LOCAL_DATA_PATH = "/streaming_data/daily"
HDFS_DAILY_PATH = HDFS_NAMENODE + "/data/streaming/daily_quotes/"
HDFS_AGG_PATH = HDFS_NAMENODE + "/data/streaming/7d_aggregates/"

conf = SparkConf().setAppName("LoadStreamingBaselines").setMaster("spark://spark-master:7077")
conf.set("hive.metastore.uris", HIVE_METASTORE_URIS)
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

quote_schema = StructType([
    StructField("symbol", StringType()),
    StructField("exchange", ArrayType(StringType())),
    StructField("quote", StructType([
        StructField("c", DoubleType()),   # current/close price
        StructField("d", DoubleType()),   # change
        StructField("dp", DoubleType()),  # change percent
        StructField("h", DoubleType()),   # day high
        StructField("l", DoubleType()),   # day low
        StructField("o", DoubleType()),   # open
        StructField("pc", DoubleType()),  # previous close
        StructField("t", LongType()),     # timestamp
    ])),
])

raw_df = (
    spark.read.schema(quote_schema)
    .option("multiLine", "true")
    .json(LOCAL_DATA_PATH + "/*/quotes.json")
    .withColumn("source_path", F.input_file_name())
    .withColumn(
        "quote_date",
        F.regexp_extract(F.col("source_path"), r"(\d{4}-\d{2}-\d{2})", 1),
    )
    .filter(
        F.col("symbol").isNotNull()
        & F.col("quote.c").isNotNull()
        & (F.col("quote.c") > 0)
        & (F.col("quote_date") != "")
    )
)

daily_df = raw_df.select(
    F.col("symbol"),
    F.col("exchange"),
    F.col("quote.c").alias("current_price"),
    F.col("quote.d").alias("change"),
    F.col("quote.dp").alias("change_pct"),
    F.col("quote.h").alias("high_price"),
    F.col("quote.l").alias("low_price"),
    F.col("quote.o").alias("open_price"),
    F.col("quote.pc").alias("prev_close"),
    F.col("quote.t").alias("api_timestamp"),
    F.col("quote_date"),
)

daily_df.write \
    .partitionBy("quote_date") \
    .mode("overwrite") \
    .parquet(HDFS_DAILY_PATH)

print(f"Written daily quotes to {HDFS_DAILY_PATH}", flush=True)

agg_df = daily_df.groupBy("symbol").agg(
    F.max("high_price").alias("max_7d_high"),
    F.min("low_price").alias("min_7d_low"),
    F.avg("prev_close").alias("avg_prev_close"),
    F.countDistinct("quote_date").alias("days_count"),
)

agg_df.write \
    .mode("overwrite") \
    .parquet(HDFS_AGG_PATH)

print(f"Written 7d aggregates to {HDFS_AGG_PATH}", flush=True)

spark.sql("CREATE DATABASE IF NOT EXISTS stock_analytics")
spark.sql("USE stock_analytics")

spark.sql("DROP TABLE IF EXISTS streaming_daily_quotes")
spark.sql(f"""
    CREATE EXTERNAL TABLE streaming_daily_quotes (
        symbol          STRING,
        exchange        ARRAY<STRING>,
        current_price   DOUBLE,
        change          DOUBLE,
        change_pct      DOUBLE,
        high_price      DOUBLE,
        low_price       DOUBLE,
        open_price      DOUBLE,
        prev_close      DOUBLE,
        api_timestamp   BIGINT
    )
    PARTITIONED BY (quote_date STRING)
    STORED AS PARQUET
    LOCATION '{HDFS_DAILY_PATH}'
""")
spark.sql("MSCK REPAIR TABLE streaming_daily_quotes")

spark.sql("DROP TABLE IF EXISTS streaming_7d_aggregates")
spark.sql(f"""
    CREATE EXTERNAL TABLE streaming_7d_aggregates (
        symbol          STRING,
        max_7d_high     DOUBLE,
        min_7d_low      DOUBLE,
        avg_prev_close  DOUBLE,
        days_count      INT
    )
    STORED AS PARQUET
    LOCATION '{HDFS_AGG_PATH}'
""")

spark.stop()
